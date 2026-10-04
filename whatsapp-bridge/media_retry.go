package main

// Media links expire about a month after a message is sent, and WhatsApp then
// answers downloads with 403, 404 or 410. That covers nearly all media that
// arrives through history sync. A media retry receipt asks the phone to upload
// the file again and answers with a new direct path; it works while the phone
// is online and still holds the file. See whatsmeow's SendMediaRetryReceipt.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// mediaRetryTimeout bounds the wait for the phone's answer, which includes
// the phone uploading the file. It stays below the REST server's 60 s
// WriteTimeout so that a phone that does not answer in time yields an error
// response instead of a dropped connection.
var mediaRetryTimeout = 45 * time.Second

// mediaRetries maps a message ID to the channel of the download waiting for
// that message's MediaRetry event.
var mediaRetries sync.Map

// sendMediaRetryReceipt lets tests run the retry without a phone.
var sendMediaRetryReceipt = func(client *whatsmeow.Client, info *types.MessageInfo, mediaKey []byte) error {
	return client.SendMediaRetryReceipt(context.Background(), info, mediaKey)
}

func mediaExpired(err error) bool {
	return errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith403) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith404) ||
		errors.Is(err, whatsmeow.ErrMediaDownloadFailedWith410)
}

// handleMediaRetry hands a MediaRetry event to the download waiting for it.
func handleMediaRetry(evt *events.MediaRetry) {
	if waiting, ok := mediaRetries.Load(evt.MessageID); ok {
		select {
		case waiting.(chan *events.MediaRetry) <- evt:
		default:
		}
	}
}

// storedMessageInfo builds the MessageInfo a media retry receipt needs from
// the stored message. Group senders are stored as the user part of their
// phone JID.
func storedMessageInfo(store *MessageStore, messageID, chatJID string) (*types.MessageInfo, error) {
	chat, err := types.ParseJID(chatJID)
	if err != nil {
		return nil, fmt.Errorf("invalid chat JID %q: %w", chatJID, err)
	}
	var fromMe bool
	var sender string
	if err := store.db.QueryRow(
		`SELECT is_from_me, sender FROM messages WHERE id = ? AND chat_jid = ?`, messageID, chatJID,
	).Scan(&fromMe, &sender); err != nil {
		return nil, fmt.Errorf("failed to find message: %w", err)
	}
	info := &types.MessageInfo{
		ID: messageID,
		MessageSource: types.MessageSource{
			Chat:     chat,
			IsFromMe: fromMe,
			IsGroup:  chat.Server == types.GroupServer,
		},
	}
	if info.IsGroup {
		info.Sender = types.NewJID(sender, types.DefaultUserServer)
	}
	return info, nil
}

// requestMediaReupload asks the phone to upload a message's media again and
// returns the new direct path.
func requestMediaReupload(client *whatsmeow.Client, info *types.MessageInfo, mediaKey []byte) (string, error) {
	waiting := make(chan *events.MediaRetry, 1)
	mediaRetries.Store(info.ID, waiting)
	defer mediaRetries.Delete(info.ID)

	if err := sendMediaRetryReceipt(client, info, mediaKey); err != nil {
		return "", fmt.Errorf("failed to ask the phone to upload the media again: %w", err)
	}
	select {
	case evt := <-waiting:
		notification, err := whatsmeow.DecryptMediaRetryNotification(evt, mediaKey)
		if err != nil {
			return "", fmt.Errorf("phone did not upload the media again: %w", err)
		}
		if notification.GetResult() != waMmsRetry.MediaRetryNotification_SUCCESS {
			return "", fmt.Errorf("phone did not upload the media again: %s", notification.GetResult())
		}
		return notification.GetDirectPath(), nil
	case <-time.After(mediaRetryTimeout):
		return "", fmt.Errorf("phone did not upload the media again within %s", mediaRetryTimeout)
	}
}
