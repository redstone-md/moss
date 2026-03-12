package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rivo/tview"

	"moss/internal/mesh"
)

const (
	// Keep chunks small enough to survive the UDP transport path without relying on IP fragmentation.
	attachmentChunkSize = 512
	maxAttachmentBytes  = 8 * 1024 * 1024
)

type incomingAttachment struct {
	Room       string
	SenderID   string
	SenderName string
	FileName   string
	FileSize   int64
	FileSHA256 string
	ChunkCount int
	Chunks     [][]byte
	Received   int
	SavePath   string
	Accepted   bool
	Saved      bool
	LineID     string
	SentAt     string
}

type outgoingAttachment struct {
	Room       string
	FileName   string
	FileSize   int64
	ChunkCount int
	LineID     string
	SentAt     string
}

type preparedAttachment struct {
	Room       string
	Path       string
	Data       []byte
	FileName   string
	FileSize   int64
	FileSHA256 string
	ChunkCount int
}

func (c *chatApp) sendAttachment(room, rawPath string) error {
	if room == systemRoom || room == controlRoom {
		return fmt.Errorf("switch to a chat room before sending attachments")
	}
	prepared, err := c.prepareAttachment(room, rawPath)
	if err != nil {
		return err
	}
	c.confirmAttachmentSend(prepared)
	return nil
}

func (c *chatApp) prepareAttachment(room, rawPath string) (*preparedAttachment, error) {
	if room == systemRoom || room == controlRoom {
		return nil, fmt.Errorf("switch to a chat room before sending attachments")
	}
	path := strings.TrimSpace(rawPath)
	if path == "" {
		selected, err := c.selectAttachmentSource()
		if err != nil {
			if errors.Is(err, errDialogCancelled) {
				return nil, nil
			}
			return nil, err
		}
		path = selected
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("attachment is empty")
	}
	if len(data) > maxAttachmentBytes {
		return nil, fmt.Errorf("attachment exceeds %d MB limit", maxAttachmentBytes/(1024*1024))
	}
	fileName := filepath.Base(absPath)
	sum := sha256.Sum256(data)
	chunkCount := (len(data) + attachmentChunkSize - 1) / attachmentChunkSize
	return &preparedAttachment{
		Room:       room,
		Path:       absPath,
		Data:       data,
		FileName:   fileName,
		FileSize:   int64(len(data)),
		FileSHA256: hex.EncodeToString(sum[:]),
		ChunkCount: chunkCount,
	}, nil
}

func (c *chatApp) confirmAttachmentSend(prepared *preparedAttachment) {
	if prepared == nil {
		return
	}
	body := fmt.Sprintf(
		"Send %s (%s) to #%s?\n\nThe file will be broadcast over the current Moss room.",
		prepared.FileName,
		formatBytes(prepared.FileSize),
		prepared.Room,
	)
	modal := tview.NewModal().
		SetText(body).
		AddButtons([]string{"Send", "Cancel"}).
		SetDoneFunc(func(buttonIndex int, buttonLabel string) {
			c.closeModal("attachment-confirm")
			if buttonLabel != "Send" {
				return
			}
			go func() {
				if err := c.sendPreparedAttachment(prepared); err != nil {
					c.showAlert("Attachment", err.Error())
				}
			}()
		})
	modal.SetTitle(" Share Attachment ").SetBorder(true)
	c.showModal("attachment-confirm", modal)
}

func (c *chatApp) sendPreparedAttachment(prepared *preparedAttachment) error {
	if prepared == nil {
		return nil
	}
	transferID := fmt.Sprintf("%d-%s", time.Now().UnixNano(), c.localPeerID[:8])
	lineID := "out-" + transferID
	state := &outgoingAttachment{
		Room:       prepared.Room,
		FileName:   prepared.FileName,
		FileSize:   prepared.FileSize,
		ChunkCount: prepared.ChunkCount,
		LineID:     lineID,
		SentAt:     time.Now().Format("15:04:05"),
	}
	c.mu.Lock()
	c.outgoing[transferID] = state
	c.mu.Unlock()
	c.appendActionLine(prepared.Room, lineID, outgoingAttachmentLine(state, 0))

	meta, _ := json.Marshal(chatPayload{
		Kind:       "attachment-meta",
		Nick:       c.nickname,
		SentAt:     time.Now().Format("15:04:05"),
		TransferID: transferID,
		FileName:   prepared.FileName,
		FileSize:   prepared.FileSize,
		FileSHA256: prepared.FileSHA256,
		ChunkCount: prepared.ChunkCount,
		Attachment: true,
	})
	if code := c.node.Publish(prepared.Room, meta); code != mesh.MOSS_OK {
		c.failOutgoingTransfer(transferID, fmt.Sprintf("send failed: %v", publishError(prepared.Room, code)))
		return publishError(prepared.Room, code)
	}
	for index := 0; index < prepared.ChunkCount; index++ {
		start := index * attachmentChunkSize
		end := min(len(prepared.Data), start+attachmentChunkSize)
		chunk, _ := json.Marshal(chatPayload{
			Kind:       "attachment-chunk",
			Nick:       c.nickname,
			SentAt:     time.Now().Format("15:04:05"),
			TransferID: transferID,
			FileName:   prepared.FileName,
			FileSize:   prepared.FileSize,
			FileSHA256: prepared.FileSHA256,
			ChunkIndex: index,
			ChunkCount: prepared.ChunkCount,
			ChunkData:  base64.StdEncoding.EncodeToString(prepared.Data[start:end]),
			Attachment: true,
		})
		if code := c.node.Publish(prepared.Room, chunk); code != mesh.MOSS_OK {
			c.failOutgoingTransfer(transferID, fmt.Sprintf("send failed: %v", publishError(prepared.Room, code)))
			return publishError(prepared.Room, code)
		}
		c.updateOutgoingProgress(transferID, index+1)
	}
	c.finishOutgoingTransfer(transferID)
	return nil
}

func (c *chatApp) handleAttachmentPayload(room, senderID string, payload chatPayload) {
	senderName := emptyFallback(payload.Nick, c.displayNameForPeer(senderID))
	if senderID == c.localPeerID {
		return
	}
	switch payload.Kind {
	case "attachment-meta":
		lineID := "in-" + payload.TransferID
		c.mu.Lock()
		c.incoming[payload.TransferID] = &incomingAttachment{
			Room:       room,
			SenderID:   senderID,
			SenderName: senderName,
			FileName:   payload.FileName,
			FileSize:   payload.FileSize,
			FileSHA256: payload.FileSHA256,
			ChunkCount: payload.ChunkCount,
			Chunks:     make([][]byte, payload.ChunkCount),
			LineID:     lineID,
			SentAt:     payload.SentAt,
		}
		c.mu.Unlock()
		c.appendActionLine(room, lineID, incomingAttachmentLine(c.incoming[payload.TransferID]))
	case "attachment-chunk":
		raw, err := base64.StdEncoding.DecodeString(payload.ChunkData)
		if err != nil {
			c.showAlert("Attachment error", "Attachment chunk decode failed for "+payload.FileName)
			return
		}
		savedPath, complete, err := c.storeAttachmentChunk(payload.TransferID, payload.ChunkIndex, raw)
		if err != nil {
			c.showAlert("Attachment error", "Attachment write failed: "+err.Error())
			return
		}
		if complete {
			c.notifyTransferComplete(senderName, payload.FileName)
			c.showAlert("Attachment", "Saved to "+savedPath)
		}
	}
}

func (c *chatApp) storeAttachmentChunk(transferID string, index int, raw []byte) (string, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.incoming[transferID]
	if state == nil {
		return "", false, nil
	}
	if index < 0 || index >= len(state.Chunks) {
		return "", false, fmt.Errorf("attachment chunk index out of range")
	}
	if state.Chunks[index] == nil {
		state.Chunks[index] = append([]byte(nil), raw...)
		state.Received++
	}
	c.updateRoomLineLocked(state.Room, state.LineID, incomingAttachmentLine(state))
	if state.Received < state.ChunkCount {
		return "", false, nil
	}
	if !state.Accepted || state.SavePath == "" {
		c.updateRoomLineLocked(state.Room, state.LineID, incomingAttachmentLine(state))
		return "", false, nil
	}
	targetPath, err := c.finalizeIncomingAttachmentLocked(transferID, state)
	if err != nil {
		return "", false, err
	}
	return targetPath, true, nil
}

func (c *chatApp) finalizeIncomingAttachmentLocked(transferID string, state *incomingAttachment) (string, error) {
	data := make([]byte, 0, state.FileSize)
	for _, chunk := range state.Chunks {
		data = append(data, chunk...)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != state.FileSHA256 {
		delete(c.incoming, transferID)
		return "", fmt.Errorf("attachment checksum mismatch")
	}
	targetPath := state.SavePath
	if targetPath == "" {
		targetDir := filepath.Join(c.downloadsDir, sanitizeName(c.meshID), sanitizeName(state.Room))
		if err := os.MkdirAll(targetDir, 0o755); err != nil {
			delete(c.incoming, transferID)
			return "", err
		}
		targetPath = uniqueAttachmentPath(targetDir, state.FileName)
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		delete(c.incoming, transferID)
		return "", err
	}
	if err := os.WriteFile(targetPath, data, 0o644); err != nil {
		delete(c.incoming, transferID)
		return "", err
	}
	state.Saved = true
	c.updateRoomLineLocked(state.Room, state.LineID, fmt.Sprintf(
		"[%s] attachment saved from %s: %s -> %s",
		time.Now().Format("15:04:05"),
		state.SenderName,
		state.FileName,
		targetPath,
	))
	delete(c.incoming, transferID)
	return targetPath, nil
}

func uniqueAttachmentPath(dir, fileName string) string {
	base := filepath.Base(fileName)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	candidate := filepath.Join(dir, base)
	for index := 1; ; index++ {
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate
		}
		candidate = filepath.Join(dir, fmt.Sprintf("%s-%d%s", name, index, ext))
	}
}

func formatBytes(size int64) string {
	switch {
	case size >= 1024*1024:
		return fmt.Sprintf("%.1f MB", float64(size)/(1024*1024))
	case size >= 1024:
		return fmt.Sprintf("%.1f KB", float64(size)/1024)
	default:
		return fmt.Sprintf("%d B", size)
	}
}

func publishError(room string, code int32) error {
	switch code {
	case mesh.MOSS_ERR_NO_PEERS:
		return fmt.Errorf("room #%s has no connected peers yet", room)
	default:
		return fmt.Errorf("publish failed with code %d", code)
	}
}

func (c *chatApp) updateOutgoingProgress(transferID string, sentChunks int) {
	c.mu.RLock()
	state := c.outgoing[transferID]
	c.mu.RUnlock()
	if state == nil {
		return
	}
	c.updateRoomLine(state.Room, state.LineID, outgoingAttachmentLine(state, sentChunks))
}

func (c *chatApp) finishOutgoingTransfer(transferID string) {
	c.mu.Lock()
	state := c.outgoing[transferID]
	delete(c.outgoing, transferID)
	c.mu.Unlock()
	if state == nil {
		return
	}
	c.updateRoomLine(state.Room, state.LineID, outgoingAttachmentLine(state, state.ChunkCount))
}

func (c *chatApp) failOutgoingTransfer(transferID, status string) {
	c.mu.Lock()
	state := c.outgoing[transferID]
	delete(c.outgoing, transferID)
	c.mu.Unlock()
	if state == nil {
		return
	}
	c.updateRoomLine(state.Room, state.LineID, fmt.Sprintf("[%s] attachment failed: %s (%s)", time.Now().Format("15:04:05"), state.FileName, status))
}

func (c *chatApp) showIncomingAttachmentModal(transferID string) {
	c.mu.RLock()
	state := c.incoming[transferID]
	c.mu.RUnlock()
	if state == nil {
		return
	}
	if state.Saved {
		c.showAlert("Attachment", "Attachment is already saved.")
		return
	}
	body := fmt.Sprintf(
		"Download %s from %s?\n\nSize: %s\nReceived: %d/%d chunks",
		state.FileName,
		state.SenderName,
		formatBytes(state.FileSize),
		state.Received,
		state.ChunkCount,
	)
	modal := tview.NewModal().
		SetText(body).
		AddButtons([]string{"Download", "Cancel"}).
		SetDoneFunc(func(buttonIndex int, buttonLabel string) {
			c.closeModal("attachment-download")
			if buttonLabel != "Download" {
				return
			}
			go func() {
				if err := c.acceptIncomingAttachment(transferID); err != nil {
					c.showAlert("Attachment", err.Error())
				}
			}()
		})
	modal.SetTitle(" Download Attachment ").SetBorder(true)
	c.showModal("attachment-download", modal)
}

func (c *chatApp) acceptIncomingAttachment(transferID string) error {
	c.mu.RLock()
	state := c.incoming[transferID]
	c.mu.RUnlock()
	if state == nil {
		return nil
	}
	savePath, err := c.selectAttachmentSavePath(state.FileName)
	if err != nil {
		if errors.Is(err, errDialogCancelled) {
			return nil
		}
		return err
	}

	c.mu.Lock()
	state = c.incoming[transferID]
	if state == nil {
		c.mu.Unlock()
		return nil
	}
	state.Accepted = true
	state.SavePath = savePath
	c.updateRoomLineLocked(state.Room, state.LineID, incomingAttachmentLine(state))
	complete := state.Received >= state.ChunkCount
	var targetPath string
	if complete {
		targetPath, err = c.finalizeIncomingAttachmentLocked(transferID, state)
	}
	c.mu.Unlock()
	c.queueRefresh()
	if err != nil {
		return err
	}
	if complete {
		c.notifyTransferComplete(state.SenderName, state.FileName)
		c.showAlert("Attachment", "Saved to "+targetPath)
	}
	return nil
}

func incomingAttachmentLine(state *incomingAttachment) string {
	if state == nil {
		return ""
	}
	progress := attachmentProgressBar(state.Received, state.ChunkCount)
	switch {
	case state.Saved:
		return fmt.Sprintf("[%s] attachment from %s saved: %s", time.Now().Format("15:04:05"), state.SenderName, state.FileName)
	case state.Accepted && state.Received < state.ChunkCount:
		return fmt.Sprintf("[%s] downloading from %s: %s %s %d/%d", state.SentAt, state.SenderName, state.FileName, progress, state.Received, state.ChunkCount)
	case state.Accepted:
		return fmt.Sprintf("[%s] ready to save from %s: %s (%s)", state.SentAt, state.SenderName, state.FileName, formatBytes(state.FileSize))
	case state.Received >= state.ChunkCount:
		return fmt.Sprintf("[%s] %s shared %s (%s). Click or press Enter to save.", state.SentAt, state.SenderName, state.FileName, formatBytes(state.FileSize))
	default:
		return fmt.Sprintf("[%s] %s offered %s (%s). Click or press Enter to download. %s %d/%d", state.SentAt, state.SenderName, state.FileName, formatBytes(state.FileSize), progress, state.Received, state.ChunkCount)
	}
}

func outgoingAttachmentLine(state *outgoingAttachment, sentChunks int) string {
	if state == nil {
		return ""
	}
	progress := attachmentProgressBar(sentChunks, state.ChunkCount)
	if sentChunks >= state.ChunkCount {
		return fmt.Sprintf("[%s] you shared attachment: %s (%s)", time.Now().Format("15:04:05"), state.FileName, formatBytes(state.FileSize))
	}
	return fmt.Sprintf("[%s] sending attachment: %s %s %d/%d", state.SentAt, state.FileName, progress, sentChunks, state.ChunkCount)
}

func attachmentProgressBar(done, total int) string {
	const width = 16
	if total <= 0 {
		return "[" + strings.Repeat("-", width) + "]"
	}
	if done < 0 {
		done = 0
	}
	if done > total {
		done = total
	}
	filled := done * width / total
	return "[" + strings.Repeat("=", filled) + strings.Repeat("-", width-filled) + "]"
}
