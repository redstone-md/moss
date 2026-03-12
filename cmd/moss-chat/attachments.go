package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"moss/internal/mesh"
)

const (
	attachmentChunkSize       = 512
	maxAttachmentBytes  int64 = 5 * 1024 * 1024 * 1024
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
	TransferID  string
	Room        string
	Path        string
	FileName    string
	FileSize    int64
	FileSHA256  string
	ChunkCount  int
	LineID      string
	SentAt      string
	LastTarget  string
}

type preparedAttachment struct {
	Room       string
	Path       string
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
	info, err := os.Stat(absPath)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("attachments must be files, not folders")
	}
	if info.Size() == 0 {
		return nil, fmt.Errorf("attachment is empty")
	}
	if info.Size() > maxAttachmentBytes {
		return nil, fmt.Errorf("attachment exceeds %d GB limit", maxAttachmentBytes/(1024*1024*1024))
	}
	sum, err := attachmentFileSHA256(absPath)
	if err != nil {
		return nil, err
	}
	chunkCount := int((info.Size() + attachmentChunkSize - 1) / attachmentChunkSize)
	return &preparedAttachment{
		Room:       room,
		Path:       absPath,
		FileName:   filepath.Base(absPath),
		FileSize:   info.Size(),
		FileSHA256: sum,
		ChunkCount: chunkCount,
	}, nil
}

func (c *chatApp) confirmAttachmentSend(prepared *preparedAttachment) {
	if prepared == nil {
		return
	}
	body := fmt.Sprintf(
		"Offer %s (%s) in #%s?\n\nThe file will not transfer until somebody requests it and you approve the upload.",
		prepared.FileName,
		formatBytes(prepared.FileSize),
		prepared.Room,
	)
	c.showChoiceModal("attachment-confirm", body, []string{"Offer", "Cancel"}, func(buttonLabel string) {
		if buttonLabel != "Offer" {
			return
		}
		go func() {
			if err := c.offerAttachment(prepared); err != nil {
				c.showAlert("Attachment", err.Error())
			}
		}()
	})
}

func (c *chatApp) offerAttachment(prepared *preparedAttachment) error {
	if prepared == nil {
		return nil
	}
	transferID := fmt.Sprintf("%d-%s", time.Now().UnixNano(), c.localPeerID[:8])
	lineID := "out-" + transferID
	state := &outgoingAttachment{
		TransferID: transferID,
		Room:       prepared.Room,
		Path:       prepared.Path,
		FileName:   prepared.FileName,
		FileSize:   prepared.FileSize,
		FileSHA256: prepared.FileSHA256,
		ChunkCount: prepared.ChunkCount,
		LineID:     lineID,
		SentAt:     time.Now().Format("15:04:05"),
	}
	c.mu.Lock()
	c.outgoing[transferID] = state
	c.mu.Unlock()
	c.appendActionLine(prepared.Room, lineID, outgoingAttachmentOfferLine(state))

	meta, _ := json.Marshal(chatPayload{
		Kind:       "attachment-meta",
		Nick:       c.nickname,
		SentAt:     state.SentAt,
		TransferID: transferID,
		FileName:   prepared.FileName,
		FileSize:   prepared.FileSize,
		FileSHA256: prepared.FileSHA256,
		ChunkCount: prepared.ChunkCount,
		Attachment: true,
	})
	if code := c.node.Publish(prepared.Room, meta); code != mesh.MOSS_OK {
		c.failOutgoingTransfer(transferID, fmt.Sprintf("offer failed: %v", publishError(prepared.Room, code)))
		return publishError(prepared.Room, code)
	}
	return nil
}

func (c *chatApp) handleAttachmentPayload(room, senderID string, payload chatPayload) {
	if payload.Target != "" && payload.Target != c.localPeerID {
		return
	}
	senderName := emptyFallback(payload.Nick, c.displayNameForPeer(senderID))
	if senderID == c.localPeerID {
		return
	}
	if payload.Room != "" {
		if normalized, err := normalizeRoom(payload.Room); err == nil {
			room = normalized
		}
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
		c.appendActionLine(room, lineID, incomingAttachmentOfferLine(c.incoming[payload.TransferID]))
	case "attachment-start":
		c.mu.Lock()
		state := c.incoming[payload.TransferID]
		if state != nil {
			state.SentAt = payload.SentAt
			c.updateRoomLineLocked(state.Room, state.LineID, incomingAttachmentLine(state))
		}
		c.mu.Unlock()
		c.queueRefresh()
	case "attachment-denied":
		c.mu.Lock()
		state := c.incoming[payload.TransferID]
		if state != nil {
			c.updateRoomLineLocked(state.Room, state.LineID, fmt.Sprintf("[%s] %s declined sending %s.", time.Now().Format("15:04:05"), senderName, state.FileName))
		}
		c.mu.Unlock()
		c.queueRefresh()
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
		"Request %s from %s?\n\nSize: %s",
		state.FileName,
		state.SenderName,
		formatBytes(state.FileSize),
	)
	c.showChoiceModal("attachment-download", body, []string{"Request", "Cancel"}, func(buttonLabel string) {
		if buttonLabel != "Request" {
			return
		}
		go func() {
			if err := c.acceptIncomingAttachment(transferID); err != nil {
				c.showAlert("Attachment", err.Error())
			}
		}()
	})
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
	state.SentAt = time.Now().Format("15:04:05")
	c.updateRoomLineLocked(state.Room, state.LineID, incomingAttachmentWaitingLine(state))
	c.mu.Unlock()
	c.queueRefresh()
	c.requestAttachmentTransfer(transferID)
	return nil
}

func (c *chatApp) requestAttachmentTransfer(transferID string) {
	c.mu.RLock()
	state := c.incoming[transferID]
	c.mu.RUnlock()
	if state == nil {
		return
	}
	payload, _ := json.Marshal(chatPayload{
		Kind:       "attachment-request",
		Nick:       c.nickname,
		SentAt:     time.Now().Format("15:04:05"),
		Target:     state.SenderID,
		TransferID: transferID,
		Room:       state.Room,
		FileName:   state.FileName,
	})
	code := c.node.Publish(controlRoom, payload)
	if code != mesh.MOSS_OK && code != mesh.MOSS_ERR_NO_PEERS {
		c.showAlert("Attachment", fmt.Sprintf("Attachment request failed: %d", code))
	}
}

func (c *chatApp) promptAttachmentRequest(requesterID, transferID string) {
	c.mu.RLock()
	state := c.outgoing[transferID]
	c.mu.RUnlock()
	if state == nil {
		return
	}
	requesterName := c.displayNameForPeer(requesterID)
	body := fmt.Sprintf(
		"%s wants to download %s (%s).\n\nSend it now?",
		requesterName,
		state.FileName,
		formatBytes(state.FileSize),
	)
	c.showChoiceModal("attachment-request", body, []string{"Send", "Decline"}, func(buttonLabel string) {
		switch buttonLabel {
		case "Send":
			go func() {
				if err := c.streamAttachmentToTarget(transferID, requesterID); err != nil {
					c.showAlert("Attachment", err.Error())
				}
			}()
		case "Decline":
			go c.declineAttachmentTransfer(transferID, requesterID)
		}
	})
}

func (c *chatApp) streamAttachmentToTarget(transferID, targetPeerID string) error {
	c.mu.Lock()
	state := c.outgoing[transferID]
	if state != nil {
		state.LastTarget = targetPeerID
		state.SentAt = time.Now().Format("15:04:05")
		c.updateRoomLineLocked(state.Room, state.LineID, outgoingAttachmentProgressLine(state, 0, c.displayNameForPeer(targetPeerID)))
	}
	c.mu.Unlock()
	c.queueRefresh()
	if state == nil {
		return fmt.Errorf("attachment %s is no longer available", transferID)
	}

	file, err := os.Open(state.Path)
	if err != nil {
		c.failOutgoingTransfer(transferID, "open failed: "+err.Error())
		return err
	}
	defer file.Close()

	started, _ := json.Marshal(chatPayload{
		Kind:       "attachment-start",
		Nick:       c.nickname,
		SentAt:     state.SentAt,
		Target:     targetPeerID,
		TransferID: transferID,
		Room:       state.Room,
		FileName:   state.FileName,
		FileSize:   state.FileSize,
		FileSHA256: state.FileSHA256,
		ChunkCount: state.ChunkCount,
		Attachment: true,
	})
	if code := c.node.Publish(controlRoom, started); code != mesh.MOSS_OK {
		c.failOutgoingTransfer(transferID, fmt.Sprintf("send failed: %v", publishError(controlRoom, code)))
		return publishError(controlRoom, code)
	}

	buffer := make([]byte, attachmentChunkSize)
	for index := 0; index < state.ChunkCount; index++ {
		n, readErr := io.ReadFull(file, buffer)
		if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			c.failOutgoingTransfer(transferID, "read failed: "+readErr.Error())
			return readErr
		}
		if n == 0 {
			break
		}
		chunk, _ := json.Marshal(chatPayload{
			Kind:       "attachment-chunk",
			Nick:       c.nickname,
			SentAt:     state.SentAt,
			Target:     targetPeerID,
			TransferID: transferID,
			Room:       state.Room,
			FileName:   state.FileName,
			FileSize:   state.FileSize,
			FileSHA256: state.FileSHA256,
			ChunkIndex: index,
			ChunkCount: state.ChunkCount,
			ChunkData:  base64.StdEncoding.EncodeToString(buffer[:n]),
			Attachment: true,
		})
		if code := c.node.Publish(controlRoom, chunk); code != mesh.MOSS_OK {
			c.failOutgoingTransfer(transferID, fmt.Sprintf("send failed: %v", publishError(controlRoom, code)))
			return publishError(controlRoom, code)
		}
		c.updateOutgoingProgress(transferID, index+1, targetPeerID)
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
	}
	c.finishOutgoingTransfer(transferID, targetPeerID)
	return nil
}

func (c *chatApp) declineAttachmentTransfer(transferID, targetPeerID string) {
	payload, _ := json.Marshal(chatPayload{
		Kind:       "attachment-denied",
		Nick:       c.nickname,
		SentAt:     time.Now().Format("15:04:05"),
		Target:     targetPeerID,
		TransferID: transferID,
		Attachment: true,
	})
	_ = c.node.Publish(controlRoom, payload)
}

func (c *chatApp) updateOutgoingProgress(transferID string, sentChunks int, targetPeerID string) {
	c.mu.RLock()
	state := c.outgoing[transferID]
	c.mu.RUnlock()
	if state == nil {
		return
	}
	c.updateRoomLine(state.Room, state.LineID, outgoingAttachmentProgressLine(state, sentChunks, c.displayNameForPeer(targetPeerID)))
}

func (c *chatApp) finishOutgoingTransfer(transferID, targetPeerID string) {
	c.mu.RLock()
	state := c.outgoing[transferID]
	c.mu.RUnlock()
	if state == nil {
		return
	}
	c.updateRoomLine(state.Room, state.LineID, outgoingAttachmentDoneLine(state, c.displayNameForPeer(targetPeerID)))
}

func (c *chatApp) failOutgoingTransfer(transferID, status string) {
	c.mu.Lock()
	state := c.outgoing[transferID]
	c.mu.Unlock()
	if state == nil {
		return
	}
	c.updateRoomLine(state.Room, state.LineID, fmt.Sprintf("[%s] attachment failed: %s (%s)", time.Now().Format("15:04:05"), state.FileName, status))
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
	case size >= 1024*1024*1024:
		return fmt.Sprintf("%.1f GB", float64(size)/(1024*1024*1024))
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
	case mesh.MOSS_ERR_MESSAGE_TOO_LARGE:
		return fmt.Errorf("message exceeds runtime size limits")
	default:
		return fmt.Errorf("publish failed with code %d", code)
	}
}

func incomingAttachmentOfferLine(state *incomingAttachment) string {
	if state == nil {
		return ""
	}
	return fmt.Sprintf("[%s] %s offered %s (%s). Click or press Enter to request it.", state.SentAt, state.SenderName, state.FileName, formatBytes(state.FileSize))
}

func incomingAttachmentWaitingLine(state *incomingAttachment) string {
	if state == nil {
		return ""
	}
	return fmt.Sprintf("[%s] waiting for %s to approve %s...", state.SentAt, state.SenderName, state.FileName)
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
		return fmt.Sprintf("[%s] waiting for download data from %s: %s", state.SentAt, state.SenderName, state.FileName)
	default:
		return incomingAttachmentOfferLine(state)
	}
}

func outgoingAttachmentOfferLine(state *outgoingAttachment) string {
	if state == nil {
		return ""
	}
	return fmt.Sprintf("[%s] you offered attachment: %s (%s). Waiting for download requests.", state.SentAt, state.FileName, formatBytes(state.FileSize))
}

func outgoingAttachmentProgressLine(state *outgoingAttachment, sentChunks int, targetLabel string) string {
	if state == nil {
		return ""
	}
	if strings.TrimSpace(targetLabel) == "" {
		targetLabel = "peer"
	}
	progress := attachmentProgressBar(sentChunks, state.ChunkCount)
	return fmt.Sprintf("[%s] sending %s to %s %s %d/%d", state.SentAt, state.FileName, targetLabel, progress, sentChunks, state.ChunkCount)
}

func outgoingAttachmentDoneLine(state *outgoingAttachment, targetLabel string) string {
	if state == nil {
		return ""
	}
	if strings.TrimSpace(targetLabel) == "" {
		targetLabel = "peer"
	}
	return fmt.Sprintf("[%s] you shared attachment with %s: %s (%s)", time.Now().Format("15:04:05"), targetLabel, state.FileName, formatBytes(state.FileSize))
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

func attachmentFileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}
