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

	"moss/internal/mesh"
)

const (
	attachmentChunkSize = 24 * 1024
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
	Rejected   bool
}

func (c *chatApp) sendAttachment(room, rawPath string) error {
	if room == systemRoom || room == controlRoom {
		return fmt.Errorf("switch to a chat room before sending attachments")
	}
	path := strings.TrimSpace(rawPath)
	if path == "" {
		selected, err := c.selectAttachmentSource()
		if err != nil {
			if errors.Is(err, errDialogCancelled) {
				return nil
			}
			return err
		}
		path = selected
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(absPath)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return fmt.Errorf("attachment is empty")
	}
	if len(data) > maxAttachmentBytes {
		return fmt.Errorf("attachment exceeds %d MB limit", maxAttachmentBytes/(1024*1024))
	}
	fileName := filepath.Base(absPath)
	sum := sha256.Sum256(data)
	transferID := fmt.Sprintf("%d-%s", time.Now().UnixNano(), c.localPeerID[:8])
	chunkCount := (len(data) + attachmentChunkSize - 1) / attachmentChunkSize

	meta, _ := json.Marshal(chatPayload{
		Kind:       "attachment-meta",
		Nick:       c.nickname,
		SentAt:     time.Now().Format("15:04:05"),
		TransferID: transferID,
		FileName:   fileName,
		FileSize:   int64(len(data)),
		FileSHA256: hex.EncodeToString(sum[:]),
		ChunkCount: chunkCount,
		Attachment: true,
	})
	if code := c.node.Publish(room, meta); code != mesh.MOSS_OK {
		return publishError(room, code)
	}
	for index := 0; index < chunkCount; index++ {
		start := index * attachmentChunkSize
		end := min(len(data), start+attachmentChunkSize)
		chunk, _ := json.Marshal(chatPayload{
			Kind:       "attachment-chunk",
			Nick:       c.nickname,
			SentAt:     time.Now().Format("15:04:05"),
			TransferID: transferID,
			FileName:   fileName,
			FileSize:   int64(len(data)),
			FileSHA256: hex.EncodeToString(sum[:]),
			ChunkIndex: index,
			ChunkCount: chunkCount,
			ChunkData:  base64.StdEncoding.EncodeToString(data[start:end]),
			Attachment: true,
		})
		if code := c.node.Publish(room, chunk); code != mesh.MOSS_OK {
			return publishError(room, code)
		}
	}
	return nil
}

func (c *chatApp) handleAttachmentPayload(room, senderID string, payload chatPayload) {
	senderName := emptyFallback(payload.Nick, c.displayNameForPeer(senderID))
	if senderID == c.localPeerID {
		if payload.Kind == "attachment-meta" {
			c.appendLine(room, fmt.Sprintf("[%s] you shared attachment: %s (%s)", payload.SentAt, payload.FileName, formatBytes(payload.FileSize)))
		}
		return
	}
	switch payload.Kind {
	case "attachment-meta":
		savePath, err := c.selectAttachmentSavePath(payload.FileName)
		if err != nil {
			if errors.Is(err, errDialogCancelled) {
				c.appendLine(room, fmt.Sprintf("[%s] attachment from %s was declined", payload.SentAt, senderName))
				return
			}
			c.appendLine(room, fmt.Sprintf("[%s] attachment from %s was skipped: %v", payload.SentAt, senderName, err))
			return
		}
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
			SavePath:   savePath,
		}
		c.mu.Unlock()
		c.appendLine(room, fmt.Sprintf("[%s] %s is sending attachment: %s (%s) -> %s", payload.SentAt, senderName, payload.FileName, formatBytes(payload.FileSize), savePath))
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
			c.appendLine(room, fmt.Sprintf("[%s] attachment saved from %s: %s -> %s", time.Now().Format("15:04:05"), senderName, payload.FileName, savedPath))
			c.notifyTransferComplete(senderName, payload.FileName)
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
	if state.Rejected {
		delete(c.incoming, transferID)
		return "", false, nil
	}
	if index < 0 || index >= len(state.Chunks) {
		return "", false, fmt.Errorf("attachment chunk index out of range")
	}
	if state.Chunks[index] == nil {
		state.Chunks[index] = append([]byte(nil), raw...)
		state.Received++
	}
	if state.Received < state.ChunkCount {
		return "", false, nil
	}
	data := make([]byte, 0, state.FileSize)
	for _, chunk := range state.Chunks {
		data = append(data, chunk...)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != state.FileSHA256 {
		delete(c.incoming, transferID)
		return "", false, fmt.Errorf("attachment checksum mismatch")
	}
	targetPath := state.SavePath
	if targetPath == "" {
		targetDir := filepath.Join(c.downloadsDir, sanitizeName(c.meshID), sanitizeName(state.Room))
		if err := os.MkdirAll(targetDir, 0o755); err != nil {
			delete(c.incoming, transferID)
			return "", false, err
		}
		targetPath = uniqueAttachmentPath(targetDir, state.FileName)
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		delete(c.incoming, transferID)
		return "", false, err
	}
	if err := os.WriteFile(targetPath, data, 0o644); err != nil {
		delete(c.incoming, transferID)
		return "", false, err
	}
	delete(c.incoming, transferID)
	return targetPath, true, nil
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
