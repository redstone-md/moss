package main

import (
	"fmt"
	"strings"

	"github.com/gen2brain/beeep"
)

func init() {
	beeep.AppName = "Moss Chat"
}

func (c *chatApp) containsMention(text string) bool {
	needle := "@" + strings.ToLower(strings.TrimSpace(c.nickname))
	if needle == "@" {
		return false
	}
	return strings.Contains(strings.ToLower(text), needle)
}

func (c *chatApp) notifyIncomingMessage(room, senderName, text string) {
	mention := c.containsMention(text)
	direct := strings.HasPrefix(room, "dm-")
	if !mention && !direct {
		return
	}
	title := "Moss Chat"
	switch {
	case direct:
		title = "Direct message from " + senderName
	case mention:
		title = "Mention from " + senderName
	}
	c.signalNotification(title, text)
}

func (c *chatApp) notifyTransferComplete(senderName, fileName string) {
	c.signalNotification("Attachment received from "+senderName, fileName)
}

func (c *chatApp) notifyIncomingCall(senderName string) {
	c.signalNotification("Incoming call from "+senderName, "Use /answer or /decline")
}

func (c *chatApp) signalNotification(title, body string) {
	c.mu.RLock()
	enabled := c.sounds
	c.mu.RUnlock()
	if !enabled {
		return
	}
	go func() {
		fmt.Print("\a")
		_ = beeep.Beep(880, 180)
		_ = beeep.Notify(title, body, "")
	}()
}
