package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"moss/internal/mesh"
)

type callState struct {
	ID       string
	PeerID   string
	PeerName string
	Room     string
	Status   string
}

func (c callState) summary() string {
	if c.Status == "" {
		return "-"
	}
	return c.Status
}

func (c *chatApp) startCall(target string) error {
	peerID, label, err := c.resolvePeerTarget(target)
	if err != nil {
		return err
	}
	room, err := c.openDirectRoom(target)
	if err != nil {
		return err
	}
	callID := fmt.Sprintf("call-%d", time.Now().UnixNano())
	c.mu.Lock()
	c.callState = callState{
		ID:       callID,
		PeerID:   peerID,
		PeerName: label,
		Room:     room,
		Status:   "dialing",
	}
	c.mu.Unlock()
	return c.sendCallSignal("call_invite", peerID, callID, room)
}

func (c *chatApp) answerCall() error {
	c.mu.Lock()
	state := c.callState
	if state.Status != "ringing" {
		c.mu.Unlock()
		return fmt.Errorf("there is no incoming call to answer")
	}
	c.callState.Status = "active"
	c.mu.Unlock()
	if err := c.ensureDirectCallRoom(state.Room, state.PeerName); err != nil {
		return err
	}
	c.systemMessage("Call answered: " + state.PeerName)
	return c.sendCallSignal("call_accept", state.PeerID, state.ID, state.Room)
}

func (c *chatApp) declineCall() error {
	c.mu.Lock()
	state := c.callState
	if state.Status != "ringing" {
		c.mu.Unlock()
		return fmt.Errorf("there is no incoming call to decline")
	}
	c.callState = callState{}
	c.mu.Unlock()
	c.systemMessage("Call declined: " + state.PeerName)
	return c.sendCallSignal("call_decline", state.PeerID, state.ID, state.Room)
}

func (c *chatApp) hangupCall() error {
	c.mu.Lock()
	state := c.callState
	if state.Status == "" {
		c.mu.Unlock()
		return fmt.Errorf("there is no active call")
	}
	c.callState = callState{}
	c.mu.Unlock()
	c.systemMessage("Call ended: " + state.PeerName)
	return c.sendCallSignal("call_hangup", state.PeerID, state.ID, state.Room)
}

func (c *chatApp) sendCallSignal(kind, target, callID, room string) error {
	payload, _ := json.Marshal(chatPayload{
		Kind:       kind,
		Nick:       c.nickname,
		SentAt:     time.Now().Format("15:04:05"),
		Target:     target,
		CallID:     callID,
		CallAction: kind,
		Room:       room,
	})
	if code := c.node.Publish(controlRoom, payload); code != mesh.MOSS_OK {
		return publishError(controlRoom, code)
	}
	return nil
}

func (c *chatApp) handleCallPayload(senderID string, payload chatPayload) {
	peerName := emptyFallback(payload.Nick, c.displayNameForPeer(senderID))
	switch payload.Kind {
	case "call_invite":
		if err := c.ensureDirectCallRoom(payload.Room, peerName); err != nil {
			c.systemMessage("Call setup failed: " + err.Error())
			return
		}
		c.mu.Lock()
		c.callState = callState{
			ID:       payload.CallID,
			PeerID:   senderID,
			PeerName: peerName,
			Room:     payload.Room,
			Status:   "ringing",
		}
		c.mu.Unlock()
		c.systemMessage("Incoming call from " + peerName)
		c.notifyIncomingCall(peerName)
		c.showIncomingCallModal(peerName)
	case "call_accept":
		c.mu.Lock()
		if c.callState.ID == payload.CallID {
			c.callState.Status = "active"
		}
		c.mu.Unlock()
		c.systemMessage("Call connected: " + peerName)
	case "call_decline":
		c.mu.Lock()
		if c.callState.ID == payload.CallID {
			c.callState = callState{}
		}
		c.mu.Unlock()
		c.systemMessage("Call declined by " + peerName)
	case "call_hangup":
		c.mu.Lock()
		if c.callState.ID == payload.CallID || strings.EqualFold(c.callState.PeerID, senderID) {
			c.callState = callState{}
		}
		c.mu.Unlock()
		c.systemMessage("Call ended by " + peerName)
	}
	c.queueRefresh()
}

func (c *chatApp) ensureDirectCallRoom(room, peerName string) error {
	if room == "" {
		return fmt.Errorf("call room is empty")
	}
	if !c.ensureSubscribedRoom(room) {
		return fmt.Errorf("failed to subscribe call room")
	}
	c.setRoomTitle(room, "@"+peerName)
	return nil
}

func (c *chatApp) showIncomingCallModal(peerName string) {
	c.showChoiceModal("incoming-call", "Incoming call from "+peerName, []string{"Answer", "Decline"}, func(buttonLabel string) {
		switch buttonLabel {
		case "Answer":
			_ = c.answerCall()
		case "Decline":
			_ = c.declineCall()
		}
	})
}
