package server

import (
	"chaogarden-server/pkg/packets"
	"context"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// ClientState defines the behavior of a client in a particular state.
type ClientState interface {
	HandleLoginRequest(client *Client, req *packets.LoginRequest)
	HandlePlayerMove(client *Client, req *packets.PlayerMoveRequest)
}

// UnauthenticatedState is the initial state for a client.
type UnauthenticatedState struct {
	hub *Hub
}

func (s *UnauthenticatedState) HandleLoginRequest(client *Client, req *packets.LoginRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	user, err := s.hub.queries.GetUserByUsername(ctx, strings.ToLower(req.Username))
	if err != nil {
		client.logger.Printf("Login failed for user '%s': user not found.", req.Username)
		denyMsg := &packets.DenyResponse{Reason: "Invalid credentials"}
		s.hub.SendMessage(client, 7, denyMsg)
		return
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		client.logger.Printf("Login failed for user '%s': incorrect password.", req.Username)
		denyMsg := &packets.DenyResponse{Reason: "Invalid credentials"}
		s.hub.SendMessage(client, 7, denyMsg)
		return
	}
	client.logger.Printf("User '%s' authenticated successfully.", user.Username)
	client.SetUserID(user.ID)
	client.SetUsername(user.Username)
	connectedState := &ConnectedState{hub: s.hub}
	client.SetState(connectedState)
	playerMsg := &packets.Player{
		Id:              uint64(user.ID),
		Name:            user.Username,
		BaomainJsonData: string(user.BaomainData),
	}
	s.hub.SendMessage(client, 22, playerMsg)
}

func (s *UnauthenticatedState) HandlePlayerMove(client *Client, req *packets.PlayerMoveRequest) {
	client.logger.Println("Received PlayerMoveRequest from unauthenticated client. Ignoring.")
}

// ConnectedState is the state for a fully authenticated and connected client.
type ConnectedState struct {
	hub *Hub
}

func (s *ConnectedState) HandleLoginRequest(client *Client, req *packets.LoginRequest) {
	client.logger.Println("Received login request while already connected. Ignoring.")
}

func (s *ConnectedState) HandlePlayerMove(client *Client, req *packets.PlayerMoveRequest) {
	client.logger.Println("Received PlayerMoveRequest from non-world client. Ignoring.")
}

func (s *ConnectedState) HandleEnterWorld(client *Client) {
	client.logger.Println("Handling EnterWorldRequest, transitioning to OverworldState.")
	overworld := &OverworldState{hub: s.hub}
	client.SetState(overworld)
	overworld.OnEnter(client)
}