// ingame.go (Revised)
package server

import (
	"context"
	"math"
	"time"

	"chaogarden-server/internal/server/objects"
	"chaogarden-server/pkg/packets"
)

type InGameState struct {
	client                 ClientInterfacer
	cancelPlayerUpdateLoop context.CancelFunc
}

func (s *InGameState) Name() string                 { return "ingame" }
func (s *InGameState) SetClient(client ClientInterfacer) { s.client = client }

func (s *InGameState) OnEnter() {
	playerData, err := s.client.DbTx().GetPlayerByUserId(context.Background(), s.client.GetUserID())
	if err != nil {
		s.client.Logger().Printf("Error fetching player data: %v", err)
		s.client.Close("player data not found")
		return
	}
	player := objects.NewPlayer(s.client.GetUserID(), s.client.GetUsername(), int32(playerData.Color))
	player.DbId = playerData.ID
	player.X, player.Y = SpawnCoords(player.Radius, s.client.SharedGameObjects().Players)
	s.client.SharedGameObjects().Players.AddWithId(s.client.Id(), player)
	s.client.Logger().Printf("Player '%s' spawned for client %d.", player.Name, s.client.Id())
	
	// FIXED: Do NOT automatically send the state. Wait for the client to ask for it.
	// go s.sendInitialMinigameState()

	ctx, cancel := context.WithCancel(context.Background())
	s.cancelPlayerUpdateLoop = cancel
	go s.playerUpdateLoop(ctx)
}

func (s *InGameState) sendInitialMinigameState() {
	statePacket := &packets.MinigameState{}
	
	s.client.SharedGameObjects().Players.ForEach(func(id uint64, p *objects.Player) {
		statePacket.Players = append(statePacket.Players, &packets.PlayerMessage{
			Id: id, Name: p.Name, X: p.X, Y: p.Y, Radius: p.Radius,
			Direction: p.Direction, Speed: p.Speed, Color: p.Color,
		})
	})
	s.client.SharedGameObjects().Spores.ForEach(func(id uint64, sp *objects.Spore) {
		statePacket.Spores = append(statePacket.Spores, &packets.SporeMessage{
			Id: id, X: sp.X, Y: sp.Y, Radius: sp.Radius,
		})
	})
	
	s.client.SocketSend(&packets.Packet{Msg: &packets.Packet_MinigameState{MinigameState: statePacket}})
	s.client.Logger().Printf("Sent initial minigame state to client %d.", s.client.Id())
}


func (s *InGameState) playerUpdateLoop(ctx context.Context) {
	ticker := time.NewTicker(16 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			player, ok := s.client.SharedGameObjects().Players.Get(s.client.Id())
			if !ok {
				return
			}
			player.X += player.Speed * math.Cos(player.Direction) * 0.016
			player.Y += player.Speed * math.Sin(player.Direction) * 0.016
		case <-ctx.Done():
			return
		}
	}
}

func (s *InGameState) HandleMessage(senderId uint64, packet *packets.Packet) {
	switch msg := packet.Msg.(type) {
	// FIXED: Add a handler for the new request from the client.
	case *packets.Packet_RequestMinigameState:
		go s.sendInitialMinigameState()
	case *packets.Packet_PlayerDirection:
		player, ok := s.client.SharedGameObjects().Players.Get(s.client.Id())
		if ok {
			player.Direction = msg.PlayerDirection.GetDirection()
		}
	case *packets.Packet_SporeConsumed:
		s.client.GetHub().HandleSporeConsumed(s.client, msg.SporeConsumed)
	case *packets.Packet_PlayerConsumed:
		s.client.GetHub().HandlePlayerConsumed(s.client, msg.PlayerConsumed)
	case *packets.Packet_Chat:
		s.client.GetHub().HandleChatMessage(s.client, msg.Chat)
	case *packets.Packet_LeaveMinigameRequest:
		s.client.SetState(&Connected{})
	// ... (other cases remain the same)
	}
}

func (s *InGameState) OnExit() {
	if s.cancelPlayerUpdateLoop != nil {
		s.cancelPlayerUpdateLoop()
	}
	s.client.SharedGameObjects().Players.Remove(s.client.Id())
	s.client.Logger().Printf("Player for client %d removed from game.", s.client.Id())
}