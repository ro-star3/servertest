package server

import (
	"chaogarden-server/internal/server/world"
	"chaogarden-server/pkg/packets"
	"context"
	"encoding/json"
)

type OverworldState struct {
	hub *Hub
}

func (s *OverworldState) HandleLoginRequest(client *Client, req *packets.LoginRequest) {
	client.logger.Println("Received login request while in overworld. Ignoring.")
}

func (s *OverworldState) OnEnter(client *Client) {
	client.logger.Println("Client entering Overworld state.")
	hub := client.GetHub()

	// --- THIS IS THE NEW PART ---
	// 1. Immediately tell the client its unique world ID.
	// This is the most reliable way for the client to identify itself.
	idPacket := &packets.IdMessage{
		Id: client.Id(),
	}
	hub.SendMessage(client, 3, idPacket) // 3 = ID_MESSAGE
	client.logger.Printf("Sent ID_MESSAGE with world ID %d to client.", client.Id())
	// ---------------------------

	queries, tx, err := client.DbTx()
	if err != nil {
		client.logger.Printf("Could not begin transaction: %v", err)
		client.Close("db error")
		return
	}
	defer tx.Rollback()

	user, err := queries.GetUserByID(context.Background(), client.GetUserID())
	if err != nil {
		client.logger.Printf("Could not get user data for overworld: %v", err)
		client.Close("user data error")
		return
	}

	saveData := make(map[string]interface{})
	if user.BaomainData != nil && len(user.BaomainData) > 2 {
		if err := json.Unmarshal(user.BaomainData, &saveData); err != nil {
			client.logger.Printf("Error unmarshalling player save data: %v", err)
		}
	}

	mapID, _ := saveData["current_map_id"].(string)
	if mapID == "" {
		mapID = "PalletTown"
	}
	xVal, okX := saveData["x"].(float64)
	yVal, okY := saveData["y"].(float64)
	if !okX || !okY {
		xVal = 100
		yVal = 100
	}

	player := &world.Player{
		ID:       client.Id(),
		Username: client.GetUsername(),
		MapID:    mapID,
		X:        xVal,
		Y:        yVal,
	}
	hub.World.AddPlayer(player)

	mapDataPacket := &packets.MapData{
		MapId:   player.MapID,
		Players: []*packets.OtherPlayer{},
	}

	hub.World.RangePlayers(func(p *world.Player) bool {
		if p.MapID == player.MapID {
			mapDataPacket.Players = append(mapDataPacket.Players, &packets.OtherPlayer{
				PlayerId: p.ID,
				Username: p.Username,
				X:        int32(p.X),
				Y:        int32(p.Y),
			})
		}
		return true
	})

	hub.SendMessage(client, 51, mapDataPacket)
	client.logger.Printf("Player %s spawned at %s and sent initial map data.", player.Username, player.MapID)
}

func (s *OverworldState) HandlePlayerMove(client *Client, req *packets.PlayerMoveRequest) {
	hub := client.GetHub()

	ok := hub.World.UpdatePlayerPosition(client.Id(), float64(req.TargetX), float64(req.TargetY), int(req.Direction))
	if !ok {
		client.logger.Println("Attempted to move a player not found in the world.")
		return
	}

	player, _ := hub.World.GetPlayer(client.Id())

	positionUpdatePacket := &packets.PlayerPositionUpdate{
		PlayerId:  player.ID,
		NewX:      req.TargetX,
		NewY:      req.TargetY,
		Direction: req.Direction,
	}

	hub.World.RangePlayers(func(otherPlayer *world.Player) bool {
		if otherPlayer.MapID == player.MapID {
			if otherClient, ok := hub.Clients.Load(otherPlayer.ID); ok {
				hub.SendMessage(otherClient, 53, positionUpdatePacket)
			}
		}
		return true
	})
}