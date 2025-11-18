package server

import (
	"context"
	"chaogarden-server/internal/server/db"
	"chaogarden-server/pkg/packets"
	"encoding/json"
)

type Connected struct {
	client ClientInterfacer
}

func (c *Connected) Name() string { return "Connected" }
func (c *Connected) SetClient(client ClientInterfacer) { c.client = client }

func (c *Connected) OnEnter() {
	c.client.GetHub().NotifyFriendsOfStatusChange(c.client, true)
}
func (c *Connected) OnExit() {}

func (c *Connected) HandleMessage(senderId uint64, packet *packets.Packet) {
	switch msg := packet.Msg.(type) {
	case *packets.Packet_SaveDataRequest:
		c.handleSaveData(msg.SaveDataRequest)
	case *packets.Packet_AdminBanRequest:
		c.client.GetHub().HandleAdminBanRequest(c.client, msg.AdminBanRequest)
	case *packets.Packet_AdminGiveItemRequest:
		c.client.GetHub().HandleAdminGiveItem(c.client, msg.AdminGiveItemRequest)
	case *packets.Packet_Chat:
		c.client.GetHub().HandleChatMessage(c.client, msg.Chat)
	case *packets.Packet_EnterMinigameRequest: 
		c.client.SetState(&InGameState{})
	case *packets.Packet_HiscoresRequest:
		c.client.Logger().Println("Received HiscoresRequest from client.")
		c.client.GetHub().HandleHiscoreBoardRequest(c.client)
	case *packets.Packet_FriendRequestList:
		c.client.GetHub().HandleFriendRequestList(c.client)
	case *packets.Packet_FriendRequestSend:
		c.client.GetHub().HandleFriendRequestSend(c.client, msg.FriendRequestSend)
	case *packets.Packet_FriendRequestResponse:
		c.client.Logger().Printf("Connected state: Received FriendRequestResponse from client '%s' for user '%s'.", c.client.GetUsername(), msg.FriendRequestResponse.TargetUsername)
		c.client.GetHub().HandleFriendRequestResponse(c.client, msg.FriendRequestResponse)
	case *packets.Packet_FriendRemove:
		c.client.GetHub().HandleFriendRemove(c.client, msg.FriendRemove)
	case *packets.Packet_TradeRequestSend:
		c.client.GetHub().HandleTradeRequest(c.client, msg.TradeRequestSend)
	case *packets.Packet_TradeRequestResponse:
		c.client.GetHub().HandleTradeResponse(c.client, msg.TradeRequestResponse)
	case *packets.Packet_TradeUpdate:
		c.client.GetHub().HandleTradeUpdate(c.client, msg.TradeUpdate)
	case *packets.Packet_TradeAccept:
		c.client.GetHub().HandleTradeAccept(c.client, msg.TradeAccept)
	case *packets.Packet_TradeCancel:
		c.client.GetHub().HandleTradeCancel(c.client, msg.TradeCancel)
	}
}

func (c *Connected) handleSaveData(req *packets.SaveDataRequest) {
	if c.client.GetUserID() == 0 {
		return
	}
	queries := c.client.DbTx()
	ctx := context.Background()
	jsonData := json.RawMessage(req.JsonData)
	err := queries.UpdateUserData(ctx, db.UpdateUserDataParams{
		ID: c.client.GetUserID(),
		BaomainData: jsonData,
	})
	if err != nil {
		c.client.Logger().Printf("Failed to save user data: %v", err)
	}
}