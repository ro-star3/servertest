package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"strings"
	"time"

	"chaogarden-server/internal/server/db"
	"chaogarden-server/internal/server/objects"
	"chaogarden-server/pkg/packets"
)

type PlayerSaveData struct {
	Rings     int64             `json:"rings"`
	Inventory map[string]int64 `json:"inventory"`
	ChaoData  []ChaoSaveData    `json:"chao_data"`
}

type ChaoSaveData struct {
	ChaoID   int64  `json:"chao_id"`
	ChaoName string `json:"chao_name"`
	Rarity   string `json:"rarity"`
}

var ItemRegistry = map[string]bool{
	"rings":    true,
	"toy_ball": true,
	"nut":      true,
	"egg":      true,
}

const (
	MaxSpores        = 1000
	ValidationBuffer = 10.0
)
type DbTx struct {
	Ctx     context.Context
	Queries *db.Queries
}
type SharedGameObjects struct {
	Players *objects.SharedCollection[*objects.Player]
	Spores  *objects.SharedCollection[*objects.Spore]
}
type ClientInterfacer interface {
	Id() uint64
	GetUserID() int64
	SetUserID(id int64)
	GetUsername() string
	SetUsername(name string)
	GetIP() string
	Initialize(id uint64, ip string)
	SetState(newState ClientStateHandler)
	ProcessMessage(senderId uint64, packet *packets.Packet)
	SocketSend(packet *packets.Packet)
	Broadcast(packet *packets.Packet)
	WritePump() chan<- *packets.Packet
	DbTx() *db.Queries
	SharedGameObjects() *SharedGameObjects
	Logger() *log.Logger
	Close(reason string)
	GetHub() *Hub
}
type Hub struct {
	Clients           *objects.SharedCollection[ClientInterfacer]
	BroadcastChan     chan *packets.Packet
	RegisterChan      chan ClientInterfacer
	UnregisterChan    chan ClientInterfacer
	dbPool            *sql.DB
	SharedGameObjects *SharedGameObjects
	ActiveTrades      map[int64]*TradeState
}
type ClientStateHandler interface {
	Name() string
	SetClient(client ClientInterfacer)
	OnEnter()
	HandleMessage(senderId uint64, packet *packets.Packet)
	OnExit()
}

type TradeState struct {
	PlayerOneID       int64
	PlayerTwoID       int64
	PlayerOneOffer    *packets.TradeUpdate
	PlayerTwoOffer    *packets.TradeUpdate
	PlayerOneLockedIn bool
	PlayerTwoLockedIn bool
}

func (h *Hub) NewDbTx() *DbTx {
	return &DbTx{Ctx: context.Background(), Queries: db.New(h.dbPool)}
}

func NewHub(dataDirPath string, databaseUrl string) *Hub {
	dbPool, err := sql.Open("mysql", databaseUrl)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}

	if err = dbPool.Ping(); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}
	log.Println("Successfully connected to the MySQL database.")

	return &Hub{
		Clients:           objects.NewSharedCollection[ClientInterfacer](),
		BroadcastChan:     make(chan *packets.Packet, 256),
		RegisterChan:      make(chan ClientInterfacer),
		UnregisterChan:    make(chan ClientInterfacer),
		dbPool:            dbPool,
		SharedGameObjects: &SharedGameObjects{
			Players: objects.NewSharedCollection[*objects.Player](),
			Spores:  objects.NewSharedCollection[*objects.Spore](),
		},
		ActiveTrades: make(map[int64]*TradeState),
	}
}

func (h *Hub) Run() {
	log.Println("[HUB] Run() started. Starting goroutines...")
	go h.runSporeSpawner()
	go h.BroadcastPlayerPositions()
	log.Println("[HUB] Goroutines started. Entering main select loop.")
	for {
		select {
		case client := <-h.RegisterChan:
			h.Clients.AddWithId(client.Id(), client)
			client.Logger().Printf("Client %d connected", client.Id())
		case client := <-h.UnregisterChan:
			if _, ok := h.Clients.Get(client.Id()); ok {
				h.Clients.Remove(client.Id())
			}
			h.NotifyFriendsOfStatusChange(client, false)

		case packet := <-h.BroadcastChan:
			h.Clients.ForEach(func(_ uint64, client ClientInterfacer) {
				select {
				case client.WritePump() <- packet:
				default:
					client.Close("write buffer full")
				}
			})
		}
	}
}

func (h *Hub) BroadcastPlayerPositions() {
	log.Println("[HUB] Player Position broadcast loop started.")
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		if h.SharedGameObjects.Players.Len() == 0 {
			continue
		}
		h.SharedGameObjects.Players.ForEach(func(playerID uint64, player *objects.Player) {
			playerPacket := &packets.Packet{
				SenderId: playerID,
				Msg: &packets.Packet_PlayerMessage{
					PlayerMessage: &packets.PlayerMessage{
						Id: playerID, Name: player.Name, X: player.X, Y: player.Y,
						Radius: player.Radius, Direction: player.Direction, Speed: player.Speed, Color: player.Color,
					},
				},
			}
			h.BroadcastChan <- playerPacket
		})
	}
}

func (h *Hub) runSporeSpawner() {
	log.Println("[HUB] Spore replenishing loop started.")
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		if h.SharedGameObjects.Players.Len() == 0 {
			continue
		}

		sporesRemaining := h.SharedGameObjects.Spores.Len()
		diff := MaxSpores - sporesRemaining

		if diff <= 0 {
			continue
		}

		log.Printf("%d spores remain - going to replenish %d spores", sporesRemaining, diff)

		for i := 0; i < min(diff, 10); i++ {
			spore := objects.NewSpore(
				(rand.Float64()*2-1)*3000,
				(rand.Float64()*2-1)*3000,
				rand.Float64()*5+5,
			)
			h.SharedGameObjects.Spores.AddWithId(spore.ID, spore)

			sporePacket := &packets.Packet{
				SenderId: 0,
				Msg: &packets.Packet_Spore{
					Spore: &packets.SporeMessage{
						Id: spore.ID, X: spore.X, Y: spore.Y, Radius: spore.Radius,
					},
				},
			}
			h.BroadcastChan <- sporePacket
		}
	}
}

func (h *Hub) HandleHiscoreBoardRequest(client ClientInterfacer) {
	queries := h.NewDbTx().Queries
	ctx := context.Background()

	scores, err := queries.GetTopScores(ctx, db.GetTopScoresParams{
		Limit:  10,
		Offset: 0,
	})
	if err != nil {
		log.Printf("Error fetching top scores: %v", err)
		return
	}
	
	log.Printf("[HUB] Found %d scores in the database to send to client %d.", len(scores), client.Id())

	hiscoreBoard := &packets.MinigameState{}
	for i, score := range scores {
		radius := math.Sqrt(float64(score.BestScore) / math.Pi)

		hiscoreBoard.Players = append(hiscoreBoard.Players, &packets.PlayerMessage{
			Id:     uint64(i + 1), 
			Name:   score.Name,
			Radius: radius,
		})
	}

	client.SocketSend(&packets.Packet{Msg: &packets.Packet_MinigameState{MinigameState: hiscoreBoard}})
}

// --- Trade System Handlers ---
func (h *Hub) HandleTradeRequest(requestingClient ClientInterfacer, req *packets.TradeRequestSend) {
	var targetClient ClientInterfacer
	
	h.Clients.ForEach(func(_ uint64, c ClientInterfacer) {
		if strings.EqualFold(c.GetUsername(), req.TargetUsername) {
			targetClient = c
		}
	})

	if targetClient == nil {
		return
	}

	forwardedPacket := &packets.Packet{
		Msg: &packets.Packet_TradeRequestReceive{
			TradeRequestReceive: &packets.TradeRequestReceive{
				FromUsername: requestingClient.GetUsername(),
			},
		},
	}
	targetClient.SocketSend(forwardedPacket)
}

func (h *Hub) HandleTradeResponse(respondingClient ClientInterfacer, res *packets.TradeRequestResponse) {
	var originalRequester ClientInterfacer
	h.Clients.ForEach(func(_ uint64, c ClientInterfacer) {
		if strings.EqualFold(c.GetUsername(), res.TargetUsername) {
			originalRequester = c
		}
	})

	if originalRequester == nil {
		return
	}

	if res.Accept {
		tradeState := &TradeState{
			PlayerOneID: originalRequester.GetUserID(),
			PlayerTwoID: respondingClient.GetUserID(),
			PlayerOneOffer: &packets.TradeUpdate{},
			PlayerTwoOffer: &packets.TradeUpdate{},
		}
		h.ActiveTrades[originalRequester.GetUserID()] = tradeState
		h.ActiveTrades[respondingClient.GetUserID()] = tradeState

		startPacket1 := &packets.Packet{
			Msg: &packets.Packet_TradeStart{
				TradeStart: &packets.TradeStart{
					OtherUsername: respondingClient.GetUsername(),
				},
			},
		}
		originalRequester.SocketSend(startPacket1)

		startPacket2 := &packets.Packet{
			Msg: &packets.Packet_TradeStart{
				TradeStart: &packets.TradeStart{
					OtherUsername: originalRequester.GetUsername(),
				},
			},
		}
		respondingClient.SocketSend(startPacket2)
	} else {
		declineMsg := &packets.Packet{
			Msg: &packets.Packet_TradeCancel{
				TradeCancel: &packets.TradeCancel{
					Reason: respondingClient.GetUsername() + " declined the trade request.",
				},
			},
		}
		originalRequester.SocketSend(declineMsg)
	}
}

func (h *Hub) HandleTradeUpdate(client ClientInterfacer, update *packets.TradeUpdate) {
	tradeState, ok := h.ActiveTrades[client.GetUserID()]
	if !ok {
		client.Logger().Println("Received TradeUpdate for a non-existent trade.")
		return
	}
	
	var otherPlayerID int64
	var myOffer *packets.TradeUpdate

	if client.GetUserID() == tradeState.PlayerOneID {
		tradeState.PlayerOneOffer = update
		myOffer = tradeState.PlayerOneOffer
		otherPlayerID = tradeState.PlayerTwoID
	} else {
		tradeState.PlayerTwoOffer = update
		myOffer = tradeState.PlayerTwoOffer
		otherPlayerID = tradeState.PlayerOneID
	}
	
	otherClient, ok := h.Clients.Get(uint64(otherPlayerID))
	if !ok {
		client.Logger().Println("Other trade client not found, cancelling trade.")
		h.CancelTrade(tradeState, "The other player disconnected.")
		return
	}
	
	updatePacket := &packets.Packet{
		Msg: &packets.Packet_TradeUpdate{
			TradeUpdate: myOffer,
		},
	}
	otherClient.SocketSend(updatePacket)
}

func (h *Hub) HandleTradeAccept(client ClientInterfacer, accept *packets.TradeAccept) {
	tradeState, ok := h.ActiveTrades[client.GetUserID()]
	if !ok {
		client.Logger().Println("Received TradeAccept for a non-existent trade.")
		return
	}
	
	var otherPlayerID int64
	var myOffer *packets.TradeUpdate
	var otherOffer *packets.TradeUpdate

	if client.GetUserID() == tradeState.PlayerOneID {
		tradeState.PlayerOneLockedIn = accept.IsLockedIn
		otherPlayerID = tradeState.PlayerTwoID
		myOffer = tradeState.PlayerOneOffer
		otherOffer = tradeState.PlayerTwoOffer
	} else {
		tradeState.PlayerTwoLockedIn = accept.IsLockedIn
		otherPlayerID = tradeState.PlayerOneID
		myOffer = tradeState.PlayerTwoOffer
		otherOffer = tradeState.PlayerOneOffer
	}

	otherClient, ok := h.Clients.Get(uint64(otherPlayerID))
	if !ok {
		client.Logger().Println("Other trade client not found, cancelling trade.")
		h.CancelTrade(tradeState, "The other player disconnected.")
		return
	}
	
	otherUpdatePacket := &packets.Packet{
		Msg: &packets.Packet_TradeUpdate{
			TradeUpdate: &packets.TradeUpdate{
				IsLockedIn: accept.IsLockedIn,
				Rings:      myOffer.Rings,
				Items:      myOffer.Items,
				Chao:       myOffer.Chao,
			},
		},
	}
	otherClient.SocketSend(otherUpdatePacket)
	
	myUpdatePacket := &packets.Packet{
		Msg: &packets.Packet_TradeUpdate{
			TradeUpdate: &packets.TradeUpdate{
				IsLockedIn: accept.IsLockedIn,
				Rings:      otherOffer.Rings,
				Items:      otherOffer.Items,
				Chao:       otherOffer.Chao,
			},
		},
	}
	client.SocketSend(myUpdatePacket)

	if tradeState.PlayerOneLockedIn && tradeState.PlayerTwoLockedIn {
		h.finalizeTrade(tradeState)
	}
}

func (h *Hub) HandleTradeCancel(client ClientInterfacer, req *packets.TradeCancel) {
	tradeState, ok := h.ActiveTrades[client.GetUserID()]
	if !ok {
		client.Logger().Println("Received TradeCancel for a non-existent trade.")
		return
	}

	h.CancelTrade(tradeState, req.Reason)
}

func (h *Hub) CancelTrade(tradeState *TradeState, reason string) {
	if client1, ok := h.Clients.Get(uint64(tradeState.PlayerOneID)); ok {
		cancelPacket := &packets.Packet{
			Msg: &packets.Packet_TradeCancel{
				TradeCancel: &packets.TradeCancel{Reason: reason},
			},
		}
		client1.SocketSend(cancelPacket)
	}
	if client2, ok := h.Clients.Get(uint64(tradeState.PlayerTwoID)); ok {
		cancelPacket := &packets.Packet{
			Msg: &packets.Packet_TradeCancel{
				TradeCancel: &packets.TradeCancel{Reason: reason},
			},
		}
		client2.SocketSend(cancelPacket)
	}
	delete(h.ActiveTrades, tradeState.PlayerOneID)
	delete(h.ActiveTrades, tradeState.PlayerTwoID)
}

func (h *Hub) finalizeTrade(tradeState *TradeState) {
    player1Client, ok1 := h.Clients.Get(uint64(tradeState.PlayerOneID))
    player2Client, ok2 := h.Clients.Get(uint64(tradeState.PlayerTwoID))
    if !ok1 || !ok2 {
        h.CancelTrade(tradeState, "One or more players disconnected.")
        return
    }

    ctx := context.Background()
    queries := h.NewDbTx().Queries

    user1, err1 := queries.GetUserByID(ctx, tradeState.PlayerOneID)
    user2, err2 := queries.GetUserByID(ctx, tradeState.PlayerTwoID)
    if err1 != nil || err2 != nil {
        h.CancelTrade(tradeState, "Failed to fetch user data for trade.")
        return
    }

    var saveData1, saveData2 PlayerSaveData
	if user1.BaomainData != nil {
		json.Unmarshal(user1.BaomainData, &saveData1)
	}
	if user2.BaomainData != nil {
		json.Unmarshal(user2.BaomainData, &saveData2)
	}

    if saveData1.Inventory == nil {
        saveData1.Inventory = make(map[string]int64)
    }
    if saveData2.Inventory == nil {
        saveData2.Inventory = make(map[string]int64)
    }
	if saveData1.ChaoData == nil {
		saveData1.ChaoData = make([]ChaoSaveData, 0)
	}
	if saveData2.ChaoData == nil {
		saveData2.ChaoData = make([]ChaoSaveData, 0)
	}


    h.processTradeOffers(tradeState.PlayerOneOffer, &saveData1, tradeState.PlayerTwoOffer, &saveData2)
    h.processTradeOffers(tradeState.PlayerTwoOffer, &saveData2, tradeState.PlayerOneOffer, &saveData1)

    updatedData1, _ := json.Marshal(saveData1)
    updatedData2, _ := json.Marshal(saveData2)
    queries.UpdateUserData(ctx, db.UpdateUserDataParams{ID: user1.ID, BaomainData: updatedData1})
    queries.UpdateUserData(ctx, db.UpdateUserDataParams{ID: user2.ID, BaomainData: updatedData2})
    
    updatePacket1 := &packets.Packet{
        Msg: &packets.Packet_PlayerDataUpdate{
            PlayerDataUpdate: &packets.PlayerDataUpdate{
                BaomainJsonData: string(updatedData1),
            },
        },
    }
    player1Client.SocketSend(updatePacket1)

    updatePacket2 := &packets.Packet{
        Msg: &packets.Packet_PlayerDataUpdate{
            PlayerDataUpdate: &packets.PlayerDataUpdate{
                BaomainJsonData: string(updatedData2),
            },
        },
    }
    player2Client.SocketSend(updatePacket2)

    successPacket := &packets.Packet{
        Msg: &packets.Packet_Chat{
            Chat: &packets.ChatMessage{
                Msg: "Trade finalized successfully!",
            },
        },
    }
    player1Client.SocketSend(successPacket)
    player2Client.SocketSend(successPacket)

    delete(h.ActiveTrades, tradeState.PlayerOneID)
    delete(h.ActiveTrades, tradeState.PlayerTwoID)
}

func (h *Hub) processTradeOffers(senderOffer *packets.TradeUpdate, senderSaveData *PlayerSaveData, receiverOffer *packets.TradeUpdate, receiverSaveData *PlayerSaveData) {
    senderSaveData.Rings -= senderOffer.Rings
    for _, item := range senderOffer.Items {
        senderSaveData.Inventory[item.ItemId] -= int64(item.Amount)
    }
	// Remove Chao from sender's data
    for _, chaoOffer := range senderOffer.Chao {
        for i, chao := range senderSaveData.ChaoData {
            if chao.ChaoID == chaoOffer.ChaoId {
                senderSaveData.ChaoData = append(senderSaveData.ChaoData[:i], senderSaveData.ChaoData[i+1:]...)
                break
            }
        }
    }

    receiverSaveData.Rings += senderOffer.Rings
    for _, item := range senderOffer.Items {
        receiverSaveData.Inventory[item.ItemId] += int64(item.Amount)
    }
	// Add Chao to receiver's data
	for _, chaoOffer := range senderOffer.Chao {
		receiverSaveData.ChaoData = append(receiverSaveData.ChaoData, ChaoSaveData{
			ChaoID: chaoOffer.ChaoId,
			ChaoName: chaoOffer.ChaoName,
			Rarity: chaoOffer.Rarity,
		})
	}
}

func (h *Hub) HandleFriendRequestList(client ClientInterfacer) {
	queries := h.NewDbTx().Queries
	ctx := context.Background()

	dbFriends, err := queries.GetFriendshipsForUser(ctx, db.GetFriendshipsForUserParams{
		UserOneID:   client.GetUserID(),
		UserOneID_2: client.GetUserID(),
		UserTwoID:   client.GetUserID(),
	})
	if err != nil {
		log.Printf("Error fetching friendships for user %d: %v", client.GetUserID(), err)
		return
	}

	friendList := &packets.FriendListUpdate{}
	for _, dbFriend := range dbFriends {
		isOnline := false
		h.Clients.ForEach(func(_ uint64, c ClientInterfacer) {
			if c.GetUserID() == dbFriend.User.ID {
				isOnline = true
			}
		})

		var status string
		if dbFriend.Status == "pending" {
			if dbFriend.ActionUserID == client.GetUserID() {
				status = "pending_sent"
			} else {
				status = "pending_received"
			}
		} else {
			status = dbFriend.Status
		}

		friendList.Friends = append(friendList.Friends, &packets.FriendListUpdate_Friend{
			Username: dbFriend.User.Username,
			IsOnline: isOnline,
			Status:   status,
		})
	}

	client.SocketSend(&packets.Packet{Msg: &packets.Packet_FriendListUpdate{FriendListUpdate: friendList}})
}

func (h *Hub) HandleFriendRequestSend(client ClientInterfacer, req *packets.FriendRequestSend) {
	queries := h.NewDbTx().Queries
	ctx := context.Background()

	targetUser, err := queries.GetUserByUsername(ctx, strings.ToLower(req.TargetUsername))
	if err != nil {
		return
	}

	if targetUser.ID == client.GetUserID() {
		return
	}

	userOneID := min(client.GetUserID(), targetUser.ID)
	userTwoID := max(client.GetUserID(), targetUser.ID)

	err = queries.CreateFriendRequest(ctx, db.CreateFriendRequestParams{
		UserOneID:    userOneID,
		UserTwoID:    userTwoID,
		ActionUserID: client.GetUserID(),
	})
	if err != nil {
		log.Printf("Error creating friend request: %v", err)
		return
	}

	h.NotifyUserOfFriendUpdate(targetUser.ID)
	h.NotifyUserOfFriendUpdate(client.GetUserID())
}

func (h *Hub) HandleFriendRequestResponse(client ClientInterfacer, req *packets.FriendRequestResponse) {
	queries := h.NewDbTx().Queries
	ctx := context.Background()

	targetUser, err := queries.GetUserByUsername(ctx, strings.ToLower(req.TargetUsername))
	if err != nil {
		return
	}

	userOneID := min(client.GetUserID(), targetUser.ID)
	userTwoID := max(client.GetUserID(), targetUser.ID)

	if req.Accept {
		err = queries.UpdateFriendshipStatus(ctx, db.UpdateFriendshipStatusParams{
			Status:       "accepted",
			ActionUserID: client.GetUserID(),
			UserOneID:    userOneID,
			UserTwoID:    userTwoID,
		})
	} else {
		err = queries.DeleteFriendship(ctx, db.DeleteFriendshipParams{
			UserOneID: userOneID,
			UserTwoID: userTwoID,
		})
	}
	if err != nil {
		log.Printf("Error responding to friend request: %v", err)
		return
	}

	h.NotifyUserOfFriendUpdate(targetUser.ID)
	h.NotifyUserOfFriendUpdate(client.GetUserID())
}

func (h *Hub) HandleFriendRemove(client ClientInterfacer, req *packets.FriendRemove) {
	queries := h.NewDbTx().Queries
	ctx := context.Background()

	targetUser, err := queries.GetUserByUsername(ctx, strings.ToLower(req.TargetUsername))
	if err != nil {
		return
	}

	userOneID := min(client.GetUserID(), targetUser.ID)
	userTwoID := max(client.GetUserID(), targetUser.ID)

	err = queries.DeleteFriendship(ctx, db.DeleteFriendshipParams{
		UserOneID: userOneID,
		UserTwoID: userTwoID,
	})
	if err != nil {
		log.Printf("Error removing friend: %v", err)
		return
	}

	h.NotifyUserOfFriendUpdate(targetUser.ID)
	h.NotifyUserOfFriendUpdate(client.GetUserID())
}

func (h *Hub) NotifyUserOfFriendUpdate(userID int64) {
	h.Clients.ForEach(func(_ uint64, c ClientInterfacer) {
		if c.GetUserID() == userID {
			h.HandleFriendRequestList(c)
		}
	})
}

func (h *Hub) NotifyFriendsOfStatusChange(client ClientInterfacer, isOnline bool) {
	queries := h.NewDbTx().Queries
	ctx := context.Background()

	dbFriends, err := queries.GetFriendshipsForUser(ctx, db.GetFriendshipsForUserParams{
		UserOneID:   client.GetUserID(),
		UserOneID_2: client.GetUserID(),
		UserTwoID:   client.GetUserID(),
	})
	if err != nil {
		return
	}

	for _, friend := range dbFriends {
		if friend.Status == "accepted" {
			h.NotifyUserOfFriendUpdate(friend.User.ID)
		}
	}
}

func (h *Hub) HandleChatMessage(client ClientInterfacer, chat *packets.ChatMessage) {
	if len(chat.Msg) > 256 {
		return
	}
	formattedText := client.GetUsername() + ": " + chat.Msg
	chatPacket := &packets.Packet{
		SenderId: client.Id(),
		Msg:      &packets.Packet_Chat{Chat: &packets.ChatMessage{Msg: formattedText}},
	}
	h.BroadcastChan <- chatPacket
}

func (h *Hub) HandleAdminGiveItem(client ClientInterfacer, req *packets.AdminGiveItemRequest) {
	sendAdminResponse := func(success bool, message string) {
		resp := &packets.Packet{
			Msg: &packets.Packet_AdminActionResponse{
				AdminActionResponse: &packets.AdminActionResponse{
					Success:      success,
					ResponseText: message,
				},
			},
		}
		client.SocketSend(resp)
	}

	queries := h.NewDbTx().Queries
	ctx := context.Background()

	adminUser, err := queries.GetUserByID(ctx, client.GetUserID())
	if err != nil || !adminUser.IsAdmin {
		sendAdminResponse(false, "You do not have permission.")
		return
	}
	if req.Amount <= 0 {
		sendAdminResponse(false, "Amount must be a positive number.")
		return
	}
	if _, ok := ItemRegistry[req.ItemId]; !ok {
		sendAdminResponse(false, fmt.Sprintf("Item '%s' not found.", req.ItemId))
		return
	}
	targetUser, err := queries.GetUserByUsername(ctx, strings.ToLower(req.TargetUsername))
	if err != nil {
		sendAdminResponse(false, fmt.Sprintf("User '%s' not found.", req.TargetUsername))
		return
	}

	var saveData PlayerSaveData
	if targetUser.BaomainData != nil {
		if err := json.Unmarshal(targetUser.BaomainData, &saveData); err != nil {
			log.Printf("WARN: Could not unmarshal data for user '%s', proceeding with fresh data. Error: %v", targetUser.Username, err)
			saveData = PlayerSaveData{}
		}
	}

	if saveData.Inventory == nil {
		saveData.Inventory = make(map[string]int64)
	}

	if req.ItemId == "rings" {
		saveData.Rings += int64(req.Amount)
	} else {
		saveData.Inventory[req.ItemId] += int64(req.Amount)
	}

	updatedData, err := json.Marshal(saveData)
	if err != nil {
		sendAdminResponse(false, "Failed to serialize updated player data.")
		return
	}

	err = queries.UpdateUserData(ctx, db.UpdateUserDataParams{
		ID:          targetUser.ID,
		BaomainData: updatedData,
	})
	if err != nil {
		sendAdminResponse(false, "Failed to save updated player data to the database.")
		return
	}

	h.Clients.ForEach(func(_ uint64, targetClient ClientInterfacer) {
		if targetClient.GetUserID() == targetUser.ID {
			updatePacket := &packets.Packet{
				Msg: &packets.Packet_PlayerDataUpdate{
					PlayerDataUpdate: &packets.PlayerDataUpdate{
						BaomainJsonData: string(updatedData),
					},
				},
			}
			targetClient.SocketSend(updatePacket)
		}
	})

	sendAdminResponse(true, fmt.Sprintf("Gave %d x %s to %s.", req.Amount, req.ItemId, targetUser.Username))
	log.Printf("[ADMIN] %s gave %d x %s to %s", adminUser.Username, req.Amount, req.ItemId, targetUser.Username)
}

func (h *Hub) HandleAdminBanRequest(client ClientInterfacer, req *packets.AdminBanRequest) {
	sendAdminResponse := func(success bool, message string) {
		resp := &packets.Packet{
			Msg: &packets.Packet_AdminActionResponse{
				AdminActionResponse: &packets.AdminActionResponse{
					Success:      success,
					ResponseText: message,
				},
			},
		}
		client.SocketSend(resp)
	}

	queries := h.NewDbTx().Queries
	ctx := context.Background()

	adminUser, err := queries.GetUserByID(ctx, client.GetUserID())
	if err != nil || !adminUser.IsAdmin {
		sendAdminResponse(false, "You do not have permission.")
		return
	}

	targetUser, err := queries.GetUserByUsername(ctx, strings.ToLower(req.TargetUsername))
	if err != nil {
		sendAdminResponse(false, fmt.Sprintf("User '%s' not found.", req.TargetUsername))
		return
	}

	if targetUser.IsAdmin {
		sendAdminResponse(false, "You cannot ban another administrator.")
		return
	}

	var banExpiry sql.NullTime
	if req.DurationSeconds > 0 {
		banExpiry.Time = time.Now().Add(time.Duration(req.DurationSeconds) * time.Second)
		banExpiry.Valid = true
	}

	err = queries.CreateUserBan(ctx, db.CreateUserBanParams{
		BannedUserID: sql.NullInt64{Int64: targetUser.ID, Valid: true},
		AdminUserID:  sql.NullInt64{Int64: client.GetUserID(), Valid: true},
		Reason:       sql.NullString{String: req.Reason, Valid: true},
		ExpiresAt:    banExpiry,
	})
	if err != nil {
		sendAdminResponse(false, "Database error while creating ban.")
		client.Logger().Printf("Error creating ban: %v", err)
		return
	}

	var targetClient ClientInterfacer
	h.Clients.ForEach(func(_ uint64, c ClientInterfacer) {
		if c.GetUserID() == targetUser.ID {
			targetClient = c
		}
	})

	banMessage := fmt.Sprintf("User '%s' has been banned. Reason: %s", targetUser.Username, req.Reason)
	if targetClient != nil {
		queries.CreateIPBan(ctx, db.CreateIPBanParams{
			BannedIpAddress: sql.NullString{String: targetClient.GetIP(), Valid: true},
			AdminUserID:     sql.NullInt64{Int64: client.GetUserID(), Valid: true},
			Reason:          sql.NullString{String: "IP of banned user: " + targetUser.Username, Valid: true},
			ExpiresAt:       banExpiry,
		})
		targetClient.Close(banMessage)
	}

	sendAdminResponse(true, banMessage)
	log.Println("[ADMIN] " + banMessage)
}

func (h *Hub) HandleSporeConsumed(client ClientInterfacer, consume *packets.SporeConsumedMessage) {
	player, ok := h.SharedGameObjects.Players.Get(client.Id())
	if !ok {
		return
	}
	spore, ok := h.SharedGameObjects.Spores.Get(consume.SporeId)
	if !ok {
		return
	}
	dx := player.X - spore.X
	dy := player.Y - spore.Y
	distSq := dx*dx + dy*dy
	allowedRadius := player.Radius + spore.Radius + ValidationBuffer
	if distSq <= allowedRadius*allowedRadius {
		player.Radius = math.Sqrt(player.Radius*player.Radius + spore.Radius*spore.Radius)
		player.BestScore = int64(player.Radius * player.Radius * math.Pi)
		h.SharedGameObjects.Spores.Remove(consume.SporeId)

		consumedPacket := &packets.Packet{
			SenderId: client.Id(),
			Msg: &packets.Packet_SporeConsumed{
				SporeConsumed: &packets.SporeConsumedMessage{SporeId: consume.SporeId},
			},
		}
		h.BroadcastChan <- consumedPacket

		ctx := context.Background()
		h.NewDbTx().Queries.UpdatePlayerBestScore(ctx, db.UpdatePlayerBestScoreParams{
			BestScore: player.BestScore,
			ID:        player.DbId,
		})
	}
}

func (h *Hub) HandlePlayerConsumed(client ClientInterfacer, consume *packets.PlayerConsumedMessage) {
	player, ok := h.SharedGameObjects.Players.Get(client.Id())
	if !ok {
		return
	}
	other, ok := h.SharedGameObjects.Players.Get(consume.PlayerId)
	if !ok {
		return
	}
	if other.ID == player.ID {
		return
	}

	dx := player.X - other.X
	dy := player.Y - other.Y
	distSq := dx*dx + dy*dy
	allowedRadius := player.Radius + other.Radius + ValidationBuffer
	if distSq <= allowedRadius*allowedRadius && player.Radius > other.Radius*1.1 {
		player.Radius = math.Sqrt(player.Radius*player.Radius + other.Radius*other.Radius)
		player.BestScore = int64(player.Radius * player.Radius * math.Pi)
		h.SharedGameObjects.Players.Remove(consume.PlayerId)

		consumedPacket := &packets.Packet{
			SenderId: client.Id(),
			Msg: &packets.Packet_PlayerConsumed{
				PlayerConsumed: &packets.PlayerConsumedMessage{PlayerId: consume.PlayerId},
			},
		}
		h.BroadcastChan <- consumedPacket

		if otherClient, ok := h.Clients.Get(consume.PlayerId); ok {
			otherClient.SetState(&InGameState{})
		}
		ctx := context.Background()
		h.NewDbTx().Queries.UpdatePlayerBestScore(ctx, db.UpdatePlayerBestScoreParams{
			BestScore: player.BestScore,
			ID:        player.DbId,
		})
	}
}