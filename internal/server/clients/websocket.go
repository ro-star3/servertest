// internal/server/clients/websocket.go (Corrected and Complete)
package clients

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"chaogarden-server/internal/server"
	"chaogarden-server/internal/server/db"
	"chaogarden-server/internal/server/objects"
	"chaogarden-server/pkg/packets"

	"github.com/gorilla/websocket"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/proto"
)

// Initial state for new connections before authentication.
type Unauthenticated struct {
	client server.ClientInterfacer
}

func (u *Unauthenticated) Name() string                                  { return "Unauthenticated" }
func (u *Unauthenticated) SetClient(client server.ClientInterfacer)      { u.client = client }
func (u *Unauthenticated) OnEnter() {
	idPacket := &packets.Packet{Msg: &packets.Packet_Id{Id: &packets.IdMessage{Id: u.client.Id()}}}
	u.client.SocketSend(idPacket)
}
func (u *Unauthenticated) OnExit() {}
func (u *Unauthenticated) HandleMessage(senderId uint64, packet *packets.Packet) {
	switch msg := packet.Msg.(type) {
	case *packets.Packet_LoginRequest:
		u.handleLogin(msg.LoginRequest)
	case *packets.Packet_RegisterRequest:
		u.handleRegister(msg.RegisterRequest)
	default:
		u.client.Logger().Printf("Received unexpected packet type in Unauthenticated state: %T", msg)
	}
}

func (u *Unauthenticated) handleLogin(req *packets.LoginRequest) {
	queries := u.client.DbTx()
	ctx := context.Background()

	user, err := queries.GetUserByUsername(ctx, strings.ToLower(req.Username))
	if err != nil {
		u.client.SocketSend(&packets.Packet{Msg: &packets.Packet_DenyResponse{DenyResponse: &packets.DenyResponse{Reason: "Invalid username or password."}}})
		return
	}

	// BAN ENFORCEMENT
	_, err = queries.GetActiveBanForUser(ctx, sql.NullInt64{Int64: user.ID, Valid: true})
	if err == nil {
		u.client.SocketSend(&packets.Packet{Msg: &packets.Packet_DenyResponse{DenyResponse: &packets.DenyResponse{Reason: "This account is banned."}}})
		u.client.Close("banned user login attempt")
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		u.client.SocketSend(&packets.Packet{Msg: &packets.Packet_DenyResponse{DenyResponse: &packets.DenyResponse{Reason: "Invalid username or password."}}})
		return
	}

	u.client.SetUserID(user.ID)
	u.client.SetUsername(user.Username)
	playerData := &packets.Player{
		Id:              uint64(user.ID),
		Name:            user.Username,
		BaomainJsonData: string(user.BaomainData),
	}

	u.client.SocketSend(&packets.Packet{Msg: &packets.Packet_PlayerData{PlayerData: playerData}})
	u.client.Logger().Printf("User '%s' logged in successfully.", req.Username)
	u.client.SetState(&server.Connected{})
}

func (u *Unauthenticated) handleRegister(req *packets.RegisterRequest) {
	queries := u.client.DbTx()
	ctx := context.Background()

	if _, err := queries.GetUserByUsername(ctx, strings.ToLower(req.Username)); err == nil {
		u.client.SocketSend(&packets.Packet{Msg: &packets.Packet_DenyResponse{DenyResponse: &packets.DenyResponse{Reason: "Username is already taken."}}})
		return
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		u.client.SocketSend(&packets.Packet{Msg: &packets.Packet_DenyResponse{DenyResponse: &packets.DenyResponse{Reason: "Server error during registration."}}})
		return
	}

	userResult, err := queries.CreateUser(ctx, db.CreateUserParams{
		Username:          req.Username,
		PasswordHash:      string(hashedPassword),
		UsernameLowercase: strings.ToLower(req.Username),
		BaomainData:       json.RawMessage("{}"),
	})
	if err != nil {
		u.client.Logger().Printf("Error calling CreateUser: %v", err)
		u.client.SocketSend(&packets.Packet{Msg: &packets.Packet_DenyResponse{DenyResponse: &packets.DenyResponse{Reason: "Could not create user."}}})
		return
	}
	userID, err := userResult.LastInsertId()
	if err != nil {
		u.client.Logger().Printf("Error getting LastInsertId: %v", err)
		u.client.SocketSend(&packets.Packet{Msg: &packets.Packet_DenyResponse{DenyResponse: &packets.DenyResponse{Reason: "Could not retrieve user ID after registration."}}})
		return
	}

	err = queries.CreatePlayer(ctx, db.CreatePlayerParams{
		UserID: userID,
		Name:   req.Username,
		Color:  int32(req.Color),
	})
	if err != nil {
		u.client.Logger().Printf("Error calling CreatePlayer: %v", err)
		u.client.SocketSend(&packets.Packet{Msg: &packets.Packet_DenyResponse{DenyResponse: &packets.DenyResponse{Reason: "Could not create player data."}}})
		return
	}
	u.client.SocketSend(&packets.Packet{Msg: &packets.Packet_DenyResponse{DenyResponse: &packets.DenyResponse{Reason: "Registration successful! Please log in."}}})
}

// WebSocketClient implements the ClientInterfacer
type WebSocketClient struct {
	hub           *server.Hub
	conn          *websocket.Conn
	send          chan *packets.Packet
	id            uint64
	userID        int64
	username      string
	ip            string
	logger        *log.Logger
	state         server.ClientStateHandler
	sharedObjects *server.SharedGameObjects
}
func ServeWs(hub *server.Hub, w http.ResponseWriter, r *http.Request) {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		log.Printf("Error parsing remote address: %v. Denying connection.", err)
		http.Error(w, "Cannot determine remote address", http.StatusBadRequest)
		return
	}
	_, err = hub.NewDbTx().Queries.GetActiveBanForIP(context.Background(), sql.NullString{String: ip, Valid: true})
	if err == nil {
		log.Printf("Denied connection from banned IP: %s", ip)
		http.Error(w, "Access denied.", http.StatusForbidden)
		return
	}

	var upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	client := &WebSocketClient{
		hub:           hub,
		conn:          conn,
		send:          make(chan *packets.Packet, 256),
		logger:        log.New(log.Writer(), fmt.Sprintf("[Client %s] ", ip), log.LstdFlags),
		sharedObjects: hub.SharedGameObjects,
	}
	client.Initialize(objects.NextID(), ip)
	hub.RegisterChan <- client
	go client.writePump()
	go client.readPump()
}
func (c *WebSocketClient) Id() uint64                      { return c.id }
func (c *WebSocketClient) GetUserID() int64                { return c.userID }
func (c *WebSocketClient) SetUserID(id int64)              { c.userID = id }
func (c *WebSocketClient) GetUsername() string             { return c.username }
func (c *WebSocketClient) SetUsername(name string)         { c.username = name }
func (c *WebSocketClient) GetIP() string                   { return c.ip }
func (c *WebSocketClient) WritePump() chan<- *packets.Packet { return c.send }
func (c *WebSocketClient) DbTx() *db.Queries                 { return c.hub.NewDbTx().Queries }
func (c *WebSocketClient) SharedGameObjects() *server.SharedGameObjects {
	return c.sharedObjects
}
func (c *WebSocketClient) Logger() *log.Logger { return c.logger }
func (c *WebSocketClient) GetHub() *server.Hub   { return c.hub }
func (c *WebSocketClient) Initialize(id uint64, ip string) {
	c.id = id
	c.ip = ip
	c.logger.SetPrefix(fmt.Sprintf("[Client %d %s] ", id, ip))
	c.SetState(&Unauthenticated{})
}
func (c *WebSocketClient) SetState(newState server.ClientStateHandler) {
	if c.state != nil {
		c.state.OnExit()
	}
	c.state = newState
	c.state.SetClient(c)
	c.state.OnEnter()
}
func (c *WebSocketClient) ProcessMessage(senderId uint64, packet *packets.Packet) {
	if c.state != nil {
		c.state.HandleMessage(senderId, packet)
	}
}
func (c *WebSocketClient) SocketSend(packet *packets.Packet) {
	select {
	case c.send <- packet:
	default:
		c.Close("send buffer full")
	}
}
func (c *WebSocketClient) Broadcast(packet *packets.Packet) {
	c.hub.BroadcastChan <- packet
}
func (c *WebSocketClient) Close(reason string) {
	c.logger.Printf("Closing connection: %s", reason)
	c.hub.UnregisterChan <- c
	close(c.send)
	c.conn.Close()
}
func (c *WebSocketClient) readPump() {
	defer func() {
		c.Close("read pump ended")
	}()
	c.conn.SetReadLimit(4096)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error { c.conn.SetReadDeadline(time.Now().Add(60 * time.Second)); return nil })
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.logger.Printf("read error: %v", err)
			}
			break
		}
		var packet packets.Packet
		if err := proto.Unmarshal(message, &packet); err != nil {
			c.logger.Printf("failed to unmarshal packet: %v", err)
			continue
		}
		c.ProcessMessage(c.id, &packet)
	}
}
func (c *WebSocketClient) writePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case packet, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			data, err := proto.Marshal(packet)
			if err != nil {
				c.logger.Printf("failed to marshal packet: %v", err)
				continue
			}
			if err := c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}