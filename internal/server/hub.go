package server

import (
	"bytes"
	"chaogarden-server/internal/server/db"
	"chaogarden-server/internal/server/world"
	"database/sql"
	"encoding/binary"
	"log"
	"math/rand"
	"os"
	"strconv"

	"chaogarden-server/pkg/packets"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	hub      *Hub
	conn     *websocket.Conn
	send     chan []byte
	id       uint64
	logger   *log.Logger
	state    ClientState
	userID   int64
	username string
}

type MessagePayload struct {
	Client  *Client
	Message []byte
}

func NewClient(hub *Hub, conn *websocket.Conn) *Client {
	clientID := rand.Uint64()
	clientLogger := log.New(os.Stdout, "[Client "+strconv.FormatUint(clientID, 10)+" "+conn.RemoteAddr().String()+"] ", log.Ldate|log.Ltime)

	client := &Client{
		hub:    hub,
		conn:   conn,
		send:   make(chan []byte, 256),
		id:     clientID,
		logger: clientLogger,
	}

	unauthenticatedState := &UnauthenticatedState{hub: hub}
	client.SetState(unauthenticatedState)
	return client
}

type Hub struct {
	Clients          *SafeClientMap
	Register         chan *Client
	Unregister       chan *Client
	HandleRawMessage chan MessagePayload
	Broadcast        chan []byte
	World            *world.World
	logger           *log.Logger
	db               *sql.DB
	queries          *db.Queries
}

func NewHub(databaseUrl string) *Hub {
	dbConn, err := sql.Open("mysql", databaseUrl)
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}
	if err := dbConn.Ping(); err != nil {
		log.Fatalf("Error pinging database: %v", err)
	}

	queries := db.New(dbConn)
	worldManager := world.NewWorld(queries)

	return &Hub{
		Register:         make(chan *Client),
		Unregister:       make(chan *Client),
		HandleRawMessage: make(chan MessagePayload),
		Broadcast:        make(chan []byte),
		Clients:          NewSafeClientMap(),
		World:            worldManager,
		logger:           log.New(os.Stdout, "[HUB] ", log.Ldate|log.Ltime),
		db:               dbConn,
		queries:          queries,
	}
}

func (h *Hub) Run() {
	h.logger.Println("Run() started. Entering main select loop.")
	for {
		select {
		case client := <-h.Register:
			h.Clients.Store(client.id, client)
			client.logger.Printf("Client %d connected", client.id)
		case client := <-h.Unregister:
			if c, ok := h.Clients.Load(client.id); ok {
				h.World.RemovePlayer(c.id)
				close(c.send)
				h.Clients.Delete(c.id)
				c.logger.Printf("Client %d disconnected", c.id)
			}
		case payload := <-h.HandleRawMessage:
			h.parseAndRouteMessage(payload.Client, payload.Message)
		case message := <-h.Broadcast:
			h.Clients.Range(func(key uint64, client *Client) bool {
				select {
				case client.send <- message:
				default:
					close(client.send)
					h.Clients.Delete(client.id)
				}
				return true
			})
		}
	}
}

func (h *Hub) parseAndRouteMessage(client *Client, message []byte) {
	if len(message) < 1 {
		client.logger.Println("Received empty packet.")
		return
	}

	packetType := message[0]
	packetData := message[1:]
	packetReader := bytes.NewReader(packetData)

	switch packetType {
	case 4: // LoginRequest
		var userLen uint32
		binary.Read(packetReader, binary.LittleEndian, &userLen)
		userBytes := make([]byte, userLen)
		packetReader.Read(userBytes)
		username := string(bytes.TrimRight(userBytes, "\x00"))

		var passLen uint32
		binary.Read(packetReader, binary.LittleEndian, &passLen)
		passBytes := make([]byte, passLen)
		packetReader.Read(passBytes)
		password := string(bytes.TrimRight(passBytes, "\x00"))

		req := &packets.LoginRequest{Username: username, Password: password}
		client.state.HandleLoginRequest(client, req)

	case 50: // EnterWorldRequest
		if s, ok := client.state.(*ConnectedState); ok {
			s.HandleEnterWorld(client)
		} else {
			client.logger.Println("Received EnterWorldRequest from non-connected client.")
		}

	case 52: // PlayerMoveRequest
		var targetX, targetY, direction int32
		binary.Read(packetReader, binary.LittleEndian, &targetX)
		binary.Read(packetReader, binary.LittleEndian, &targetY)
		binary.Read(packetReader, binary.LittleEndian, &direction)

		req := &packets.PlayerMoveRequest{
			TargetX:   targetX,
			TargetY:   targetY,
			Direction: direction,
		}
		client.state.HandlePlayerMove(client, req)

	default:
		client.logger.Printf("Received unhandled raw packet type: %d", packetType)
	}
}

func (h *Hub) SendMessage(client *Client, packetType byte, msg proto.Message) {
	data, err := proto.Marshal(msg)
	if err != nil {
		client.logger.Printf("Failed to marshal message: %v", err)
		return
	}
	fullPacket := append([]byte{packetType}, data...)
	client.send <- fullPacket
}

func (c *Client) Id() uint64               { return c.id }
func (c *Client) Logger() *log.Logger      { return c.logger }
func (c *Client) GetState() ClientState    { return c.state }
func (c *Client) SetState(state ClientState) { c.state = state }
func (c *Client) GetUserID() int64         { return c.userID }
func (c *Client) SetUserID(id int64)         { c.userID = id }
func (c *Client) GetUsername() string      { return c.username }
func (c *Client) SetUsername(name string)     { c.username = name }
func (c *Client) GetHub() *Hub             { return c.hub }
func (c *Client) DbTx() (*db.Queries, *sql.Tx, error) {
	tx, err := c.hub.db.Begin()
	if err != nil {
		return nil, nil, err
	}
	return c.hub.queries.WithTx(tx), tx, nil
}
func (c *Client) Close(reason string) { c.logger.Println(reason); c.conn.Close() }