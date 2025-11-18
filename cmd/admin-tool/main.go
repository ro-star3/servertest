package main

import (
	"flag"
	"log"
	"net/http"
	"path/filepath"

	"chaogarden-server/internal/server"
	"chaogarden-server/internal/server/clients"
)

func main() {
	// Use flags to make the server configurable from the command line
	var dataDirPath = flag.String("data-dir", ".", "The directory to find the db.sqlite file.")
	var clientDirPath = flag.String("client-dir", "./html5", "The directory containing the Godot HTML5 client files.")
	var port = flag.String("port", "7777", "The port for the server to listen on.")
	flag.Parse()

	// Ensure paths are clean and absolute for reliable logging
	absClientPath, err := filepath.Abs(*clientDirPath)
	if err != nil {
		log.Fatalf("Could not determine absolute path for client directory: %v", err)
	}

	hub := server.NewHub(*dataDirPath)
	go hub.Run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		clients.ServeWs(hub, w, r)
	})

	// Serve the static files for the Godot client
	fs := http.FileServer(http.Dir(*clientDirPath))
	http.Handle("/", fs)

	log.Printf("Starting server on port :%s", *port)
	log.Println("Serving client files from:", absClientPath)

	if err := http.ListenAndServe(":"+*port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}