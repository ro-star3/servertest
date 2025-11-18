package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"

	"chaogarden-server/internal/server"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, loading from environment variables...")
	}

	databaseUrl := os.Getenv("DATABASE_URL")
	if databaseUrl == "" {
		log.Fatal("DATABASE_URL environment variable is not set. Example: chaogarden_user:your_password@tcp(127.0.0.1:3306)/plorbgarden_db?charset=utf8mb4&parseTime=True&loc=Local")
	}

	gameServer := server.NewHub(databaseUrl)
	go gameServer.Run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		server.ServeWs(gameServer, w, r)
	})

	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	html5Dir := filepath.Join(dir, "html5")
	log.Printf("Serving client files from: %s", html5Dir)
	http.Handle("/", http.FileServer(http.Dir(html5Dir)))

	log.Println("Starting server on port :7777")
	err = http.ListenAndServe(":7777", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}