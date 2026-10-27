package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" { addr = ":8081" }
	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Header().Set("content-type", "application/json"); _ = json.NewEncoder(w).Encode(map[string]string{"status":"ok"}) })
	s := &http.Server{Addr:addr, ReadHeaderTimeout:5*time.Second}
	log.Printf("crawler health endpoint listening on %s", addr)
	log.Fatal(s.ListenAndServe())
}
