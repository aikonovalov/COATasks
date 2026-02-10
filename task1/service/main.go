package main

import (
	"encoding/json"
	"log"
	"net/http"
)

type HealthResponse struct {
	StatusCode int    `json:"status_code"`
	Status     string `json:"status"`
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	json.NewEncoder(w).Encode(HealthResponse{
		StatusCode: 200,
		Status:     "OK",
	})
}

func main() {
	http.HandleFunc("/health", healthHandler)
	
	
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
