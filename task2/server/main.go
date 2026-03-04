package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"myproject/api"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	pool *pgxpool.Pool
}

func main() {
	databse_url := os.Getenv("DATABASE_URL")
	if databse_url == "" {
		databse_url = "postgres://postgres:postgres@localhost:5432/marketplace?sslmode=disable"
	}

	pool, err := pgxpool.New(context.Background(), databse_url)
	if err != nil {
		log.Fatalf("%v", err)
	}

	defer pool.Close()

	if err := pool.Ping(context.Background()); err != nil {
		log.Fatalf("%v", err)
	}

	envGetter = os.Getenv

	srv := &Server{pool: pool}
	r := chi.NewRouter()
	r.Use(logging)

	r.Post("/auth/register", srv.handleRegister)
	r.Post("/auth/login", srv.handleLogin)
	r.Post("/auth/refresh", srv.handleRefresh)

	r.Group(func(r chi.Router) {
		r.Use(authMiddleware)
		r.Mount("/", api.HandlerFromMux(srv, r))
	})

	addr := ":8080"
	if p := os.Getenv("PORT"); p != "" {
		addr = ":" + p
	}







	log.Fatal(http.ListenAndServe(addr, r))
}
