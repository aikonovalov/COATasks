generate:
	go generate ./...

build: generate
	go build -o server ./server

up:
	docker compose up -d postgres
	docker compose up flyway

down:
	docker compose down

all: generate build up

.PHONY: generate build up down all
