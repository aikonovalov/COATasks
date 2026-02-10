FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY service/go.mod service/go.sum* ./
RUN go mod download || true

COPY service/ ./

RUN CGO_ENABLED=0 GOOS=linux go build -o service .

FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/service .

EXPOSE 8080

CMD ["./service"]
