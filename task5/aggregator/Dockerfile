FROM golang:1.22-alpine AS builder

WORKDIR /build

COPY go.mod ./
ENV GONOSUMDB="*"
ENV GOFLAGS="-mod=mod"
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /aggregator ./cmd/

FROM alpine:3.21

COPY --from=builder /aggregator /aggregator
ENTRYPOINT ["/aggregator"]
