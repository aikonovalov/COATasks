FROM golang:1.22-alpine AS proto-gen

RUN apk add --no-cache protobuf

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2

WORKDIR /build
COPY proto/ proto/

RUN mkdir -p /build/producer

RUN cd proto && protoc \
      -I . \
      -I /usr/include \
      --go_out=/build/producer \
      --go_opt=module=movie/producer \
      event.proto

FROM golang:1.22-alpine AS builder

WORKDIR /build/producer

COPY --from=proto-gen /build/producer/gen ./gen

COPY producer/go.mod ./
ENV GONOSUMDB="*"
ENV GOFLAGS="-mod=mod"
RUN go mod download

COPY producer/ .
RUN CGO_ENABLED=0 GOOS=linux go build -o /producer ./cmd/

FROM alpine:3.21

COPY --from=builder /producer /producer
ENTRYPOINT ["/producer"]
