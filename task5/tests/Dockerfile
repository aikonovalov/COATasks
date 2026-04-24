FROM golang:1.22-alpine

WORKDIR /tests

ENV GONOSUMDB="*"
ENV GOFLAGS="-mod=mod"

COPY go.mod ./
RUN go mod download

COPY . .

ENTRYPOINT ["go", "test", "-v", "-timeout", "60s", "./integration/..."]
