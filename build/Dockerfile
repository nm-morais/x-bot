FROM golang:1.16 AS builder

WORKDIR $GOPATH/src/github.com/nm-morais/XBot
COPY . .
RUN go mod download
RUN GOOS=linux GOARCH=amd64 go build -o /go/bin/XBot *.go

FROM debian:stable-slim as xbot
COPY --from=builder /go/bin/XBot /go/bin/XBot
COPY config /config

RUN chmod +x /go/bin/XBot