FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go mod vendor && CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o /jqcli ./cmd/jqcli


FROM alpine:3.21

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /jqcli /jqcli

RUN mkdir -p csv logs

VOLUME ["/app/csv", "/app/logs"]

ENTRYPOINT ["/jqcli"]
