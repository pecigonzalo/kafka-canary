FROM golang:1.20-alpine as builder

ARG VERSION

RUN mkdir -p /kafka-canary/

WORKDIR /kafka-canary

COPY . .

RUN go mod download

RUN CGO_ENABLED=0 go build \
  -ldflags "-s -w \
  -X main.version=${VERSION}" \
  -a -o bin/kafka-canary cmd/kafka-canary/*

FROM alpine:3.17

ARG VERSION

LABEL maintainer="pecigonzalo"

RUN addgroup -S app \
  && adduser -S -G app app

RUN apk --no-cache add \
  ca-certificates curl netcat-openbsd

WORKDIR /home/app

COPY --from=builder /kafka-canary/bin/kafka-canary .
RUN chown -R app:app ./

USER app

CMD ["./kafka-canary"]
