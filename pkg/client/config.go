package client

import "github.com/pecigonzalo/kafka-canary/pkg/canary"

type Config struct {
}

type Client struct {
}

func NewClient(canary canary.Config, client Config) (*Client, error) {
	return &Client{}, nil
}
