// Package services defines an interface for canary services and related implementations
package services

import (
	"encoding/json"
	"fmt"
)

// CanaryMessage defines the payload of a canary message
type CanaryMessage struct {
	ProducerID string `json:"producerId"`
	MessageID  int    `json:"messageId"`
	Timestamp  int64  `json:"timestamp"`
}

func NewCanaryMessage(bytes []byte) (CanaryMessage, error) {
	var cm CanaryMessage
	err := json.Unmarshal(bytes, &cm)
	return cm, err
}

func (cm CanaryMessage) JSON() string {
	json, _ := json.Marshal(cm)
	return string(json)
}

func (cm CanaryMessage) String() string {
	return fmt.Sprintf("{ProducerID:%s, MessageID:%d, Timestamp:%d}",
		cm.ProducerID, cm.MessageID, cm.Timestamp)
}
