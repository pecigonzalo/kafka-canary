package util

import (
	"errors"
	"io"
	"os"
	"syscall"
	"time"
)

// NowInMilliseconds returns the current time in milliseconds
func NowInMilliseconds() int64 {
	return time.Now().UnixNano() / 1000000
}

// IsDisconnection returns true if the err provided represents a TCP disconnection
func IsDisconnection(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ETIMEDOUT) || errors.Is(err, os.ErrDeadlineExceeded)
}
