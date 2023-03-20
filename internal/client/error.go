package client

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/segmentio/kafka-go"
)

func KafkaErrorsToErr(kerrs map[string]error) error {
	var errs []error
	for _, err := range kerrs {
		if err != nil {
			errs = append(errs, err)
		}
	}
	err := errors.Join(errs...)
	if len(errs) > 0 {
		return fmt.Errorf("kafka errors: %w", err)
	}
	return nil
}

func IncrementalAlterConfigsResponseResourcesError(resources []kafka.IncrementalAlterConfigsResponseResource) error {
	var errs []error
	for _, resource := range resources {
		if resource.Error != nil {
			errs = append(errs, fmt.Errorf("resource(%s) error: %w", resource.ResourceName, resource.Error))
		}
	}
	err := errors.Join(errs...)
	if len(errs) > 0 {
		return fmt.Errorf("alter config errors: %w", err)
	}
	return nil
}

func AlterPartitionReassignmentsRequestAssignmentError(results []kafka.AlterPartitionReassignmentsResponsePartitionResult) error {
	var errs []error
	for _, result := range results {
		if result.Error != nil {
			errs = append(errs, fmt.Errorf("partition(%d) had error: %w", result.PartitionID, result.Error))
		}
	}
	err := errors.Join(errs...)
	if len(errs) > 0 {
		return fmt.Errorf("alter partition errors: %w", err)
	}
	return nil
}

func IsTransientNetworkError(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE)
}

// IsDisconnection returns true if the err provided represents a TCP disconnection
func IsDisconnection(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ETIMEDOUT) || errors.Is(err, os.ErrDeadlineExceeded)
}
