// Package beanstalkd worker commands implementation.
package beanstalkd

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Reserve reserves a job from any tube in the watch list.
//
// This command will block until a job becomes available.
// The job will be reserved for the client and must be deleted, released, or buried.
func (c *Client) Reserve() (*Job, error) {
	return c.ReserveWithTimeout(0)
}

// ReserveWithTimeout reserves a job from any tube in the watch list with a timeout.
//
// Parameters:
//   - timeout: timeout in seconds (0 = no timeout, blocks indefinitely)
//
// Returns the reserved job or an error if timeout is reached.
func (c *Client) ReserveWithTimeout(timeout uint32) (*Job, error) {
	var command string
	if timeout == 0 {
		command = "reserve"
	} else {
		command = fmt.Sprintf("reserve-with-timeout %d", timeout)
	}

	response, err := c.sendCommand(command)
	if err != nil {
		return nil, err
	}

	// Parse response
	parts := strings.Fields(response)
	if len(parts) < 3 {
		return nil, parseError(response)
	}

	switch parts[0] {
	case "RESERVED":
		jobID, err := ParseJobID(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid job ID in response: %w", err)
		}

		bodySize, err := strconv.Atoi(parts[2])
		if err != nil {
			return nil, fmt.Errorf("invalid body size in response: %w", err)
		}

		// Read job body
		body := make([]byte, bodySize)
		if _, err := c.reader.Read(body); err != nil {
			return nil, fmt.Errorf("failed to read job body: %w", err)
		}

		// Read trailing \r\n
		if _, err := c.reader.ReadString('\n'); err != nil {
			return nil, fmt.Errorf("failed to read body terminator: %w", err)
		}

		return &Job{
			ID:   jobID,
			Body: body,
		}, nil
	case "TIMED_OUT":
		return nil, &Error{Type: "TIMED_OUT", Message: "reserve command timed out"}
	case "DEADLINE_SOON":
		return nil, &Error{Type: "DEADLINE_SOON", Message: "job deadline is soon"}
	default:
		return nil, parseError(response)
	}
}

// Delete removes a job from the server entirely.
//
// This command is normally used by workers after they have successfully completed a job.
func (c *Client) Delete(jobID uint64) error {
	command := fmt.Sprintf("delete %d", jobID)
	response, err := c.sendCommand(command)
	if err != nil {
		return err
	}

	switch response {
	case "DELETED":
		return nil
	case "NOT_FOUND":
		return &Error{Type: "NOT_FOUND", Message: "job not found"}
	default:
		return parseError(response)
	}
}

// Release puts a reserved job back into the ready queue.
//
// Parameters:
//   - jobID: the job to release
//   - priority: new priority for the job
//   - delay: seconds to wait before putting the job in the ready queue
func (c *Client) Release(jobID uint64, priority, delay uint32) error {
	command := fmt.Sprintf("release %d %d %d", jobID, priority, delay)
	response, err := c.sendCommand(command)
	if err != nil {
		return err
	}

	switch response {
	case "RELEASED":
		return nil
	case "BURIED":
		return &Error{Type: "BURIED", Message: "job was buried due to memory constraints"}
	case "NOT_FOUND":
		return &Error{Type: "NOT_FOUND", Message: "job not found"}
	default:
		return parseError(response)
	}
}

// Bury puts a job into the "buried" state.
//
// Buried jobs are put into a FIFO linked list and will not be touched by the server
// again until a client kicks them with the "kick" command.
func (c *Client) Bury(jobID uint64, priority uint32) error {
	command := fmt.Sprintf("bury %d %d", jobID, priority)
	response, err := c.sendCommand(command)
	if err != nil {
		return err
	}

	switch response {
	case "BURIED":
		return nil
	case "NOT_FOUND":
		return &Error{Type: "NOT_FOUND", Message: "job not found"}
	default:
		return parseError(response)
	}
}

// Touch allows a worker to request more time to work on a job.
//
// This is useful for jobs that potentially take a long time, but you still want
// the benefits of a TTR. A worker may periodically tell the server that it's
// still alive and processing the job (perhaps it's on a long polling call,
// downloading a large file, or doing heavy disk I/O).
func (c *Client) Touch(jobID uint64) error {
	command := fmt.Sprintf("touch %d", jobID)
	response, err := c.sendCommand(command)
	if err != nil {
		return err
	}

	switch response {
	case "TOUCHED":
		return nil
	case "NOT_FOUND":
		return &Error{Type: "NOT_FOUND", Message: "job not found"}
	default:
		return parseError(response)
	}
}

// Watch adds a tube to the watch list for the current connection.
//
// A reserve command will take a job from any of the tubes in the watch list.
// For each new connection, the watch list initially contains just the tube "default".
func (c *Client) Watch(tube string) (uint32, error) {
	// Validate tube name
	if err := validateTubeName(tube); err != nil {
		return 0, fmt.Errorf("invalid tube name: %w", err)
	}

	command := fmt.Sprintf("watch %s", tube)
	response, err := c.sendCommand(command)
	if err != nil {
		return 0, err
	}

	// Parse response
	parts := strings.Fields(response)
	if len(parts) < 2 {
		return 0, parseError(response)
	}

	if parts[0] != "WATCHING" {
		return 0, parseError(response)
	}

	count, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid watch count in response: %w", err)
	}

	return uint32(count), nil
}

// Ignore removes a tube from the watch list for the current connection.
//
// Returns the number of tubes remaining in the watch list.
func (c *Client) Ignore(tube string) (uint32, error) {
	// Validate tube name
	if err := validateTubeName(tube); err != nil {
		return 0, fmt.Errorf("invalid tube name: %w", err)
	}

	command := fmt.Sprintf("ignore %s", tube)
	response, err := c.sendCommand(command)
	if err != nil {
		return 0, err
	}

	// Parse response
	parts := strings.Fields(response)
	if len(parts) < 2 {
		return 0, parseError(response)
	}

	if parts[0] != "WATCHING" {
		return 0, parseError(response)
	}

	count, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid watch count in response: %w", err)
	}

	return uint32(count), nil
}

// ReserveJob is a convenience method that reserves a job and returns it with context support.
func (c *Client) ReserveJob(ctx context.Context) (*Job, error) {
	// For now, we'll use a simple approach with context cancellation
	// In a production implementation, you might want to use a more sophisticated approach
	done := make(chan error, 1)
	var job *Job
	var err error

	go func() {
		job, err = c.Reserve()
		done <- err
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-done:
		return job, err
	}
}

// ReserveJobWithTimeout is a convenience method that reserves a job with a timeout.
func (c *Client) ReserveJobWithTimeout(timeout time.Duration) (*Job, error) {
	timeoutSeconds := uint32(timeout.Seconds())
	if timeoutSeconds == 0 && timeout > 0 {
		timeoutSeconds = 1 // Minimum timeout is 1 second
	}
	return c.ReserveWithTimeout(timeoutSeconds)
}
