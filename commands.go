// Package beanstalkd additional commands implementation.
package beanstalkd

import (
	"fmt"
	"strconv"
	"strings"
)

// Peek retrieves a job by its ID without reserving it.
func (c *Client) Peek(jobID uint64) (*Job, error) {
	command := fmt.Sprintf("peek %d", jobID)
	return c.peekCommand(command)
}

// PeekReady retrieves the next ready job without reserving it.
func (c *Client) PeekReady() (*Job, error) {
	command := "peek-ready"
	return c.peekCommand(command)
}

// PeekDelayed retrieves the next delayed job without reserving it.
func (c *Client) PeekDelayed() (*Job, error) {
	command := "peek-delayed"
	return c.peekCommand(command)
}

// PeekBuried retrieves the next buried job without reserving it.
func (c *Client) PeekBuried() (*Job, error) {
	command := "peek-buried"
	return c.peekCommand(command)
}

// peekCommand is a helper method for peek commands.
func (c *Client) peekCommand(command string) (*Job, error) {
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
	case "FOUND":
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
	case "NOT_FOUND":
		return nil, &Error{Type: "NOT_FOUND", Message: "job not found"}
	default:
		return nil, parseError(response)
	}
}

// Kick moves jobs from the buried state to the ready state.
//
// Parameters:
//   - bound: maximum number of jobs to kick
//
// Returns the number of jobs actually kicked.
func (c *Client) Kick(bound uint32) (uint32, error) {
	command := fmt.Sprintf("kick %d", bound)
	response, err := c.sendCommand(command)
	if err != nil {
		return 0, err
	}

	// Parse response
	parts := strings.Fields(response)
	if len(parts) < 2 {
		return 0, parseError(response)
	}

	if parts[0] != "KICKED" {
		return 0, parseError(response)
	}

	count, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid kick count in response: %w", err)
	}

	return uint32(count), nil
}

// KickJob moves a specific job from the buried state to the ready state.
func (c *Client) KickJob(jobID uint64) error {
	command := fmt.Sprintf("kick-job %d", jobID)
	response, err := c.sendCommand(command)
	if err != nil {
		return err
	}

	switch response {
	case "KICKED":
		return nil
	case "NOT_FOUND":
		return &Error{Type: "NOT_FOUND", Message: "job not found"}
	default:
		return parseError(response)
	}
}

// StatsJob returns statistical information about a specific job.
func (c *Client) StatsJob(jobID uint64) (*JobStats, error) {
	command := fmt.Sprintf("stats-job %d", jobID)
	response, err := c.sendCommand(command)
	if err != nil {
		return nil, err
	}

	// Parse response
	parts := strings.Fields(response)
	if len(parts) < 2 {
		return nil, parseError(response)
	}

	switch parts[0] {
	case "OK":
		// Read YAML data
		dataSize, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid data size in response: %w", err)
		}

		data := make([]byte, dataSize)
		if _, err := c.reader.Read(data); err != nil {
			return nil, fmt.Errorf("failed to read stats data: %w", err)
		}

		// Read trailing \r\n
		if _, err := c.reader.ReadString('\n'); err != nil {
			return nil, fmt.Errorf("failed to read data terminator: %w", err)
		}

		// Parse YAML data
		stats, err := parseJobStats(string(data))
		if err != nil {
			return nil, fmt.Errorf("failed to parse job stats: %w", err)
		}

		return stats, nil
	case "NOT_FOUND":
		return nil, &Error{Type: "NOT_FOUND", Message: "job not found"}
	default:
		return nil, parseError(response)
	}
}

// StatsTube returns statistical information about a specific tube.
func (c *Client) StatsTube(tube string) (*TubeStats, error) {
	// Validate tube name
	if err := validateTubeName(tube); err != nil {
		return nil, fmt.Errorf("invalid tube name: %w", err)
	}

	command := fmt.Sprintf("stats-tube %s", tube)
	response, err := c.sendCommand(command)
	if err != nil {
		return nil, err
	}

	// Parse response
	parts := strings.Fields(response)
	if len(parts) < 2 {
		return nil, parseError(response)
	}

	switch parts[0] {
	case "OK":
		// Read YAML data
		dataSize, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid data size in response: %w", err)
		}

		data := make([]byte, dataSize)
		if _, err := c.reader.Read(data); err != nil {
			return nil, fmt.Errorf("failed to read stats data: %w", err)
		}

		// Read trailing \r\n
		if _, err := c.reader.ReadString('\n'); err != nil {
			return nil, fmt.Errorf("failed to read data terminator: %w", err)
		}

		// Parse YAML data
		stats, err := parseTubeStats(string(data))
		if err != nil {
			return nil, fmt.Errorf("failed to parse tube stats: %w", err)
		}

		return stats, nil
	case "NOT_FOUND":
		return nil, &Error{Type: "NOT_FOUND", Message: "tube not found"}
	default:
		return nil, parseError(response)
	}
}

// Stats returns statistical information about the entire server.
func (c *Client) Stats() (*ServerStats, error) {
	command := "stats"
	response, err := c.sendCommand(command)
	if err != nil {
		return nil, err
	}

	// Parse response
	parts := strings.Fields(response)
	if len(parts) < 2 {
		return nil, parseError(response)
	}

	if parts[0] != "OK" {
		return nil, parseError(response)
	}

	// Read YAML data
	dataSize, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid data size in response: %w", err)
	}

	data := make([]byte, dataSize)
	if _, err := c.reader.Read(data); err != nil {
		return nil, fmt.Errorf("failed to read stats data: %w", err)
	}

	// Read trailing \r\n
	if _, err := c.reader.ReadString('\n'); err != nil {
		return nil, fmt.Errorf("failed to read data terminator: %w", err)
	}

	// Parse YAML data
	stats, err := parseServerStats(string(data))
	if err != nil {
		return nil, fmt.Errorf("failed to parse server stats: %w", err)
	}

	return stats, nil
}

// ListTubes returns a list of all existing tubes.
func (c *Client) ListTubes() ([]string, error) {
	command := "list-tubes"
	response, err := c.sendCommand(command)
	if err != nil {
		return nil, err
	}

	// Parse response
	parts := strings.Fields(response)
	if len(parts) < 2 {
		return nil, parseError(response)
	}

	if parts[0] != "OK" {
		return nil, parseError(response)
	}

	// Read YAML data
	dataSize, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid data size in response: %w", err)
	}

	data := make([]byte, dataSize)
	if _, err := c.reader.Read(data); err != nil {
		return nil, fmt.Errorf("failed to read tubes data: %w", err)
	}

	// Read trailing \r\n
	if _, err := c.reader.ReadString('\n'); err != nil {
		return nil, fmt.Errorf("failed to read data terminator: %w", err)
	}

	// Parse YAML data
	tubes, err := parseTubeList(string(data))
	if err != nil {
		return nil, fmt.Errorf("failed to parse tubes list: %w", err)
	}

	return tubes, nil
}

// ListTubeUsed returns the tube currently being used by the client.
func (c *Client) ListTubeUsed() (string, error) {
	command := "list-tube-used"
	response, err := c.sendCommand(command)
	if err != nil {
		return "", err
	}

	// Parse response
	parts := strings.Fields(response)
	if len(parts) < 2 {
		return "", parseError(response)
	}

	if parts[0] != "USING" {
		return "", parseError(response)
	}

	return parts[1], nil
}

// ListTubesWatched returns a list of tubes currently being watched by the client.
func (c *Client) ListTubesWatched() ([]string, error) {
	command := "list-tubes-watched"
	response, err := c.sendCommand(command)
	if err != nil {
		return nil, err
	}

	// Parse response
	parts := strings.Fields(response)
	if len(parts) < 2 {
		return nil, parseError(response)
	}

	if parts[0] != "OK" {
		return nil, parseError(response)
	}

	// Read YAML data
	dataSize, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid data size in response: %w", err)
	}

	data := make([]byte, dataSize)
	if _, err := c.reader.Read(data); err != nil {
		return nil, fmt.Errorf("failed to read tubes data: %w", err)
	}

	// Read trailing \r\n
	if _, err := c.reader.ReadString('\n'); err != nil {
		return nil, fmt.Errorf("failed to read data terminator: %w", err)
	}

	// Parse YAML data
	tubes, err := parseTubeList(string(data))
	if err != nil {
		return nil, fmt.Errorf("failed to parse tubes list: %w", err)
	}

	return tubes, nil
}

// PauseTube delays any new job being reserved for a given time.
func (c *Client) PauseTube(tube string, delay uint32) error {
	// Validate tube name
	if err := validateTubeName(tube); err != nil {
		return fmt.Errorf("invalid tube name: %w", err)
	}

	command := fmt.Sprintf("pause-tube %s %d", tube, delay)
	response, err := c.sendCommand(command)
	if err != nil {
		return err
	}

	switch response {
	case "PAUSED":
		return nil
	case "NOT_FOUND":
		return &Error{Type: "NOT_FOUND", Message: "tube not found"}
	default:
		return parseError(response)
	}
}

// Quit closes the connection to the server.
func (c *Client) Quit() error {
	command := "quit"
	_, err := c.sendCommand(command)
	if err != nil {
		return err
	}
	return c.Close()
}
