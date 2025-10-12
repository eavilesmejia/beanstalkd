// Package beanstalkd producer commands implementation.
package beanstalkd

import (
	"fmt"
	"strings"
)

// Put inserts a job into the currently used tube.
//
// Parameters:
//   - tube: the tube name (will be set using Use() if not already set)
//   - priority: job priority (0 = most urgent, 4294967295 = least urgent)
//   - delay: seconds to wait before putting the job in the ready queue
//   - ttr: time to run in seconds (minimum 1)
//   - body: the job body
//
// Returns the job ID on success.
func (c *Client) Put(tube string, priority, delay, ttr uint32, body []byte) (uint64, error) {
	// Validate job body
	if err := ValidateJobBody(body); err != nil {
		return 0, fmt.Errorf("invalid job body: %w", err)
	}

	// Use the specified tube
	if err := c.Use(tube); err != nil {
		return 0, fmt.Errorf("failed to use tube %s: %w", tube, err)
	}

	// Validate parameters
	if ttr == 0 {
		ttr = 1 // Server will silently increase 0 to 1
	}

	// Build command
	command := fmt.Sprintf("put %d %d %d %d", priority, delay, ttr, len(body))

	// Send command with body
	response, err := c.sendCommandWithBody(command, body)
	if err != nil {
		return 0, err
	}

	// Parse response
	parts := strings.Fields(response)
	if len(parts) < 2 {
		return 0, parseError(response)
	}

	switch parts[0] {
	case "INSERTED":
		jobID, err := ParseJobID(parts[1])
		if err != nil {
			return 0, fmt.Errorf("invalid job ID in response: %w", err)
		}
		return jobID, nil
	case "BURIED":
		jobID, err := ParseJobID(parts[1])
		if err != nil {
			return 0, fmt.Errorf("invalid job ID in response: %w", err)
		}
		return jobID, fmt.Errorf("job buried due to memory constraints")
	default:
		return 0, parseError(response)
	}
}

// Use sets the tube for subsequent Put commands.
//
// If no Use command has been issued, jobs will be put into the tube named "default".
// The tube will be created if it doesn't exist.
func (c *Client) Use(tube string) error {
	// Validate tube name
	if err := validateTubeName(tube); err != nil {
		return fmt.Errorf("invalid tube name: %w", err)
	}

	command := fmt.Sprintf("use %s", tube)
	response, err := c.sendCommand(command)
	if err != nil {
		return err
	}

	// Parse response
	parts := strings.Fields(response)
	if len(parts) < 2 {
		return parseError(response)
	}

	if parts[0] != "USING" {
		return parseError(response)
	}

	// Verify we're using the correct tube
	if parts[1] != tube {
		return fmt.Errorf("expected to use tube %s, but server returned %s", tube, parts[1])
	}

	return nil
}

// validateTubeName validates a tube name according to the Beanstalk protocol.
func validateTubeName(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("tube name cannot be empty")
	}
	if len(name) > 200 {
		return fmt.Errorf("tube name cannot exceed 200 bytes")
	}
	if name[0] == '-' {
		return fmt.Errorf("tube name cannot begin with hyphen")
	}

	// Validate each character in the tube name
	for _, char := range name {
		if !isValidTubeNameChar(char) {
			return fmt.Errorf("tube name contains invalid character: %c", char)
		}
	}

	return nil
}

// isValidTubeNameChar checks if a character is valid in a tube name.
func isValidTubeNameChar(char rune) bool {
	return (char >= 'A' && char <= 'Z') ||
		(char >= 'a' && char <= 'z') ||
		(char >= '0' && char <= '9') ||
		char == '-' || char == '+' || char == '/' ||
		char == ';' || char == '.' || char == '$' ||
		char == '_' || char == '(' || char == ')'
}
