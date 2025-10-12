// Package beanstalkd tests.
package beanstalkd

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockServer represents a mock Beanstalk server for testing.
type mockServer struct {
	ln     net.Listener
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

// newMockServer creates a new mock server.
func newMockServer() (*mockServer, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	server := &mockServer{ln: ln}

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		server.conn = conn
		server.reader = bufio.NewReader(conn)
		server.writer = bufio.NewWriter(conn)
	}()

	return server, nil
}

// Close closes the mock server.
func (s *mockServer) Close() error {
	if s.conn != nil {
		s.conn.Close()
	}
	return s.ln.Close()
}

// Address returns the server address.
func (s *mockServer) Address() string {
	return s.ln.Addr().String()
}

// SendResponse sends a response to the client.
func (s *mockServer) SendResponse(response string) error {
	if s.writer == nil {
		return fmt.Errorf("no connection")
	}
	_, err := s.writer.WriteString(response + "\r\n")
	if err != nil {
		return err
	}
	return s.writer.Flush()
}

// SendJobResponse sends a job response with body to the client.
func (s *mockServer) SendJobResponse(jobID uint64, bodySize int, body string) error {
	if s.writer == nil {
		return fmt.Errorf("no connection")
	}

	// Send FOUND response
	response := fmt.Sprintf("FOUND %d %d", jobID, bodySize)
	_, err := s.writer.WriteString(response + "\r\n")
	if err != nil {
		return err
	}

	// Send body
	_, err = s.writer.WriteString(body)
	if err != nil {
		return err
	}

	// Send CRLF terminator
	_, err = s.writer.WriteString("\r\n")
	if err != nil {
		return err
	}

	return s.writer.Flush()
}

// SendReservedResponse sends a RESERVED response with body to the client.
func (s *mockServer) SendReservedResponse(jobID uint64, bodySize int, body string) error {
	if s.writer == nil {
		return fmt.Errorf("no connection")
	}

	// Send RESERVED response
	response := fmt.Sprintf("RESERVED %d %d", jobID, bodySize)
	_, err := s.writer.WriteString(response + "\r\n")
	if err != nil {
		return err
	}

	// Send body
	_, err = s.writer.WriteString(body)
	if err != nil {
		return err
	}

	// Send CRLF terminator
	_, err = s.writer.WriteString("\r\n")
	if err != nil {
		return err
	}

	return s.writer.Flush()
}

// ReadCommand reads a command from the client.
func (s *mockServer) ReadCommand() (string, error) {
	if s.reader == nil {
		return "", fmt.Errorf("no connection")
	}
	return s.reader.ReadString('\n')
}

func TestConnect(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	client, err := Connect(server.Address())
	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	assert.NotNil(t, client.conn)
	assert.NotNil(t, client.reader)
	assert.NotNil(t, client.writer)
}

func TestConnectWithTimeout(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	client, err := ConnectWithTimeout(server.Address(), 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, client)
	defer client.Close()

	assert.NotNil(t, client.conn)
	assert.NotNil(t, client.reader)
	assert.NotNil(t, client.writer)
}

func TestConnectTimeout(t *testing.T) {
	// Try to connect to a non-existent server
	client, err := ConnectWithTimeout("127.0.0.1:99999", 100*time.Millisecond)
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestClose(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	client, err := Connect(server.Address())
	require.NoError(t, err)

	err = client.Close()
	assert.NoError(t, err)

	// Second close should not error
	err = client.Close()
	assert.NoError(t, err)
}

func TestErrorTypes(t *testing.T) {
	tests := []struct {
		response string
		expected string
	}{
		{"OUT_OF_MEMORY", "OUT_OF_MEMORY"},
		{"INTERNAL_ERROR", "INTERNAL_ERROR"},
		{"BAD_FORMAT", "BAD_FORMAT"},
		{"UNKNOWN_COMMAND", "UNKNOWN_COMMAND"},
		{"EXPECTED_CRLF", "EXPECTED_CRLF"},
		{"JOB_TOO_BIG", "JOB_TOO_BIG"},
		{"DRAINING", "DRAINING"},
		{"NOT_FOUND", "NOT_FOUND"},
		{"DEADLINE_SOON", "DEADLINE_SOON"},
		{"TIMED_OUT", "TIMED_OUT"},
		{"UNKNOWN_ERROR", "UNKNOWN"},
	}

	for _, test := range tests {
		t.Run(test.response, func(t *testing.T) {
			err := parseError(test.response)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), test.expected)
		})
	}
}

func TestValidateTubeName(t *testing.T) {
	tests := []struct {
		name      string
		valid     bool
		expectErr bool
	}{
		{"default", true, false},
		{"my-tube", true, false},
		{"my_tube", true, false},
		{"my.tube", true, false},
		{"my+tube", true, false},
		{"my/tube", true, false},
		{"my;tube", true, false},
		{"my$tube", true, false},
		{"my(tube)", true, false},
		{"tube123", true, false},
		{"", false, true},
		{"-tube", false, true},
		{"tube@", false, true},
		{"tube#", false, true},
		{"tube ", false, true},
		{"tube\t", false, true},
		{"tube\n", false, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateTubeName(test.name)
			if test.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestIsValidTubeNameChar(t *testing.T) {
	validChars := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-+/.$_;()"
	for _, char := range validChars {
		assert.True(t, isValidTubeNameChar(rune(char)), "char %c should be valid", char)
	}

	invalidChars := "@#!%^&*[]{}|\\:<>?`~ \t\n\r"
	for _, char := range invalidChars {
		assert.False(t, isValidTubeNameChar(rune(char)), "char %c should be invalid", char)
	}
}
