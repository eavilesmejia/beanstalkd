// Package beanstalkd producer tests.
package beanstalkd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUse(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	client, err := Connect(server.Address())
	require.NoError(t, err)
	defer client.Close()

	// Mock server response
	go func() {
		time.Sleep(10 * time.Millisecond)
		server.SendResponse("USING my-tube")
	}()

	err = client.Use("my-tube")
	assert.NoError(t, err)
}

func TestUseInvalidTube(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	client, err := Connect(server.Address())
	require.NoError(t, err)
	defer client.Close()

	// Test invalid tube names
	invalidTubes := []string{"", "-tube", "tube@", "tube with space"}

	for _, tube := range invalidTubes {
		t.Run(tube, func(t *testing.T) {
			err := client.Use(tube)
			assert.Error(t, err)
		})
	}
}

func TestPut(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	client, err := Connect(server.Address())
	require.NoError(t, err)
	defer client.Close()

	// Mock server responses
	go func() {
		time.Sleep(10 * time.Millisecond)
		// First response for USE command
		server.SendResponse("USING my-tube")
		time.Sleep(10 * time.Millisecond)
		// Second response for PUT command
		server.SendResponse("INSERTED 123")
	}()

	jobID, err := client.Put("my-tube", 1024, 0, 60, []byte("hello world"))
	require.NoError(t, err)
	assert.Equal(t, uint64(123), jobID)
}

func TestPutBuried(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	client, err := Connect(server.Address())
	require.NoError(t, err)
	defer client.Close()

	// Mock server responses
	go func() {
		time.Sleep(10 * time.Millisecond)
		// First response for USE command
		server.SendResponse("USING my-tube")
		time.Sleep(10 * time.Millisecond)
		// Second response for PUT command
		server.SendResponse("BURIED 123")
	}()

	jobID, err := client.Put("my-tube", 1024, 0, 60, []byte("hello world"))
	assert.Error(t, err)
	assert.Equal(t, uint64(123), jobID)
	assert.Contains(t, err.Error(), "buried")
}

func TestPutError(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	client, err := Connect(server.Address())
	require.NoError(t, err)
	defer client.Close()

	// Mock server responses
	go func() {
		time.Sleep(10 * time.Millisecond)
		// First response for USE command
		server.SendResponse("USING my-tube")
		time.Sleep(10 * time.Millisecond)
		// Second response for PUT command
		server.SendResponse("JOB_TOO_BIG")
	}()

	jobID, err := client.Put("my-tube", 1024, 0, 60, []byte("hello world"))
	assert.Error(t, err)
	assert.Equal(t, uint64(0), jobID)
	assert.Contains(t, err.Error(), "JOB_TOO_BIG")
}

func TestPutWithDelay(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	client, err := Connect(server.Address())
	require.NoError(t, err)
	defer client.Close()

	// Mock server responses
	go func() {
		time.Sleep(10 * time.Millisecond)
		// First response for USE command
		server.SendResponse("USING my-tube")
		time.Sleep(10 * time.Millisecond)
		// Second response for PUT command
		server.SendResponse("INSERTED 456")
	}()

	jobID, err := client.Put("my-tube", 2048, 30, 120, []byte("delayed job"))
	require.NoError(t, err)
	assert.Equal(t, uint64(456), jobID)
}

func TestPutZeroTTR(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	client, err := Connect(server.Address())
	require.NoError(t, err)
	defer client.Close()

	// Mock server responses
	go func() {
		time.Sleep(10 * time.Millisecond)
		// First response for USE command
		server.SendResponse("USING my-tube")
		time.Sleep(10 * time.Millisecond)
		// Second response for PUT command
		server.SendResponse("INSERTED 789")
	}()

	// TTR of 0 should be handled by the server (increased to 1)
	jobID, err := client.Put("my-tube", 1024, 0, 0, []byte("zero ttr job"))
	require.NoError(t, err)
	assert.Equal(t, uint64(789), jobID)
}
