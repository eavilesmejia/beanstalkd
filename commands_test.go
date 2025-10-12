// Package beanstalkd commands tests.
package beanstalkd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeek(t *testing.T) {
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
		server.SendJobResponse(123, 11, "hello world")
	}()

	job, err := client.Peek(123)
	require.NoError(t, err)
	assert.Equal(t, uint64(123), job.ID)
	assert.Equal(t, []byte("hello world"), job.Body)
}

func TestPeekNotFound(t *testing.T) {
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
		server.SendResponse("NOT_FOUND")
	}()

	job, err := client.Peek(999)
	assert.Error(t, err)
	assert.Nil(t, job)
	assert.Contains(t, err.Error(), "NOT_FOUND")
}

func TestPeekReady(t *testing.T) {
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
		server.SendJobResponse(456, 9, "ready job")
	}()

	job, err := client.PeekReady()
	require.NoError(t, err)
	assert.Equal(t, uint64(456), job.ID)
	assert.Equal(t, []byte("ready job"), job.Body)
}

func TestPeekDelayed(t *testing.T) {
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
		server.SendJobResponse(789, 11, "delayed job")
	}()

	job, err := client.PeekDelayed()
	require.NoError(t, err)
	assert.Equal(t, uint64(789), job.ID)
	assert.Equal(t, []byte("delayed job"), job.Body)
}

func TestPeekBuried(t *testing.T) {
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
		server.SendJobResponse(101, 10, "buried job")
	}()

	job, err := client.PeekBuried()
	require.NoError(t, err)
	assert.Equal(t, uint64(101), job.ID)
	assert.Equal(t, []byte("buried job"), job.Body)
}

func TestKick(t *testing.T) {
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
		server.SendResponse("KICKED 5")
	}()

	count, err := client.Kick(10)
	require.NoError(t, err)
	assert.Equal(t, uint32(5), count)
}

func TestKickJob(t *testing.T) {
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
		server.SendResponse("KICKED")
	}()

	err = client.KickJob(123)
	assert.NoError(t, err)
}

func TestKickJobNotFound(t *testing.T) {
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
		server.SendResponse("NOT_FOUND")
	}()

	err = client.KickJob(999)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NOT_FOUND")
}

func TestListTubeUsed(t *testing.T) {
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

	tube, err := client.ListTubeUsed()
	require.NoError(t, err)
	assert.Equal(t, "my-tube", tube)
}

func TestPauseTube(t *testing.T) {
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
		server.SendResponse("PAUSED")
	}()

	err = client.PauseTube("my-tube", 60)
	assert.NoError(t, err)
}

func TestPauseTubeNotFound(t *testing.T) {
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
		server.SendResponse("NOT_FOUND")
	}()

	err = client.PauseTube("nonexistent-tube", 60)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NOT_FOUND")
}

func TestPauseTubeInvalidName(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	client, err := Connect(server.Address())
	require.NoError(t, err)
	defer client.Close()

	// Test invalid tube name
	err = client.PauseTube("-invalid-tube", 60)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid tube name")
}

func TestQuit(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	client, err := Connect(server.Address())
	require.NoError(t, err)

	// Mock server response - send a response before closing
	go func() {
		time.Sleep(10 * time.Millisecond)
		server.SendResponse("OK")
		time.Sleep(10 * time.Millisecond)
		// Close the connection after QUIT
		if server.conn != nil {
			server.conn.Close()
		}
	}()

	err = client.Quit()
	assert.NoError(t, err)
}
