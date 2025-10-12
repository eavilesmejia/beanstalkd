// Package beanstalkd worker tests.
package beanstalkd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReserve(t *testing.T) {
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
		server.SendReservedResponse(123, 11, "hello world")
	}()

	job, err := client.Reserve()
	require.NoError(t, err)
	assert.Equal(t, uint64(123), job.ID)
	assert.Equal(t, []byte("hello world"), job.Body)
}

func TestReserveWithTimeout(t *testing.T) {
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
		server.SendReservedResponse(456, 12, "test message")
	}()

	job, err := client.ReserveWithTimeout(5)
	require.NoError(t, err)
	assert.Equal(t, uint64(456), job.ID)
	assert.Equal(t, []byte("test message"), job.Body)
}

func TestReserveTimeout(t *testing.T) {
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
		server.SendResponse("TIMED_OUT")
	}()

	job, err := client.ReserveWithTimeout(1)
	assert.Error(t, err)
	assert.Nil(t, job)
	assert.Contains(t, err.Error(), "TIMED_OUT")
}

func TestReserveDeadlineSoon(t *testing.T) {
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
		server.SendResponse("DEADLINE_SOON")
	}()

	job, err := client.Reserve()
	assert.Error(t, err)
	assert.Nil(t, job)
	assert.Contains(t, err.Error(), "DEADLINE_SOON")
}

func TestDelete(t *testing.T) {
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
		server.SendResponse("DELETED")
	}()

	err = client.Delete(123)
	assert.NoError(t, err)
}

func TestDeleteNotFound(t *testing.T) {
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

	err = client.Delete(999)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NOT_FOUND")
}

func TestRelease(t *testing.T) {
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
		server.SendResponse("RELEASED")
	}()

	err = client.Release(123, 1024, 0)
	assert.NoError(t, err)
}

func TestReleaseBuried(t *testing.T) {
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
		server.SendResponse("BURIED")
	}()

	err = client.Release(123, 1024, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "BURIED")
}

func TestBury(t *testing.T) {
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
		server.SendResponse("BURIED")
	}()

	err = client.Bury(123, 1024)
	assert.NoError(t, err)
}

func TestBuryNotFound(t *testing.T) {
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

	err = client.Bury(999, 1024)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NOT_FOUND")
}

func TestTouch(t *testing.T) {
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
		server.SendResponse("TOUCHED")
	}()

	err = client.Touch(123)
	assert.NoError(t, err)
}

func TestTouchNotFound(t *testing.T) {
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

	err = client.Touch(999)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NOT_FOUND")
}

func TestWatch(t *testing.T) {
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
		server.SendResponse("WATCHING 1")
	}()

	count, err := client.Watch("my-tube")
	require.NoError(t, err)
	assert.Equal(t, uint32(1), count)
}

func TestWatchInvalidTube(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	client, err := Connect(server.Address())
	require.NoError(t, err)
	defer client.Close()

	// Test invalid tube name
	count, err := client.Watch("-invalid-tube")
	assert.Error(t, err)
	assert.Equal(t, uint32(0), count)
}

func TestIgnore(t *testing.T) {
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
		server.SendResponse("WATCHING 0")
	}()

	count, err := client.Ignore("my-tube")
	require.NoError(t, err)
	assert.Equal(t, uint32(0), count)
}

func TestReserveJobWithContext(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	client, err := Connect(server.Address())
	require.NoError(t, err)
	defer client.Close()

	// Test context cancellation
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	job, err := client.ReserveJob(ctx)
	assert.Error(t, err)
	assert.Nil(t, job)
	assert.Equal(t, context.Canceled, err)
}

func TestReserveJobWithTimeout(t *testing.T) {
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
		server.SendReservedResponse(789, 12, "timeout test")
	}()

	job, err := client.ReserveJobWithTimeout(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, uint64(789), job.ID)
	assert.Equal(t, []byte("timeout test"), job.Body)
}
