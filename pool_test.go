// Package beanstalkd pool tests.
package beanstalkd

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPool(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	config := &PoolConfig{
		MaxConnections:      5,
		MinConnections:      2,
		MaxIdleTime:         1 * time.Minute,
		ConnectTimeout:      5 * time.Second,
		HealthCheckInterval: 30 * time.Second,
	}

	pool, err := NewPool(server.Address(), config)
	require.NoError(t, err)
	defer pool.Close()

	assert.Equal(t, server.Address(), pool.addr)
	assert.Equal(t, config, pool.config)
	assert.NotNil(t, pool.clients)
	assert.NotNil(t, pool.ctx)
	assert.NotNil(t, pool.cancel)
}

func TestNewPoolDefaultConfig(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	pool, err := NewPool(server.Address(), nil)
	require.NoError(t, err)
	defer pool.Close()

	assert.Equal(t, DefaultPoolConfig().MaxConnections, pool.config.MaxConnections)
	assert.Equal(t, DefaultPoolConfig().MinConnections, pool.config.MinConnections)
}

func TestNewPoolInvalidConfig(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	config := &PoolConfig{
		MaxConnections: 0, // Invalid
	}

	pool, err := NewPool(server.Address(), config)
	assert.Error(t, err)
	assert.Nil(t, pool)
}

func TestPoolGetPut(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	// Mock server responses for health checks
	go func() {
		time.Sleep(10 * time.Millisecond)
		// Send a simple response for health checks
		server.SendResponse("OK 0\r\n")
	}()

	config := &PoolConfig{
		MaxConnections:      2,
		MinConnections:      1,
		MaxIdleTime:         1 * time.Minute,
		ConnectTimeout:      5 * time.Second,
		HealthCheckInterval: 30 * time.Second,
	}

	pool, err := NewPool(server.Address(), config)
	require.NoError(t, err)
	defer pool.Close()

	// Get a client
	client, err := pool.Get()
	require.NoError(t, err)
	assert.NotNil(t, client)

	// Put it back
	pool.Put(client)

	// Get another client
	client2, err := pool.Get()
	require.NoError(t, err)
	assert.NotNil(t, client2)
}

func TestPoolStats(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	// Mock server responses for health checks
	go func() {
		time.Sleep(10 * time.Millisecond)
		// Send a simple response for health checks
		server.SendResponse("OK 0\r\n")
	}()

	config := &PoolConfig{
		MaxConnections:      3,
		MinConnections:      1,
		MaxIdleTime:         1 * time.Minute,
		ConnectTimeout:      5 * time.Second,
		HealthCheckInterval: 30 * time.Second,
	}

	pool, err := NewPool(server.Address(), config)
	require.NoError(t, err)
	defer pool.Close()

	stats := pool.Stats()
	assert.Equal(t, 1, stats["total_connections"])
	assert.Equal(t, 1, stats["available"])
	assert.Equal(t, 1, stats["active"])
	assert.Equal(t, 0, stats["idle"])
	assert.Equal(t, 3, stats["max_connections"])
	assert.Equal(t, 1, stats["min_connections"])
}

func TestPoolExecute(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	// Mock server responses for health checks
	go func() {
		time.Sleep(10 * time.Millisecond)
		// Send a simple response for health checks
		server.SendResponse("OK 0\r\n")
	}()

	config := &PoolConfig{
		MaxConnections:      2,
		MinConnections:      1,
		MaxIdleTime:         1 * time.Minute,
		ConnectTimeout:      5 * time.Second,
		HealthCheckInterval: 30 * time.Second,
	}

	pool, err := NewPool(server.Address(), config)
	require.NoError(t, err)
	defer pool.Close()

	var executed bool
	err = pool.Execute(func(client *Client) error {
		executed = true
		assert.NotNil(t, client)
		return nil
	})

	require.NoError(t, err)
	assert.True(t, executed)
}

func TestPoolExecuteWithTimeout(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	// Mock server responses for health checks
	go func() {
		time.Sleep(10 * time.Millisecond)
		// Send a simple response for health checks
		server.SendResponse("OK 0\r\n")
	}()

	config := &PoolConfig{
		MaxConnections:      2,
		MinConnections:      1,
		MaxIdleTime:         1 * time.Minute,
		ConnectTimeout:      5 * time.Second,
		HealthCheckInterval: 30 * time.Second,
	}

	pool, err := NewPool(server.Address(), config)
	require.NoError(t, err)
	defer pool.Close()

	var executed bool
	err = pool.ExecuteWithTimeout(5*time.Second, func(client *Client) error {
		executed = true
		assert.NotNil(t, client)
		return nil
	})

	require.NoError(t, err)
	assert.True(t, executed)
}

func TestPoolExecuteTimeout(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	// Mock server responses for health checks
	go func() {
		time.Sleep(10 * time.Millisecond)
		// Send a simple response for health checks
		server.SendResponse("OK 0\r\n")
	}()

	config := &PoolConfig{
		MaxConnections:      1,
		MinConnections:      0,
		MaxIdleTime:         1 * time.Minute,
		ConnectTimeout:      5 * time.Second,
		HealthCheckInterval: 30 * time.Second,
	}

	pool, err := NewPool(server.Address(), config)
	require.NoError(t, err)
	defer pool.Close()

	// Get the only connection
	client, err := pool.Get()
	require.NoError(t, err)

	// Try to execute with timeout (should timeout)
	err = pool.ExecuteWithTimeout(100*time.Millisecond, func(client *Client) error {
		return nil
	})

	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)

	// Return the connection
	pool.Put(client)
}

func TestPoolClose(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	// Mock server responses for health checks
	go func() {
		time.Sleep(10 * time.Millisecond)
		// Send a simple response for health checks
		server.SendResponse("OK 0\r\n")
	}()

	config := &PoolConfig{
		MaxConnections:      2,
		MinConnections:      1,
		MaxIdleTime:         1 * time.Minute,
		ConnectTimeout:      5 * time.Second,
		HealthCheckInterval: 30 * time.Second,
	}

	pool, err := NewPool(server.Address(), config)
	require.NoError(t, err)

	// Get a client
	client, err := pool.Get()
	require.NoError(t, err)

	// Close the pool
	err = pool.Close()
	assert.NoError(t, err)

	// Try to get another client (should fail)
	_, err = pool.Get()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pool is closed")

	// Try to put the client back (should close it)
	pool.Put(client)
}

func TestPoolPutNil(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	// Mock server responses for health checks
	go func() {
		time.Sleep(10 * time.Millisecond)
		// Send a simple response for health checks
		server.SendResponse("OK 0\r\n")
	}()

	pool, err := NewPool(server.Address(), nil)
	require.NoError(t, err)
	defer pool.Close()

	// Put nil client (should not panic)
	pool.Put(nil)
}

func TestDefaultPoolConfig(t *testing.T) {
	config := DefaultPoolConfig()
	assert.Equal(t, 10, config.MaxConnections)
	assert.Equal(t, 2, config.MinConnections)
	assert.Equal(t, 5*time.Minute, config.MaxIdleTime)
	assert.Equal(t, 5*time.Second, config.ConnectTimeout)
	assert.Equal(t, 30*time.Second, config.HealthCheckInterval)
}
