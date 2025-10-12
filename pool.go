// Package beanstalkd connection pooling implementation.
package beanstalkd

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"
)

// PoolConfig represents configuration for a connection pool.
type PoolConfig struct {
	// MaxConnections is the maximum number of connections in the pool.
	MaxConnections int

	// MinConnections is the minimum number of connections to maintain in the pool.
	MinConnections int

	// MaxIdleTime is the maximum time a connection can be idle before being closed.
	MaxIdleTime time.Duration

	// ConnectTimeout is the timeout for establishing new connections.
	ConnectTimeout time.Duration

	// HealthCheckInterval is the interval between health checks.
	HealthCheckInterval time.Duration
}

// DefaultPoolConfig returns a default pool configuration.
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		MaxConnections:      10,
		MinConnections:      2,
		MaxIdleTime:         5 * time.Minute,
		ConnectTimeout:      5 * time.Second,
		HealthCheckInterval: 30 * time.Second,
	}
}

// PooledClient represents a client in the connection pool.
type PooledClient struct {
	*Client
	lastUsed time.Time
	created  time.Time
}

// Pool represents a connection pool for Beanstalk clients.
type Pool struct {
	config     *PoolConfig
	addr       string
	clients    chan *PooledClient
	allClients []*PooledClient
	mutex      sync.RWMutex
	closed     bool
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewPool creates a new connection pool.
func NewPool(addr string, config *PoolConfig) (*Pool, error) {
	if config == nil {
		config = DefaultPoolConfig()
	}

	if config.MaxConnections <= 0 {
		return nil, fmt.Errorf("max connections must be greater than 0")
	}

	if config.MinConnections < 0 {
		config.MinConnections = 0
	}

	if config.MinConnections > config.MaxConnections {
		config.MinConnections = config.MaxConnections
	}

	ctx, cancel := context.WithCancel(context.Background())

	pool := &Pool{
		config:  config,
		addr:    addr,
		clients: make(chan *PooledClient, config.MaxConnections),
		ctx:     ctx,
		cancel:  cancel,
	}

	// Initialize minimum connections
	for i := 0; i < config.MinConnections; i++ {
		if err := pool.addConnection(); err != nil {
			pool.Close()
			return nil, fmt.Errorf("failed to initialize pool: %w", err)
		}
	}

	// Start health check goroutine
	go pool.healthCheck()

	return pool, nil
}

// Get retrieves a client from the pool.
func (p *Pool) Get() (*PooledClient, error) {
	p.mutex.RLock()
	closed := p.closed
	p.mutex.RUnlock()

	if closed {
		return nil, fmt.Errorf("pool is closed")
	}

	select {
	case client := <-p.clients:
		client.lastUsed = time.Now()
		return client, nil
	case <-p.ctx.Done():
		return nil, fmt.Errorf("pool is closed")
	default:
		// No available connections, try to create a new one
		p.mutex.Lock()
		defer p.mutex.Unlock()

		if p.closed {
			return nil, fmt.Errorf("pool is closed")
		}

		if len(p.allClients) >= p.config.MaxConnections {
			// Wait for a connection to become available
			select {
			case client := <-p.clients:
				client.lastUsed = time.Now()
				return client, nil
			case <-p.ctx.Done():
				return nil, fmt.Errorf("pool is closed")
			case <-time.After(p.config.ConnectTimeout):
				return nil, fmt.Errorf("timeout waiting for connection")
			}
		}

		// Create new connection
		if err := p.addConnection(); err != nil {
			return nil, fmt.Errorf("failed to create connection: %w", err)
		}

		// Try to get the connection we just created
		select {
		case client := <-p.clients:
			client.lastUsed = time.Now()
			return client, nil
		default:
			return nil, fmt.Errorf("failed to get connection from pool")
		}
	}
}

// Put returns a client to the pool.
func (p *Pool) Put(client *PooledClient) {
	if client == nil {
		return
	}

	p.mutex.RLock()
	closed := p.closed
	p.mutex.RUnlock()

	if closed {
		client.Close()
		return
	}

	// Check if connection is still valid
	if err := p.isConnectionHealthy(client); err != nil {
		client.Close()
		p.removeClient(client)
		return
	}

	select {
	case p.clients <- client:
		// Successfully returned to pool
	default:
		// Pool is full, close the connection
		client.Close()
		p.removeClient(client)
	}
}

// Close closes the pool and all connections.
func (p *Pool) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true
	p.cancel()

	// Close all connections
	for _, client := range p.allClients {
		client.Close()
	}

	// Close the channel
	close(p.clients)

	return nil
}

// Stats returns pool statistics.
func (p *Pool) Stats() map[string]interface{} {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	now := time.Now()
	var idleCount int
	var activeCount int

	for _, client := range p.allClients {
		if now.Sub(client.lastUsed) > p.config.MaxIdleTime {
			idleCount++
		} else {
			activeCount++
		}
	}

	return map[string]interface{}{
		"total_connections": len(p.allClients),
		"available":         len(p.clients),
		"active":            activeCount,
		"idle":              idleCount,
		"max_connections":   p.config.MaxConnections,
		"min_connections":   p.config.MinConnections,
	}
}

// addConnection adds a new connection to the pool.
func (p *Pool) addConnection() error {
	client, err := ConnectWithTimeout(p.addr, p.config.ConnectTimeout)
	if err != nil {
		return err
	}

	pooledClient := &PooledClient{
		Client:   client,
		lastUsed: time.Now(),
		created:  time.Now(),
	}

	p.allClients = append(p.allClients, pooledClient)

	select {
	case p.clients <- pooledClient:
		return nil
	default:
		// This shouldn't happen, but just in case
		client.Close()
		return fmt.Errorf("failed to add connection to pool")
	}
}

// removeClient removes a client from the pool.
func (p *Pool) removeClient(client *PooledClient) {
	// Use slices.Delete for better performance
	if i := slices.Index(p.allClients, client); i >= 0 {
		p.allClients = slices.Delete(p.allClients, i, i+1)
	}
}

// isConnectionHealthy checks if a connection is still healthy.
func (p *Pool) isConnectionHealthy(client *PooledClient) error {
	// Try to get server stats as a health check
	_, err := client.Stats()
	return err
}

// healthCheck performs periodic health checks on connections.
func (p *Pool) healthCheck() {
	ticker := time.NewTicker(p.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.performHealthCheck()
		}
	}
}

// performHealthCheck performs a health check on all connections.
func (p *Pool) performHealthCheck() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.closed {
		return
	}

	now := time.Now()
	var toRemove []*PooledClient

	// Check all connections
	for _, client := range p.allClients {
		// Check if connection is too old or unhealthy
		if now.Sub(client.lastUsed) > p.config.MaxIdleTime ||
			p.isConnectionHealthy(client) != nil {
			toRemove = append(toRemove, client)
		}
	}

	// Remove unhealthy connections
	for _, client := range toRemove {
		client.Close()
		p.removeClient(client)
	}

	// Ensure we have minimum connections
	for len(p.allClients) < p.config.MinConnections {
		if err := p.addConnection(); err != nil {
			// Log error but continue
			break
		}
	}
}

// Execute executes a function with a client from the pool.
func (p *Pool) Execute(fn func(*Client) error) error {
	client, err := p.Get()
	if err != nil {
		return err
	}
	defer p.Put(client)

	return fn(client.Client)
}

// ExecuteWithTimeout executes a function with a client from the pool with a timeout.
func (p *Pool) ExecuteWithTimeout(timeout time.Duration, fn func(*Client) error) error {
	ctx, cancel := context.WithTimeout(p.ctx, timeout)
	defer cancel()

	done := make(chan error, 1)

	go func() {
		client, err := p.Get()
		if err != nil {
			done <- err
			return
		}
		defer p.Put(client)

		done <- fn(client.Client)
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
