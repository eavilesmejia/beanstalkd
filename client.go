// Package beanstalkd provides a high-performance Go client for the Beanstalk work queue.
//
// Beanstalk is a simple, fast work queue. Its interface is generic, but was originally
// designed for reducing the latency of page views in high-volume web applications
// by running time-consuming tasks asynchronously.
//
// This client implements the complete Beanstalk protocol as specified in:
// https://raw.githubusercontent.com/beanstalkd/beanstalkd/master/doc/protocol.txt
//
// Example usage:
//
//	client, err := beanstalkd.Connect("localhost:11300")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
//	// Producer
//	jobID, err := client.Put("default", 1024, 0, 60, []byte("hello world"))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Worker
//	job, err := client.Reserve()
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Delete(job.ID)
//
//	fmt.Printf("Job %d: %s\n", job.ID, string(job.Body))
package beanstalkd

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Job represents a job in the Beanstalk queue.
type Job struct {
	ID       uint64
	Body     []byte
	Priority uint32
	Delay    uint32
	TTR      uint32
}

// JobStats represents statistics for a specific job.
type JobStats struct {
	ID       uint64 `yaml:"id"`
	Tube     string `yaml:"tube"`
	State    string `yaml:"state"`
	Priority uint32 `yaml:"priority"`
	Age      uint32 `yaml:"age"`
	Delay    uint32 `yaml:"delay"`
	TTR      uint32 `yaml:"ttr"`
	TimeLeft uint32 `yaml:"time-left"`
	File     uint32 `yaml:"file"`
	Reserves uint32 `yaml:"reserves"`
	Timeouts uint32 `yaml:"timeouts"`
	Releases uint32 `yaml:"releases"`
	Buries   uint32 `yaml:"buries"`
	Kicks    uint32 `yaml:"kicks"`
}

// TubeStats represents statistics for a specific tube.
type TubeStats struct {
	Name                string `yaml:"name"`
	CurrentJobsUrgent   uint32 `yaml:"current-jobs-urgent"`
	CurrentJobsReady    uint32 `yaml:"current-jobs-ready"`
	CurrentJobsReserved uint32 `yaml:"current-jobs-reserved"`
	CurrentJobsDelayed  uint32 `yaml:"current-jobs-delayed"`
	CurrentJobsBuried   uint32 `yaml:"current-jobs-buried"`
	TotalJobs           uint64 `yaml:"total-jobs"`
	CurrentUsing        uint32 `yaml:"current-using"`
	CurrentWaiting      uint32 `yaml:"current-waiting"`
	CurrentWatching     uint32 `yaml:"current-watching"`
	Pause               uint32 `yaml:"pause"`
	CmdDelete           uint64 `yaml:"cmd-delete"`
	CmdPauseTube        uint64 `yaml:"cmd-pause-tube"`
	PauseTimeLeft       uint32 `yaml:"pause-time-left"`
}

// ServerStats represents statistics for the entire server.
type ServerStats struct {
	CurrentJobsUrgent     uint32 `yaml:"current-jobs-urgent"`
	CurrentJobsReady      uint32 `yaml:"current-jobs-ready"`
	CurrentJobsReserved   uint32 `yaml:"current-jobs-reserved"`
	CurrentJobsDelayed    uint32 `yaml:"current-jobs-delayed"`
	CurrentJobsBuried     uint32 `yaml:"current-jobs-buried"`
	CmdPut                uint64 `yaml:"cmd-put"`
	CmdPeek               uint64 `yaml:"cmd-peek"`
	CmdPeekReady          uint64 `yaml:"cmd-peek-ready"`
	CmdPeekDelayed        uint64 `yaml:"cmd-peek-delayed"`
	CmdPeekBuried         uint64 `yaml:"cmd-peek-buried"`
	CmdReserve            uint64 `yaml:"cmd-reserve"`
	CmdReserveWithTimeout uint64 `yaml:"cmd-reserve-with-timeout"`
	CmdTouch              uint64 `yaml:"cmd-touch"`
	CmdUse                uint64 `yaml:"cmd-use"`
	CmdWatch              uint64 `yaml:"cmd-watch"`
	CmdIgnore             uint64 `yaml:"cmd-ignore"`
	CmdDelete             uint64 `yaml:"cmd-delete"`
	CmdRelease            uint64 `yaml:"cmd-release"`
	CmdBury               uint64 `yaml:"cmd-bury"`
	CmdKick               uint64 `yaml:"cmd-kick"`
	CmdStats              uint64 `yaml:"cmd-stats"`
	CmdStatsJob           uint64 `yaml:"cmd-stats-job"`
	CmdStatsTube          uint64 `yaml:"cmd-stats-tube"`
	CmdListTubes          uint64 `yaml:"cmd-list-tubes"`
	CmdListTubeUsed       uint64 `yaml:"cmd-list-tube-used"`
	CmdListTubesWatched   uint64 `yaml:"cmd-list-tubes-watched"`
	CmdPauseTube          uint64 `yaml:"cmd-pause-tube"`
	JobTimeouts           uint64 `yaml:"job-timeouts"`
	TotalJobs             uint64 `yaml:"total-jobs"`
	MaxJobSize            uint32 `yaml:"max-job-size"`
	CurrentTubes          uint32 `yaml:"current-tubes"`
	CurrentConnections    uint32 `yaml:"current-connections"`
	CurrentProducers      uint32 `yaml:"current-producers"`
	CurrentWorkers        uint32 `yaml:"current-workers"`
	CurrentWaiting        uint32 `yaml:"current-waiting"`
	TotalConnections      uint64 `yaml:"total-connections"`
	PID                   uint32 `yaml:"pid"`
	Version               string `yaml:"version"`
	RusageUtime           string `yaml:"rusage-utime"`
	RusageStime           string `yaml:"rusage-stime"`
	Uptime                uint64 `yaml:"uptime"`
	BinlogOldestIndex     uint32 `yaml:"binlog-oldest-index"`
	BinlogCurrentIndex    uint32 `yaml:"binlog-current-index"`
	BinlogMaxSize         uint32 `yaml:"binlog-max-size"`
	BinlogRecordsWritten  uint64 `yaml:"binlog-records-written"`
	BinlogRecordsMigrated uint64 `yaml:"binlog-records-migrated"`
	Draining              bool   `yaml:"draining"`
	ID                    string `yaml:"id"`
	Hostname              string `yaml:"hostname"`
	OS                    string `yaml:"os"`
	Platform              string `yaml:"platform"`
}

// Error represents a Beanstalk protocol error.
type Error struct {
	Type    string
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("beanstalkd %s: %s", e.Type, e.Message)
}

// Client represents a Beanstalk client connection.
type Client struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	mutex  sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

// Connect establishes a connection to a Beanstalk server.
func Connect(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
		ctx:    ctx,
		cancel: cancel,
	}

	return client, nil
}

// ConnectWithTimeout establishes a connection to a Beanstalk server with a timeout.
func ConnectWithTimeout(addr string, timeout time.Duration) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
		ctx:    ctx,
		cancel: cancel,
	}

	return client, nil
}

// Close closes the connection to the Beanstalk server.
func (c *Client) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Cancel context first
	if c.cancel != nil {
		c.cancel()
	}

	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}

	if err := c.Quit(); err != nil {
		return err
	}
	return nil
}

// sendCommand sends a command to the server and returns the response.
func (c *Client) sendCommand(command string) (string, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.conn == nil {
		return "", fmt.Errorf("connection is closed")
	}

	// Check context
	select {
	case <-c.ctx.Done():
		return "", c.ctx.Err()
	default:
	}

	// Send command
	if _, err := c.writer.WriteString(command + "\r\n"); err != nil {
		return "", fmt.Errorf("failed to send command: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return "", fmt.Errorf("failed to flush command: %w", err)
	}

	// Read response
	response, err := c.reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	// Remove trailing \r\n
	response = strings.TrimRight(response, "\r\n")

	return response, nil
}

// sendCommandWithBody sends a command with a body to the server and returns the response.
func (c *Client) sendCommandWithBody(command string, body []byte) (string, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.conn == nil {
		return "", fmt.Errorf("connection is closed")
	}

	// Check context
	select {
	case <-c.ctx.Done():
		return "", c.ctx.Err()
	default:
	}

	// Send command
	if _, err := c.writer.WriteString(command + "\r\n"); err != nil {
		return "", fmt.Errorf("failed to send command: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return "", fmt.Errorf("failed to flush command: %w", err)
	}

	// Send body
	if _, err := c.writer.Write(body); err != nil {
		return "", fmt.Errorf("failed to send body: %w", err)
	}
	if _, err := c.writer.WriteString("\r\n"); err != nil {
		return "", fmt.Errorf("failed to send body terminator: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return "", fmt.Errorf("failed to flush body: %w", err)
	}

	// Read response
	response, err := c.reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	// Remove trailing \r\n
	response = strings.TrimRight(response, "\r\n")

	return response, nil
}

// Error definitions for better type safety and performance
var (
	errorTypes = map[string]string{
		"OUT_OF_MEMORY":   "server cannot allocate enough memory for the job",
		"INTERNAL_ERROR":  "internal server error",
		"BAD_FORMAT":      "command line was not well-formed",
		"UNKNOWN_COMMAND": "server does not know this command",
		"EXPECTED_CRLF":   "job body must be followed by a CR-LF pair",
		"JOB_TOO_BIG":     "job body is larger than max-job-size",
		"DRAINING":        "server is in drain mode and not accepting new jobs",
		"NOT_FOUND":       "job or tube not found",
		"DEADLINE_SOON":   "job deadline is soon",
		"TIMED_OUT":       "reserve command timed out",
	}
)

// parseError parses a Beanstalk error response.
func parseError(response string) error {
	if message, exists := errorTypes[response]; exists {
		return &Error{Type: response, Message: message}
	}
	return &Error{Type: "UNKNOWN", Message: response}
}

// IsConnected checks if the client is still connected to the server.
func (c *Client) IsConnected() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.conn != nil
}

// Context returns the client's context for cancellation and timeout handling.
func (c *Client) Context() context.Context {
	return c.ctx
}

// ValidateJobBody validates a job body according to Beanstalk protocol requirements.
func ValidateJobBody(body []byte) error {
	if len(body) == 0 {
		return fmt.Errorf("job body cannot be empty")
	}
	if len(body) > 2*1024*1024 { // 2MB max job size
		return fmt.Errorf("job body exceeds maximum size of 2MB")
	}
	return nil
}

// ParseJobID parses a job ID from a string.
func ParseJobID(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}

// FormatJobID formats a job ID as a string.
func FormatJobID(id uint64) string {
	return strconv.FormatUint(id, 10)
}
