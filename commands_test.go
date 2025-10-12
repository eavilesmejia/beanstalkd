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

func TestStats(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	client, err := Connect(server.Address())
	require.NoError(t, err)
	defer client.Close()

	// Mock server response with sample stats data
	go func() {
		time.Sleep(10 * time.Millisecond)
		// Send YAML stats data
		yamlData := `current-jobs-urgent: 0
current-jobs-ready: 1
current-jobs-reserved: 0
current-jobs-delayed: 0
current-jobs-buried: 0
cmd-put: 1
cmd-peek: 0
cmd-peek-ready: 0
cmd-peek-delayed: 0
cmd-peek-buried: 0
cmd-reserve: 0
cmd-reserve-with-timeout: 0
cmd-touch: 0
cmd-use: 1
cmd-watch: 0
cmd-ignore: 0
cmd-delete: 0
cmd-release: 0
cmd-bury: 0
cmd-kick: 0
cmd-stats: 1
cmd-stats-job: 0
cmd-stats-tube: 0
cmd-list-tubes: 0
cmd-list-tube-used: 0
cmd-list-tubes-watched: 0
cmd-pause-tube: 0
job-timeouts: 0
total-jobs: 1
max-job-size: 65535
current-tubes: 1
current-connections: 1
current-producers: 1
current-workers: 0
current-waiting: 0
total-connections: 1
pid: 12345
version: "1.12"
rusage-utime: "0.000000"
rusage-stime: "0.000000"
uptime: 123456
binlog-oldest-index: 0
binlog-current-index: 0
binlog-max-size: 10485760
binlog-records-written: 0
binlog-records-migrated: 0
draining: false
id: "test-server-id"
hostname: "test-host"
os: "darwin"
platform: "x86_64"`
		server.SendStatsResponse(len(yamlData), yamlData)
	}()

	stats, err := client.Stats()
	require.NoError(t, err)
	assert.NotNil(t, stats)

	// Verify some key fields
	assert.Equal(t, uint32(0), stats.CurrentJobsUrgent)
	assert.Equal(t, uint32(1), stats.CurrentJobsReady)
	assert.Equal(t, uint32(0), stats.CurrentJobsReserved)
	assert.Equal(t, uint32(0), stats.CurrentJobsDelayed)
	assert.Equal(t, uint32(0), stats.CurrentJobsBuried)
	assert.Equal(t, uint64(1), stats.CmdPut)
	assert.Equal(t, uint64(1), stats.CmdUse)
	assert.Equal(t, uint64(1), stats.CmdStats)
	assert.Equal(t, uint64(1), stats.TotalJobs)
	assert.Equal(t, uint32(65535), stats.MaxJobSize)
	assert.Equal(t, uint32(1), stats.CurrentTubes)
	assert.Equal(t, uint32(1), stats.CurrentConnections)
	assert.Equal(t, uint32(1), stats.CurrentProducers)
	assert.Equal(t, uint32(0), stats.CurrentWorkers)
	assert.Equal(t, uint64(1), stats.TotalConnections)
	assert.Equal(t, uint32(12345), stats.PID)
	assert.Equal(t, "1.12", stats.Version)
	assert.Equal(t, "0.000000", stats.RusageUtime)
	assert.Equal(t, "0.000000", stats.RusageStime)
	assert.Equal(t, uint64(123456), stats.Uptime)
	assert.Equal(t, uint32(0), stats.BinlogOldestIndex)
	assert.Equal(t, uint32(0), stats.BinlogCurrentIndex)
	assert.Equal(t, uint32(10485760), stats.BinlogMaxSize)
	assert.Equal(t, uint64(0), stats.BinlogRecordsWritten)
	assert.Equal(t, uint64(0), stats.BinlogRecordsMigrated)
	assert.Equal(t, false, stats.Draining)
	assert.Equal(t, "test-server-id", stats.ID)
	assert.Equal(t, "test-host", stats.Hostname)
	assert.Equal(t, "darwin", stats.OS)
	assert.Equal(t, "x86_64", stats.Platform)
}

func TestStatsError(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	client, err := Connect(server.Address())
	require.NoError(t, err)
	defer client.Close()

	// Mock server error response
	go func() {
		time.Sleep(10 * time.Millisecond)
		server.SendResponse("INTERNAL_ERROR")
	}()

	stats, err := client.Stats()
	assert.Error(t, err)
	assert.Nil(t, stats)
	assert.Contains(t, err.Error(), "INTERNAL_ERROR")
}

func TestStatsInvalidData(t *testing.T) {
	server, err := newMockServer()
	require.NoError(t, err)
	defer server.Close()

	// Wait for connection
	time.Sleep(10 * time.Millisecond)

	client, err := Connect(server.Address())
	require.NoError(t, err)
	defer client.Close()

	// Mock server response with invalid data size
	go func() {
		time.Sleep(10 * time.Millisecond)
		server.SendResponse("OK invalid")
	}()

	stats, err := client.Stats()
	assert.Error(t, err)
	assert.Nil(t, stats)
	assert.Contains(t, err.Error(), "invalid data size")
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
