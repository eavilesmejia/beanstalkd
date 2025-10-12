# Beanstalkd Go Client

A high-performance, feature-complete Go client for the [Beanstalk work queue](https://beanstalkd.github.io/). This client implements the complete Beanstalk protocol as specified in the [official protocol documentation](https://raw.githubusercontent.com/beanstalkd/beanstalkd/master/doc/protocol.txt).

## Features

- **Complete Protocol Implementation**: Supports all Beanstalk commands including producer, worker, and administrative operations
- **High Performance**: Optimized for throughput with connection pooling and efficient I/O
- **Thread-Safe**: All operations are safe for concurrent use
- **Context Support**: Built-in support for Go contexts for cancellation and timeouts
- **Connection Pooling**: Automatic connection management with health checks
- **Comprehensive Testing**: Extensive test coverage with mock server support
- **Modern Go**: Written in Go 1.25+ with latest language features and optimizations

### Advanced Features

- **Improved Concurrency**: Uses `sync.RWMutex` for better read performance
- **Enhanced Context Handling**: Built-in context support for cancellation and timeouts
- **Optimized String Operations**: Improved string handling and validation
- **Better Memory Management**: Enhanced memory allocation patterns
- **Advanced Slice Operations**: Uses `slices` package for efficient slice manipulations
- **Enhanced Error Handling**: Improved error types and validation

## Installation

```bash
go get github.com/cw-elk/beanstalkd
```

## Quick Start

### Basic Usage

```go
package main

import (
    "fmt"
    "log"
    "github.com/cw-elk/beanstalkd"
)

func main() {
    // Connect to Beanstalk server
    client, err := beanstalkd.Connect("localhost:11300")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Producer: Put a job
    jobID, err := client.Put("my-tube", 1024, 0, 60, []byte("hello world"))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Job %d created\n", jobID)

    // Worker: Reserve and process a job
    job, err := client.Reserve()
    if err != nil {
        log.Fatal(err)
    }
    defer client.Delete(job.ID)

    fmt.Printf("Processing job %d: %s\n", job.ID, string(job.Body))
}
```

### Using Connection Pool

```go
package main

import (
    "fmt"
    "log"
    "time"
    "github.com/cw-elk/beanstalkd"
)

func main() {
    // Create connection pool
    config := &beanstalkd.PoolConfig{
        MaxConnections:      10,
        MinConnections:      2,
        MaxIdleTime:         5 * time.Minute,
        ConnectTimeout:      5 * time.Second,
        HealthCheckInterval: 30 * time.Second,
    }

    pool, err := beanstalkd.NewPool("localhost:11300", config)
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close()

    // Use pool to execute operations
    err = pool.Execute(func(client *beanstalkd.Client) error {
        jobID, err := client.Put("my-tube", 1024, 0, 60, []byte("pooled job"))
        if err != nil {
            return err
        }
        fmt.Printf("Job %d created via pool\n", jobID)
        return nil
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

## API Reference

### Connection Management

#### `Connect(addr string) (*Client, error)`
Establishes a connection to a Beanstalk server.

#### `ConnectWithTimeout(addr string, timeout time.Duration) (*Client, error)`
Establishes a connection with a timeout.

#### `Close() error`
Closes the connection to the server.

#### `IsConnected() bool`
Checks if the client is still connected to the server. Uses RWMutex for better performance.

#### `Context() context.Context`
Returns the client's context for cancellation and timeout handling.

### Producer Commands

#### `Put(tube string, priority, delay, ttr uint32, body []byte) (uint64, error)`
Inserts a job into the specified tube.

- `tube`: Tube name (will be created if it doesn't exist)
- `priority`: Job priority (0 = most urgent, 4294967295 = least urgent)
- `delay`: Seconds to wait before putting the job in the ready queue
- `ttr`: Time to run in seconds (minimum 1)
- `body`: Job body data

Returns the job ID on success.

#### `Use(tube string) error`
Sets the tube for subsequent Put commands.

### Worker Commands

#### `Reserve() (*Job, error)`
Reserves a job from any tube in the watch list. Blocks until a job becomes available.

#### `ReserveWithTimeout(timeout uint32) (*Job, error)`
Reserves a job with a timeout in seconds.

#### `Delete(jobID uint64) error`
Removes a job from the server entirely.

#### `Release(jobID uint64, priority, delay uint32) error`
Puts a reserved job back into the ready queue.

#### `Bury(jobID uint64, priority uint32) error`
Puts a job into the "buried" state.

#### `Touch(jobID uint64) error`
Requests more time to work on a job.

#### `Watch(tube string) (uint32, error)`
Adds a tube to the watch list.

#### `Ignore(tube string) (uint32, error)`
Removes a tube from the watch list.

### Context Support

#### `ReserveJob(ctx context.Context) (*Job, error)`
Reserves a job with context support for cancellation.

#### `ReserveJobWithTimeout(timeout time.Duration) (*Job, error)`
Reserves a job with a time.Duration timeout.

### Peek Commands

#### `Peek(jobID uint64) (*Job, error)`
Retrieves a job by its ID without reserving it.

#### `PeekReady() (*Job, error)`
Retrieves the next ready job without reserving it.

#### `PeekDelayed() (*Job, error)`
Retrieves the next delayed job without reserving it.

#### `PeekBuried() (*Job, error)`
Retrieves the next buried job without reserving it.

### Management Commands

#### `Kick(bound uint32) (uint32, error)`
Moves up to `bound` jobs from buried state to ready state.

#### `KickJob(jobID uint64) error`
Moves a specific job from buried state to ready state.

#### `Stats() (*ServerStats, error)`
Returns server statistics.

#### `StatsJob(jobID uint64) (*JobStats, error)`
Returns job statistics.

#### `StatsTube(tube string) (*TubeStats, error)`
Returns tube statistics.

#### `ListTubes() ([]string, error)`
Returns a list of all existing tubes.

#### `ListTubeUsed() (string, error)`
Returns the currently used tube.

#### `ListTubesWatched() ([]string, error)`
Returns a list of watched tubes.

#### `PauseTube(tube string, delay uint32) error`
Pauses a tube for the specified number of seconds.

### Utility Functions

#### `ValidateJobBody(body []byte) error`
Validates a job body according to Beanstalk protocol requirements.

#### `ParseJobID(s string) (uint64, error)`
Parses a job ID from a string.

#### `FormatJobID(id uint64) string`
Formats a job ID as a string.

### Connection Pooling

#### `NewPool(addr string, config *PoolConfig) (*Pool, error)`
Creates a new connection pool.

#### `PoolConfig`
Configuration for connection pooling:

```go
type PoolConfig struct {
    MaxConnections      int           // Maximum connections in pool
    MinConnections      int           // Minimum connections to maintain
    MaxIdleTime         time.Duration // Max idle time before closing
    ConnectTimeout      time.Duration // Connection timeout
    HealthCheckInterval time.Duration // Health check interval
}
```

#### `Pool.Execute(fn func(*Client) error) error`
Executes a function with a client from the pool.

#### `Pool.ExecuteWithTimeout(timeout time.Duration, fn func(*Client) error) error`
Executes a function with a timeout.

## Data Types

### Job
```go
type Job struct {
    ID       uint64
    Body     []byte
    Priority uint32
    Delay    uint32
    TTR      uint32
}
```

### JobStats
```go
type JobStats struct {
    ID          uint64
    Tube        string
    State       string
    Priority    uint32
    Age         uint32
    Delay       uint32
    TTR         uint32
    TimeLeft    uint32
    File        uint32
    Reserves    uint32
    Timeouts    uint32
    Releases    uint32
    Buries      uint32
    Kicks       uint32
}
```

### ServerStats
Contains comprehensive server statistics including job counts, command counts, and system information.

### TubeStats
Contains tube-specific statistics including job counts and command counts.

## Error Handling

The client provides detailed error information through the `Error` type:

```go
type Error struct {
    Type    string
    Message string
}
```

Common error types:
- `OUT_OF_MEMORY`: Server cannot allocate memory
- `INTERNAL_ERROR`: Internal server error
- `BAD_FORMAT`: Malformed command
- `UNKNOWN_COMMAND`: Unknown command
- `EXPECTED_CRLF`: Missing CRLF terminator
- `JOB_TOO_BIG`: Job exceeds max size
- `DRAINING`: Server in drain mode
- `NOT_FOUND`: Job or tube not found
- `DEADLINE_SOON`: Job deadline approaching
- `TIMED_OUT`: Operation timed out

## Examples

### Producer Example

```go
package main

import (
    "fmt"
    "log"
    "github.com/cw-elk/beanstalkd"
)

func main() {
    client, err := beanstalkd.Connect("localhost:11300")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Set tube
    err = client.Use("email-queue")
    if err != nil {
        log.Fatal(err)
    }

    // Put jobs with different priorities
    jobs := []struct {
        priority uint32
        delay    uint32
        ttr      uint32
        body     string
    }{
        {1024, 0, 60, "urgent email"},
        {2048, 30, 120, "delayed email"},
        {512, 0, 30, "high priority email"},
    }

    for i, job := range jobs {
        jobID, err := client.Put("email-queue", job.priority, job.delay, job.ttr, []byte(job.body))
        if err != nil {
            log.Printf("Failed to put job %d: %v", i, err)
            continue
        }
        fmt.Printf("Job %d created with ID %d\n", i, jobID)
    }
}
```

### Worker Example

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    "github.com/cw-elk/beanstalkd"
)

func main() {
    client, err := beanstalkd.Connect("localhost:11300")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Watch multiple tubes
    tubes := []string{"email-queue", "notification-queue", "image-processing"}
    for _, tube := range tubes {
        _, err := client.Watch(tube)
        if err != nil {
            log.Printf("Failed to watch tube %s: %v", tube, err)
        }
    }

    // Process jobs
    for {
        // Use context for cancellation
        ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
        job, err := client.ReserveJob(ctx)
        cancel()

        if err != nil {
            if err == context.DeadlineExceeded {
                fmt.Println("No jobs available, continuing...")
                continue
            }
            log.Printf("Failed to reserve job: %v", err)
            continue
        }

        // Process job
        fmt.Printf("Processing job %d: %s\n", job.ID, string(job.Body))
        
        // Simulate work
        time.Sleep(100 * time.Millisecond)

        // Delete job on success
        err = client.Delete(job.ID)
        if err != nil {
            log.Printf("Failed to delete job %d: %v", job.ID, err)
        } else {
            fmt.Printf("Job %d completed\n", job.ID)
        }
    }
}
```

### Pool Example

```go
package main

import (
    "fmt"
    "log"
    "sync"
    "time"
    "github.com/cw-elk/beanstalkd"
)

func main() {
    // Create pool
    config := &beanstalkd.PoolConfig{
        MaxConnections:      20,
        MinConnections:      5,
        MaxIdleTime:         5 * time.Minute,
        ConnectTimeout:      5 * time.Second,
        HealthCheckInterval: 30 * time.Second,
    }

    pool, err := beanstalkd.NewPool("localhost:11300", config)
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close()

    // Start multiple workers
    var wg sync.WaitGroup
    numWorkers := 10

    for i := 0; i < numWorkers; i++ {
        wg.Add(1)
        go func(workerID int) {
            defer wg.Done()
            
            for j := 0; j < 100; j++ {
                err := pool.Execute(func(client *beanstalkd.Client) error {
                    // Put a job
                    jobID, err := client.Put("test-tube", 1024, 0, 60, []byte(fmt.Sprintf("worker-%d-job-%d", workerID, j)))
                    if err != nil {
                        return err
                    }
                    
                    // Reserve and delete the job
                    job, err := client.ReserveWithTimeout(1)
                    if err != nil {
                        return err
                    }
                    
                    return client.Delete(job.ID)
                })
                
                if err != nil {
                    log.Printf("Worker %d failed: %v", workerID, err)
                }
            }
        }(i)
    }

    wg.Wait()
    fmt.Println("All workers completed")
}
```

### Monitoring Example

```go
package main

import (
    "fmt"
    "log"
    "time"
    "github.com/cw-elk/beanstalkd"
)

func main() {
    client, err := beanstalkd.Connect("localhost:11300")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Get server stats
    stats, err := client.Stats()
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Server Version: %s\n", stats.Version)
    fmt.Printf("Uptime: %d seconds\n", stats.Uptime)
    fmt.Printf("Current Connections: %d\n", stats.CurrentConnections)
    fmt.Printf("Total Jobs: %d\n", stats.TotalJobs)
    fmt.Printf("Ready Jobs: %d\n", stats.CurrentJobsReady)
    fmt.Printf("Reserved Jobs: %d\n", stats.CurrentJobsReserved)
    fmt.Printf("Delayed Jobs: %d\n", stats.CurrentJobsDelayed)
    fmt.Printf("Buried Jobs: %d\n", stats.CurrentJobsBuried)

    // List all tubes
    tubes, err := client.ListTubes()
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("\nTubes (%d):\n", len(tubes))
    for _, tube := range tubes {
        tubeStats, err := client.StatsTube(tube)
        if err != nil {
            log.Printf("Failed to get stats for tube %s: %v", tube, err)
            continue
        }
        
        fmt.Printf("  %s: ready=%d, reserved=%d, delayed=%d, buried=%d\n",
            tube,
            tubeStats.CurrentJobsReady,
            tubeStats.CurrentJobsReserved,
            tubeStats.CurrentJobsDelayed,
            tubeStats.CurrentJobsBuried)
    }
}
```

## Testing

The package includes comprehensive tests with a mock server implementation:

```bash
go test -v ./...
```

Run tests with coverage:

```bash
go test -v -cover ./...
```

## Performance Considerations

1. **Connection Pooling**: Use connection pools for high-throughput applications
2. **Batch Operations**: Group multiple operations when possible
3. **Timeout Handling**: Use appropriate timeouts to avoid blocking
4. **Error Handling**: Implement proper error handling and retry logic
5. **Resource Management**: Always close connections and return pooled clients

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## References

- [Beanstalk Protocol Documentation](https://raw.githubusercontent.com/beanstalkd/beanstalkd/master/doc/protocol.txt)
- [Beanstalkd Official Website](https://beanstalkd.github.io/)
- [Beanstalkd GitHub Repository](https://github.com/beanstalkd/beanstalkd)
