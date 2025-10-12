package main

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/eavilesmejia/beanstalkd"
)

func main() {
	// Example 1: Basic producer and consumer
	fmt.Println("=== Basic Producer/Consumer Example ===")
	basicExample()

	// Example 2: Connection pool example
	fmt.Println("\n=== Connection Pool Example ===")
	poolExample()

	// Example 3: Error handling example
	fmt.Println("\n=== Error Handling Example ===")
	errorHandlingExample()
}

func basicExample() {
	// Note: This example assumes a Beanstalk server is running on localhost:11300
	// For demonstration purposes, we'll show the code structure

	// Producer
	client, err := beanstalkd.Connect("localhost:11300")
	if err != nil {
		log.Printf("Failed to connect: %v", err)
		return
	}
	defer client.Close()

	// Check connection status
	if !client.IsConnected() {
		log.Printf("Client is not connected")
		return
	}

	// Put a job with validation
	jobBody := []byte("Hello, Beanstalk!")
	if err := beanstalkd.ValidateJobBody(jobBody); err != nil {
		log.Printf("Invalid job body: %v", err)
		return
	}

	jobID, err := client.Put("example-tube", 1024, 0, 60, jobBody)
	if err != nil {
		log.Printf("Failed to put job: %v", err)
		return
	}
	fmt.Printf("Job %d created successfully\n", jobID)

	// Consumer
	// Watch the tube
	_, err = client.Watch("example-tube")
	if err != nil {
		log.Printf("Failed to watch tube: %v", err)
		return
	}

	// Reserve a job with context support
	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	// defer cancel()

	// job, err := client.ReserveJob(ctx)
	// if err != nil {
	//     if err == context.DeadlineExceeded {
	//         log.Printf("No job available within timeout")
	//         return
	//     }
	//     log.Printf("Failed to reserve job: %v", err)
	//     return
	// }
	// defer client.Delete(job.ID)
	// fmt.Printf("Processing job %d: %s\n", job.ID, string(job.Body))
}

func poolExample() {
	// Create connection pool
	config := &beanstalkd.PoolConfig{
		MaxConnections:      5,
		MinConnections:      2,
		MaxIdleTime:         5 * time.Minute,
		ConnectTimeout:      5 * time.Second,
		HealthCheckInterval: 30 * time.Second,
	}

	pool, err := beanstalkd.NewPool("localhost:11300", config)
	if err != nil {
		log.Printf("Failed to create pool: %v", err)
		return
	}
	defer pool.Close()

	// Use pool to execute operations
	err = pool.Execute(func(client *beanstalkd.Client) error {
		// Validate job body
		jobBody := []byte("Pooled job")
		if err := beanstalkd.ValidateJobBody(jobBody); err != nil {
			return fmt.Errorf("invalid job body: %w", err)
		}

		// Put a job
		jobID, err := client.Put("pool-tube", 1024, 0, 60, jobBody)
		if err != nil {
			return err
		}
		fmt.Printf("Job %d created via pool\n", jobID)
		return nil
	})

	if err != nil {
		log.Printf("Pool operation failed: %v", err)
		return
	}

	// Get pool statistics
	stats := pool.Stats()
	fmt.Printf("Pool stats: %+v\n", stats)
}

func errorHandlingExample() {
	client, err := beanstalkd.Connect("localhost:11300")
	if err != nil {
		log.Printf("Failed to connect: %v", err)
		return
	}
	defer client.Close()

	// Example of error handling
	err = client.Use("") // Invalid tube name
	if err != nil {
		var beanstalkErr *beanstalkd.Error
		if errors.As(err, &beanstalkErr) {
			fmt.Printf("Beanstalk error - Type: %s, Message: %s\n", beanstalkErr.Type, beanstalkErr.Message)
		} else {
			fmt.Printf("Other error: %v\n", err)
		}
	}

	// Example of context handling
	// ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	// defer cancel()

	// Check if client is still connected
	if !client.IsConnected() {
		fmt.Println("Client is not connected")
		return
	}

	// Example of timeout handling
	// job, err := client.ReserveJob(ctx)
	// if err != nil {
	//     if errors.Is(err, context.DeadlineExceeded) {
	//         fmt.Println("Operation timed out")
	//     } else {
	//         fmt.Printf("Other error: %v\n", err)
	//     }
	// }
}
