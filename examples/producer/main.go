package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"taskhub_client"
)

func main() {
	endpoint := flag.String("url", "ws://localhost:5173/api/taskhub/v1/ws", "TaskHub WebSocket URL")
	queue := flag.String("queue", "linux-builds", "TaskHub queue slug")
	requestID := flag.String("id", "", "stable request ID for this submission")
	a := flag.Int("a", 2, "first operand")
	b := flag.Int("b", 3, "second operand")
	runAt := flag.String("run-at", "", "optional future RFC 3339 timestamp")
	flag.Parse()
	if *requestID == "" {
		log.Fatal("pass -id with a stable request ID")
	}

	submission := taskhub.Submission{
		Queue:     *queue,
		RequestID: *requestID,
		Payload:   map[string]any{"a": *a, "b": *b},
		Tags:      []string{"example"},
	}
	if *runAt != "" {
		when, err := time.Parse(time.RFC3339Nano, *runAt)
		if err != nil {
			log.Fatal("pass -run-at in RFC 3339 format")
		}
		submission.ScheduledAt = when
	}
	client, err := taskhub.NewClient(*endpoint, os.Getenv("TASKHUB_TOKEN"))
	if err != nil {
		log.Fatal(err)
	}
	id, err := client.Submit(context.Background(), submission)
	if err != nil {
		var rejected *taskhub.Error
		if errors.As(err, &rejected) && rejected.TaskID != "" {
			log.Fatalf("submission %s: %v (task %s)", *requestID, err, rejected.TaskID)
		}
		log.Fatalf("submission %s: %v", *requestID, err)
	}
	fmt.Println("Accepted task", id)
}
