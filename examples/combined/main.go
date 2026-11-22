package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/yuksbg/taskhub-client"
)

func main() {
	endpoint := flag.String("url", "ws://localhost:5173/api/taskhub/v1/ws", "TaskHub WebSocket URL")
	queue := flag.String("queue", "linux-builds", "TaskHub queue slug")
	a := flag.Int("a", 2, "first operand")
	b := flag.Int("b", 3, "second operand")
	id := flag.String("id", "", "stable ID for this submission")
	flag.Parse()
	if *id == "" {
		log.Fatal("pass -id with a stable request ID")
	}
	client, err := taskhub.NewClient(*endpoint, os.Getenv("TASKHUB_TOKEN"))
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	session, err := client.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	submitted := make(chan error, 1)
	go func() {
		taskID, err := session.Submit(ctx, taskhub.Submission{
			Queue:     *queue,
			RequestID: *id,
			Payload:   map[string]any{"a": *a, "b": *b},
		})
		if err == nil {
			fmt.Println("Accepted task", taskID)
		}
		submitted <- err
		if err != nil {
			cancel()
		}
	}()
	workErr := session.Work(ctx, *queue, func(ctx context.Context, task taskhub.Task) (map[string]any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		a, aOK := task.Payload["a"].(float64)
		b, bOK := task.Payload["b"].(float64)
		if !aOK || !bOK {
			return nil, errors.New("expected numeric a and b")
		}
		fmt.Printf("Processing task %s with sum %g\n", task.ID, a+b)
		return map[string]any{"sum": a + b}, nil
	})
	submitErr := <-submitted
	if workErr != nil && !errors.Is(workErr, context.Canceled) {
		log.Fatal(workErr)
	}
	if submitErr != nil && !errors.Is(submitErr, context.Canceled) {
		log.Fatal(submitErr)
	}
}
