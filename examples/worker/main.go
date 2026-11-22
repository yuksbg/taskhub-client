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
	flag.Parse()

	client, err := taskhub.NewClient(*endpoint, os.Getenv("TASKHUB_TOKEN"))
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("Connecting to consume %s with one worker slot\n", *queue)
	err = client.Work(ctx, *queue, func(ctx context.Context, task taskhub.Task) (map[string]any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		a, aOK := task.Payload["a"].(float64)
		b, bOK := task.Payload["b"].(float64)
		if !aOK || !bOK {
			return nil, errors.New("expected numeric a and b")
		}
		fmt.Printf("Processed task %s with sum %g\n", task.ID, a+b)
		return map[string]any{"sum": a + b}, nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
