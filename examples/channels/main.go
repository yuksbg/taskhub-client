package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"taskhub_client"
)

func main() {
	url := flag.String("url", "ws://localhost:5173/api/taskhub/v1/ws", "TaskHub WebSocket URL")
	queue := flag.String("queue", "linux-builds", "queue slug")
	concurrency := flag.Int("concurrency", 2, "maximum concurrent deliveries (1–32)")
	count := flag.Int("count", 4, "number of example tasks to submit")
	id := flag.String("id", "", "stable request ID prefix")
	flag.Parse()
	if *id == "" {
		log.Fatal("pass -id with a stable request ID prefix")
	}
	if *count < 1 {
		log.Fatal("pass -count with a positive number")
	}
	cfg := taskhub.Config{URL: *url, Token: os.Getenv("TASKHUB_TOKEN"), Queue: *queue, Concurrency: *concurrency}
	if err := run(cfg, *id, *count); err != nil {
		log.Fatal(err)
	}
}

func run(cfg taskhub.Config, id string, count int) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	channels, err := taskhub.Default.Start(ctx, cfg)
	if err != nil {
		return err
	}

	workers := sync.WaitGroup{}
	defer func() {
		stop()
		<-channels.Done
		workers.Wait()
	}()
	for range cfg.Concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for delivery := range channels.Tasks {
				if delivery.Context.Err() != nil {
					continue
				}
				a, aOK := delivery.Payload["a"].(float64)
				b, bOK := delivery.Payload["b"].(float64)
				outcome := taskhub.Outcome{DeliveryID: delivery.DeliveryID}
				if aOK && bOK {
					outcome.Result = map[string]any{"sum": a + b}
				} else {
					outcome.Err = errors.New("expected numeric a and b")
				}
				select {
				case channels.Outcomes <- outcome:
				case <-delivery.Context.Done():
				case <-channels.Done:
				}
			}
		}()
	}

	fmt.Printf("Listening on %s with %d worker slots\n", cfg.Queue, cfg.Concurrency)
	submit, submitted, recorded := channels.Submit, channels.Submitted, channels.Recorded
	for index := 0; ; {
		request := taskhub.Submission{}
		if submit != nil {
			request = taskhub.Submission{
				Queue: cfg.Queue, RequestID: fmt.Sprintf("%s-%d", id, index),
				Payload: map[string]any{"a": index, "b": 3},
			}
		}
		select {
		case submit <- request:
			index++
			if index == count {
				submit = nil
			}
		case result, ok := <-submitted:
			if !ok {
				submitted = nil
			} else if result.Err != nil {
				return fmt.Errorf("submit %s: %w", result.RequestID, result.Err)
			} else {
				fmt.Printf("Accepted %s as %s\n", result.RequestID, result.TaskID)
			}
		case deliveryID, ok := <-recorded:
			if !ok {
				recorded = nil
			} else {
				fmt.Println("Recorded", deliveryID)
			}
		case <-channels.Done:
			if errors.Is(channels.Err(), context.Canceled) {
				return nil
			}
			return channels.Err()
		}
	}
}
