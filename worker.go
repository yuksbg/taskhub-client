package taskhub

import (
	"context"
	"fmt"
	"time"
	"unicode/utf8"
)

// Task describes a single delivery attempt.
type Task struct {
	ID         string
	DeliveryID string
	Payload    map[string]any
	Tags       []string
}

// Work processes one task at a time until ctx is canceled or the connection fails.
// The handler should honor cancellation and return error messages safe to store in TaskHub.
func (c *Client) Work(ctx context.Context, queue string, handle func(context.Context, Task) (map[string]any, error)) error {
	session, err := c.Open(ctx)
	if err != nil {
		return err
	}
	defer session.Close()
	return session.Work(ctx, queue, handle)
}

// Work consumes one queue on this session. It may run concurrently with Submit.
// Work closes the session when it returns, releasing the worker's reservation.
func (s *Session) Work(ctx context.Context, queue string, handle func(context.Context, Task) (map[string]any, error)) error {
	if queue == "" || handle == nil {
		return fmt.Errorf("a queue and a task handler are required")
	}
	if !s.working.CompareAndSwap(false, true) {
		return fmt.Errorf("this session already has a worker")
	}
	defer s.Close()
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	if err := s.subscribe(ctx, queue, 1); err != nil {
		return err
	}
	for {
		message, err := s.receive(ctx, s.tasks)
		if err != nil {
			return fmt.Errorf("receive task: %w", err)
		}
		if !message.validTask() {
			return fmt.Errorf("unexpected task delivery")
		}
		if workCtx.Err() != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("worker connection lost before task %s started", message.TaskID)
		}
		task := Task{ID: message.TaskID, DeliveryID: message.DeliveryID, Payload: message.Payload, Tags: message.Tags}
		taskCtx, finish := context.WithDeadline(workCtx, message.deadline())
		result, taskErr := handle(taskCtx, task)
		if taskErr == nil {
			taskErr = taskCtx.Err()
		}
		finish()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if workCtx.Err() != nil {
			return fmt.Errorf("connection lost before reporting delivery %s", task.DeliveryID)
		}
		if err := s.report(ctx, task.DeliveryID, result, taskErr); err != nil {
			return err
		}
	}
}

func (s *Session) subscribe(ctx context.Context, queue string, concurrency int) error {
	if err := s.write(ctx, map[string]any{"type": "consume", "queue": queue, "concurrency": concurrency}); err != nil {
		return fmt.Errorf("subscribe to queue: %w", err)
	}
	wait, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ready, err := s.receive(wait, s.deliveries)
	if err != nil {
		return fmt.Errorf("confirm worker subscription: %w", err)
	}
	if ready.Type != "ready" || ready.Queue != queue || ready.Concurrency != concurrency {
		return fmt.Errorf("unexpected worker subscription response")
	}
	return nil
}

func (s *Session) report(ctx context.Context, deliveryID string, result map[string]any, taskErr error) error {
	command := map[string]any{"type": "complete", "delivery_id": deliveryID}
	if taskErr != nil {
		reason := taskErr.Error()
		if reason == "" || len(reason) > 2048 || !utf8.ValidString(reason) {
			reason = "Task handler failed."
		}
		command["type"] = "fail"
		command["error"] = reason
	} else {
		if result == nil {
			result = map[string]any{}
		}
		command["result"] = result
	}
	if err := s.write(ctx, command); err != nil {
		return fmt.Errorf("report delivery %s: %w", deliveryID, err)
	}
	wait, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	recorded, err := s.receive(wait, s.deliveries)
	if err != nil {
		return fmt.Errorf("confirm delivery %s: %w", deliveryID, err)
	}
	if recorded.Type != "recorded" || recorded.DeliveryID != deliveryID {
		return fmt.Errorf("unexpected outcome response for delivery %s", deliveryID)
	}
	return nil
}

func (message reply) validTask() bool {
	return message.Type == "task" && message.TaskID != "" && message.DeliveryID != "" && message.TimeoutSeconds >= 1 && message.TimeoutSeconds <= 86400 && message.Payload != nil
}

func (message reply) deadline() time.Time {
	return message.receivedAt.Add(time.Duration(message.TimeoutSeconds) * time.Second)
}
