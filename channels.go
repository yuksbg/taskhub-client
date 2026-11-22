package taskhub

import (
	"context"
	"fmt"
	"sync"
)

// Default is the package's channel client. It owns at most one active session.
var Default = &channelClient{}

type channelClient struct {
	mu      sync.Mutex
	running bool
}

// Config defines the singleton connection and the number of concurrent task deliveries.
type Config struct {
	URL         string
	Token       string
	Queue       string
	Concurrency int
}

// Delivery is a task with a context that expires at its execution deadline.
// Report one Outcome per delivery, using its DeliveryID.
type Delivery struct {
	Task
	Context context.Context
}

// Outcome completes a delivery with a result or fails it with an error safe to store in TaskHub.
type Outcome struct {
	DeliveryID string
	Result     map[string]any
	Err        error
}

// SubmissionResult reports acceptance or rejection of one submission.
type SubmissionResult struct {
	RequestID string
	TaskID    string
	Err       error
}

// Channels carries tasks and commands on a single WebSocket.
// Use Done when sending commands so shutdown cannot leave a sender blocked.
type Channels struct {
	Submit    chan<- Submission
	Submitted <-chan SubmissionResult
	Tasks     <-chan Delivery
	Outcomes  chan<- Outcome
	Recorded  <-chan string
	Done      <-chan struct{}
	ctx       context.Context
}

// Err returns the reason the connection ended. It returns nil while the connection is active.
func (c *Channels) Err() error {
	return context.Cause(c.ctx)
}

// Start subscribes a combined credential to one queue over one WebSocket.
// Cancel ctx to close the session and make Default available for another Start.
func (client *channelClient) Start(ctx context.Context, cfg Config) (*Channels, error) {
	if cfg.Queue == "" || cfg.Concurrency < 1 || cfg.Concurrency > 32 {
		return nil, fmt.Errorf("a queue and concurrency from 1 to 32 are required")
	}
	connection, err := NewClient(cfg.URL, cfg.Token)
	if err != nil {
		return nil, err
	}
	client.mu.Lock()
	if client.running {
		client.mu.Unlock()
		return nil, fmt.Errorf("TaskHub channel client is already running")
	}
	client.running = true
	client.mu.Unlock()

	session, err := connection.Open(ctx)
	if err == nil {
		err = session.subscribe(ctx, cfg.Queue, cfg.Concurrency)
	}
	if err != nil {
		if session != nil {
			session.Close()
		}
		client.mu.Lock()
		client.running = false
		client.mu.Unlock()
		return nil, err
	}

	submit := make(chan Submission)
	submitted := make(chan SubmissionResult, 1)
	tasks := make(chan Delivery, cfg.Concurrency)
	outcomes := make(chan Outcome)
	recorded := make(chan string, cfg.Concurrency)
	done := make(chan struct{})

	submitDone := make(chan struct{})
	go func() {
		defer close(submitDone)
		defer close(submitted)
		for {
			select {
			case <-session.ctx.Done():
				return
			case request, ok := <-submit:
				if !ok {
					session.end(context.Canceled)
					return
				}
				id, err := session.Submit(session.ctx, request)
				select {
				case submitted <- SubmissionResult{RequestID: request.RequestID, TaskID: id, Err: err}:
				case <-session.ctx.Done():
					return
				}
			}
		}
	}()
	go func() {
		err := session.runChannels(cfg.Concurrency, tasks, outcomes, recorded)
		session.end(err)
		session.Close()
		close(tasks)
		close(recorded)
		<-submitDone
		client.mu.Lock()
		client.running = false
		close(done)
		client.mu.Unlock()
	}()
	return &Channels{
		Submit: submit, Submitted: submitted, Tasks: tasks, Outcomes: outcomes,
		Recorded: recorded, Done: done, ctx: session.ctx,
	}, nil
}

func (s *Session) runChannels(concurrency int, tasks chan<- Delivery, outcomes <-chan Outcome, recorded chan<- string) error {
	active := map[string]context.CancelFunc{}
	defer func() {
		for _, cancel := range active {
			cancel()
		}
	}()
	for {
		select {
		case <-s.ctx.Done():
			return context.Cause(s.ctx)
		case message := <-s.tasks:
			_, duplicate := active[message.DeliveryID]
			if !message.validTask() || duplicate || len(active) >= concurrency {
				return fmt.Errorf("unexpected task delivery")
			}
			ctx, cancel := context.WithDeadline(s.ctx, message.deadline())
			active[message.DeliveryID] = cancel
			delivery := Delivery{Task: Task{
				ID: message.TaskID, DeliveryID: message.DeliveryID, Payload: message.Payload, Tags: message.Tags,
			}, Context: ctx}
			select {
			case tasks <- delivery:
			case <-s.ctx.Done():
				return context.Cause(s.ctx)
			}
		case outcome, ok := <-outcomes:
			if !ok {
				return context.Canceled
			}
			cancel, exists := active[outcome.DeliveryID]
			if !exists {
				return fmt.Errorf("outcome references an inactive delivery")
			}
			if err := s.report(s.ctx, outcome.DeliveryID, outcome.Result, outcome.Err); err != nil {
				return err
			}
			cancel()
			delete(active, outcome.DeliveryID)
			select {
			case recorded <- outcome.DeliveryID:
			case <-s.ctx.Done():
				return context.Cause(s.ctx)
			}
		}
	}
}
