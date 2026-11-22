package taskhub

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Session shares one TaskHub socket between a worker and submissions.
type Session struct {
	conn        *websocket.Conn
	ctx         context.Context
	end         context.CancelCauseFunc
	readerDone  chan struct{}
	submitSlot  chan struct{}
	submissions chan reply
	deliveries  chan reply
	tasks       chan reply
	writeMu     sync.Mutex
	working     atomic.Bool
}

// Open connects with this client's credential. Use a combined credential for
// both Submit and Work. Cancel ctx or call Close to release the connection.
func (c *Client) Open(ctx context.Context) (*Session, error) {
	conn, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	sessionCtx, end := context.WithCancelCause(ctx)
	s := &Session{
		conn:        conn,
		ctx:         sessionCtx,
		end:         end,
		readerDone:  make(chan struct{}),
		submitSlot:  make(chan struct{}, 1),
		submissions: make(chan reply, 1),
		deliveries:  make(chan reply, 2),
		tasks:       make(chan reply, 32),
	}
	context.AfterFunc(sessionCtx, func() { conn.Close() })
	ping := conn.PingHandler()
	conn.SetPingHandler(func(data string) error {
		if err := conn.SetReadDeadline(time.Now().Add(60 * time.Second)); err != nil {
			return err
		}
		return ping(data)
	})
	go s.read()
	return s, nil
}

// Close ends the session and waits for its socket reader to stop.
func (s *Session) Close() {
	s.end(context.Canceled)
	s.conn.Close()
	<-s.readerDone
}

// Submit uses the open socket. A nonempty RequestID lets TaskHub correlate
// the reply even while the same socket is receiving deliveries.
func (s *Session) Submit(ctx context.Context, submission Submission) (string, error) {
	if submission.RequestID == "" {
		return "", fmt.Errorf("a request ID is required for session submissions")
	}
	command, err := submission.command()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	select {
	case s.submitSlot <- struct{}{}:
		defer func() { <-s.submitSlot }()
	case <-ctx.Done():
		return "", ctx.Err()
	case <-s.ctx.Done():
		return "", context.Cause(s.ctx)
	}
	if err := s.write(ctx, command); err != nil {
		return "", fmt.Errorf("send task submission: %w", err)
	}
	message, err := s.receive(ctx, s.submissions)
	if err != nil {
		// With an uncertain reply, close to prevent the next call from reading it.
		// A correlated API rejection leaves the session usable.
		if message.RequestID != submission.RequestID {
			s.end(err)
		}
		return "", fmt.Errorf("confirm task submission: %w", err)
	}
	if message.Type != "accepted" || message.TaskID == "" || message.RequestID != submission.RequestID {
		err := fmt.Errorf("unexpected task submission response")
		s.end(err)
		return "", err
	}
	return message.TaskID, nil
}

func (s *Session) write(ctx context.Context, command map[string]any) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := context.Cause(s.ctx); err != nil {
		return err
	}
	if err := writeCommand(s.conn, command); err != nil {
		s.end(err)
		return err
	}
	return nil
}

func (s *Session) read() {
	defer close(s.readerDone)
	for {
		if err := s.conn.SetReadDeadline(time.Now().Add(60 * time.Second)); err != nil {
			s.end(err)
			return
		}
		message, err := readReply(s.conn)
		if err != nil {
			s.end(err)
			return
		}
		target := s.deliveries
		switch {
		case message.Type == "task":
			message.receivedAt = time.Now()
			target = s.tasks
		case message.RequestID != "":
			target = s.submissions
		case message.Type == "error" && message.DeliveryID == "" && message.Code == "invalid_message":
			// Malformed commands can be rejected before the server extracts their IDs.
			s.end(message.apiError())
			return
		}
		select {
		case target <- message:
		default:
			s.end(fmt.Errorf("unexpected TaskHub replies"))
			return
		}
	}
}

func (s *Session) receive(ctx context.Context, messages <-chan reply) (reply, error) {
	select {
	case message := <-messages:
		return message, message.apiError()
	case <-ctx.Done():
		return reply{}, ctx.Err()
	case <-s.ctx.Done():
		// A final reply can arrive immediately before the peer disconnects.
		select {
		case message := <-messages:
			return message, message.apiError()
		default:
			return reply{}, context.Cause(s.ctx)
		}
	}
}
