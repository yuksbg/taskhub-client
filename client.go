// Package taskhub submits and processes tasks through the TaskHub v1 WebSocket API.
package taskhub

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vmihailenco/msgpack/v5"
)

const maxMessageBytes = 512 << 10

// Client holds the endpoint and a scoped TaskHub credential.
type Client struct {
	endpoint string
	token    string
}

// NewClient takes the full ws:// or wss:// endpoint and a scoped TaskHub token.
func NewClient(endpoint, token string) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return nil, fmt.Errorf("use a full ws:// or wss:// TaskHub URL without embedded credentials")
	}
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return nil, fmt.Errorf("a valid TaskHub token is required")
	}
	return &Client{endpoint: endpoint, token: token}, nil
}

// Submission describes one task. Reuse RequestID and identical input after uncertain acceptance.
type Submission struct {
	Queue       string
	RequestID   string
	Payload     map[string]any
	Tags        []string
	ScheduledAt time.Time
}

// Error is a rejection reported by TaskHub. TaskID can be set when dispatch is uncertain.
type Error struct {
	Code    string
	Message string
	TaskID  string
}

func (e *Error) Error() string {
	return fmt.Sprintf("TaskHub %s: %s", e.Code, e.Message)
}

// Submit returns the accepted task ID. A transport error can leave acceptance uncertain.
func (c *Client) Submit(ctx context.Context, submission Submission) (string, error) {
	command, err := submission.command()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	conn, err := c.dial(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	if err := writeCommand(conn, command); err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", fmt.Errorf("send task submission: %w", err)
	}
	reply, err := readReply(conn)
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", fmt.Errorf("confirm task submission: %w", err)
	}
	if err := reply.apiError(); err != nil {
		return "", err
	}
	if reply.Type != "accepted" || reply.TaskID == "" || reply.RequestID != submission.RequestID {
		return "", fmt.Errorf("unexpected task submission response")
	}
	return reply.TaskID, nil
}

func (submission Submission) command() (map[string]any, error) {
	if submission.Queue == "" || submission.Payload == nil {
		return nil, fmt.Errorf("a queue and a payload map are required")
	}
	command := map[string]any{"type": "submit", "queue": submission.Queue, "payload": submission.Payload}
	if submission.RequestID != "" {
		command["request_id"] = submission.RequestID
	}
	if len(submission.Tags) > 0 {
		command["tags"] = submission.Tags
	}
	if !submission.ScheduledAt.IsZero() {
		command["scheduled_at"] = submission.ScheduledAt.UTC().Format(time.RFC3339Nano)
	}
	return command, nil
}

type reply struct {
	receivedAt     time.Time
	Type           string         `msgpack:"type"`
	TaskID         string         `msgpack:"task_id"`
	RequestID      string         `msgpack:"request_id"`
	DeliveryID     string         `msgpack:"delivery_id"`
	Queue          string         `msgpack:"queue"`
	Concurrency    int            `msgpack:"concurrency"`
	TimeoutSeconds int            `msgpack:"timeout_seconds"`
	Payload        map[string]any `msgpack:"payload"`
	Tags           []string       `msgpack:"tags"`
	Code           string         `msgpack:"code"`
	Message        string         `msgpack:"message"`
}

func (c *Client) dial(ctx context.Context) (*websocket.Conn, error) {
	dialer := websocket.Dialer{Proxy: http.ProxyFromEnvironment, HandshakeTimeout: 10 * time.Second}
	conn, response, err := dialer.DialContext(ctx, c.endpoint, http.Header{"Authorization": {"Bearer " + c.token}})
	if err != nil {
		if response != nil {
			response.Body.Close()
			return nil, fmt.Errorf("TaskHub upgrade rejected with HTTP %d", response.StatusCode)
		}
		return nil, fmt.Errorf("connect to TaskHub: %w", err)
	}
	conn.SetReadLimit(maxMessageBytes)
	return conn, nil
}

func writeCommand(conn *websocket.Conn, command map[string]any) error {
	data, err := msgpack.Marshal(command)
	if err != nil {
		return err
	}
	if len(data) > maxMessageBytes {
		return fmt.Errorf("TaskHub command exceeds 512 KiB")
	}
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	return conn.WriteMessage(websocket.BinaryMessage, data)
}

func readReply(conn *websocket.Conn) (reply, error) {
	message := reply{}
	kind, data, err := conn.ReadMessage()
	if err != nil {
		return message, err
	}
	if kind != websocket.BinaryMessage {
		return message, fmt.Errorf("expected a binary TaskHub response")
	}
	if err := msgpack.Unmarshal(data, &message); err != nil {
		return message, fmt.Errorf("decode TaskHub response: %w", err)
	}
	return message, nil
}

func (message reply) apiError() error {
	if message.Type == "error" {
		return &Error{Code: message.Code, Message: message.Message, TaskID: message.TaskID}
	}
	return nil
}
