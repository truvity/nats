package natsclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The JetStream defaults. A stream or consumer built by this package differs
// from the broker's own only where these say.
const (
	DefaultStreamReplicas  = 1
	DefaultStreamMaxAge    = 7 * 24 * time.Hour
	DefaultDuplicateWindow = 2 * time.Minute
	DefaultAckWait         = 30 * time.Second
	DefaultMaxDeliver      = 5
	DefaultMaxAckPending   = 1000
	publishRetryAttempts   = 3
)

// StreamSpec names a stream; zero fields take the defaults: file storage,
// limits retention, discard old, [DefaultStreamReplicas] replicas,
// [DefaultStreamMaxAge], a [DefaultDuplicateWindow] de-duplication window.
// Run three replicas against a three-node broker by setting Replicas.
type StreamSpec struct {
	Name        string
	Subjects    []string
	Description string
	Replicas    int
	MaxAge      time.Duration
}

// EnsureStream creates the stream or brings an existing one to the spec.
func (c *Client) EnsureStream(ctx context.Context, s StreamSpec) (jetstream.Stream, error) {
	if s.Name == "" || len(s.Subjects) == 0 {
		return nil, errors.New("natsclient: a stream needs a name and subjects")
	}
	for _, sub := range s.Subjects {
		if err := validStreamSubject(sub); err != nil {
			return nil, err
		}
	}
	if s.Replicas == 0 {
		s.Replicas = DefaultStreamReplicas
	}
	if s.MaxAge == 0 {
		s.MaxAge = DefaultStreamMaxAge
	}
	st, err := c.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        s.Name,
		Description: s.Description,
		Subjects:    s.Subjects,
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		Discard:     jetstream.DiscardOld,
		Replicas:    s.Replicas,
		MaxAge:      s.MaxAge,
		Duplicates:  DefaultDuplicateWindow,
	})
	if err != nil {
		return nil, fmt.Errorf("natsclient: ensure stream %s: %w", s.Name, err)
	}
	return st, nil
}

// ConsumerSpec names a durable consumer; zero fields take the defaults:
// explicit acknowledgement, [DefaultAckWait], [DefaultMaxDeliver] deliveries,
// [DefaultMaxAckPending] unacknowledged, deliver all.
type ConsumerSpec struct {
	Stream        string
	Durable       string
	FilterSubject string
	AckWait       time.Duration
	MaxDeliver    int
	MaxAckPending int
}

// EnsureConsumer creates the durable consumer or brings an existing one to the
// spec.
func (c *Client) EnsureConsumer(ctx context.Context, s ConsumerSpec) (jetstream.Consumer, error) {
	if s.Stream == "" || s.Durable == "" {
		return nil, errors.New("natsclient: a consumer needs a stream and a durable name")
	}
	if s.AckWait == 0 {
		s.AckWait = DefaultAckWait
	}
	if s.MaxDeliver == 0 {
		s.MaxDeliver = DefaultMaxDeliver
	}
	if s.MaxAckPending == 0 {
		s.MaxAckPending = DefaultMaxAckPending
	}
	cons, err := c.js.CreateOrUpdateConsumer(ctx, s.Stream, jetstream.ConsumerConfig{
		Durable:       s.Durable,
		FilterSubject: s.FilterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       s.AckWait,
		MaxDeliver:    s.MaxDeliver,
		MaxAckPending: s.MaxAckPending,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("natsclient: ensure consumer %s/%s: %w", s.Stream, s.Durable, err)
	}
	return cons, nil
}

// PublishOptions carry what a publish adds to the subject and payload.
type PublishOptions struct {
	// MsgID makes a retried publish idempotent within the stream's
	// de-duplication window.
	MsgID string
	// Traceparent and Tracestate are written as the trace headers.
	Traceparent string
	Tracestate  string
	// Headers are extra headers, sent as given.
	Headers nats.Header
}

// Publish sends data to a JetStream subject and waits for the stream's
// acknowledgement, retrying a missing responder (a stream still being created
// or a leader election) a few times.
func (c *Client) Publish(ctx context.Context, subject string, data []byte, o PublishOptions) (*jetstream.PubAck, error) {
	if err := ValidPublishSubject(subject); err != nil {
		return nil, err
	}
	msg := &nats.Msg{Subject: subject, Data: data, Header: nats.Header{}}
	for k, v := range o.Headers {
		msg.Header[k] = v
	}
	InjectTrace(msg.Header, o.Traceparent, o.Tracestate)
	opts := []jetstream.PublishOpt{jetstream.WithRetryAttempts(publishRetryAttempts)}
	if o.MsgID != "" {
		opts = append(opts, jetstream.WithMsgID(o.MsgID))
	}
	return c.js.PublishMsg(ctx, msg, opts...)
}

func validStreamSubject(s string) error {
	// A stream may bind a wildcard subject; only the reserved space is refused.
	if s == "" || s[0] == '$' {
		return fmt.Errorf("natsclient: stream subject %q is empty or in the broker's reserved space", s)
	}
	return nil
}
