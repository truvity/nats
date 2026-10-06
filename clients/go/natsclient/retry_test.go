package natsclient

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

func TestIsRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"no servers", nats.ErrNoServers, true},
		{"timeout", nats.ErrTimeout, true},
		{"EOF", io.EOF, true},
		{"refused", fmt.Errorf("dial: %w", syscall.ECONNREFUSED), true},
		{"handshake raced the callout", errors.New("nats: expected 'PONG', got 'PING'"), true},
		{"authorization", nats.ErrAuthorization, false},
		{"authorization text", errors.New("nats: Authorization Violation"), false},
		{"expired", nats.ErrAuthExpired, false},
		{"permission", nats.ErrPermissionViolation, false},
		{"unknown authority", x509.UnknownAuthorityError{}, false},
		{"hostname", x509.HostnameError{Host: "x"}, false},
		{"canceled", context.Canceled, false},
		{"deadline", context.DeadlineExceeded, false},
		{"plain", errors.New("boom"), false},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, IsRetryable(c.err), c.name)
	}
}

func fast() RetryPolicy {
	return RetryPolicy{Attempts: 4, InitialDelay: time.Millisecond, MaxDelay: 4 * time.Millisecond, Budget: time.Second}
}

func TestRetryStopsAtAttempts(t *testing.T) {
	n := 0
	err := Retry(context.Background(), fast(), func(context.Context) error { n++; return io.EOF })
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 4, n)
}

func TestRetryDoesNotRetryAPermanentError(t *testing.T) {
	n := 0
	err := Retry(context.Background(), fast(), func(context.Context) error { n++; return nats.ErrAuthorization })
	assert.ErrorIs(t, err, nats.ErrAuthorization)
	assert.Equal(t, 1, n)
}

func TestRetrySucceedsAfterTransientFailures(t *testing.T) {
	n := 0
	err := Retry(context.Background(), fast(), func(context.Context) error {
		n++
		if n < 3 {
			return nats.ErrNoServers
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, n)
}

func TestRetryHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	n := 0
	err := Retry(ctx, fast(), func(context.Context) error { n++; cancel(); return io.EOF })
	assert.Error(t, err)
	assert.Equal(t, 1, n)
}

func TestRetryRejectsABadPolicy(t *testing.T) {
	assert.Error(t, Retry(context.Background(), RetryPolicy{}, func(context.Context) error { return nil }))
}
