package natsclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

// RetryPolicy bounds the retry of connection-class failures: exponential
// backoff with full jitter, stopping at Attempts or Budget, whichever is first.
type RetryPolicy struct {
	Attempts     int           // total tries, including the first
	InitialDelay time.Duration // ceiling of the first sleep
	MaxDelay     time.Duration // ceiling of any sleep
	Budget       time.Duration // total time that may be spent waiting
}

// DefaultRetryPolicy is 5 tries, 200ms doubling to 5s, 30s of waiting.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{Attempts: 5, InitialDelay: 200 * time.Millisecond, MaxDelay: 5 * time.Second, Budget: 30 * time.Second}
}

func (p RetryPolicy) validate() error {
	if p.Attempts < 1 || p.InitialDelay <= 0 || p.MaxDelay < p.InitialDelay || p.Budget <= 0 {
		return fmt.Errorf("natsclient: retry policy %+v: Attempts>=1, 0<InitialDelay<=MaxDelay, Budget>0", p)
	}
	return nil
}

// Retry runs fn, repeating it while it fails with a retryable error (see
// [IsRetryable]). fn must be safe to run again.
func Retry(ctx context.Context, p RetryPolicy, fn func(context.Context) error) error {
	if err := p.validate(); err != nil {
		return err
	}
	start := time.Now()
	var err error
	for attempt := 1; ; attempt++ {
		if err = fn(ctx); err == nil {
			return nil
		}
		if ctx.Err() != nil || !IsRetryable(err) || attempt >= p.Attempts {
			return err
		}
		delay := backoff(p, attempt)
		if time.Since(start)+delay > p.Budget {
			return err
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return err
		case <-t.C:
		}
	}
}

func backoff(p RetryPolicy, attempt int) time.Duration {
	ceil := p.InitialDelay
	for i := 1; i < attempt && ceil < p.MaxDelay; i++ {
		ceil *= 2
	}
	ceil = min(ceil, p.MaxDelay)
	return time.Duration(rand.Int64N(int64(ceil) + 1))
}

// IsRetryable reports whether err means the broker was not reachable (down,
// restarting, not accepting yet), which a later try can fix.
//
// Never retryable: an authorization or authentication failure, a certificate
// verification failure (a second try cannot fix either), a permissions
// violation, and the caller's own cancellation or deadline.
func IsRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, nats.ErrAuthorization) || errors.Is(err, nats.ErrAuthExpired) ||
		errors.Is(err, nats.ErrAuthRevoked) ||
		errors.Is(err, nats.ErrPermissionViolation) {
		return false
	}
	var (
		unknown  x509.UnknownAuthorityError
		hostname x509.HostnameError
		invalid  x509.CertificateInvalidError
	)
	var verify *tls.CertificateVerificationError
	if errors.As(err, &unknown) || errors.As(err, &hostname) || errors.As(err, &invalid) || errors.As(err, &verify) {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "authorization violation") || strings.Contains(msg, "certificate") || strings.Contains(msg, "tls:") {
		return false
	}
	// The broker answered the handshake with a PING while it was still asking
	// its callout: it is up and busy, not refusing.
	if strings.Contains(msg, "expected 'pong'") {
		return true
	}
	if errors.Is(err, nats.ErrNoServers) || errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrConnectionClosed) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}
