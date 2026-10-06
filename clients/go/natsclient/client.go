package natsclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// healthTimeout bounds [Client.Health].
const healthTimeout = 2 * time.Second

// Client is one connection to the broker. It reconnects without limit; the
// credential files are read again for every reconnect.
type Client struct {
	cfg    Config
	nc     *nats.Conn
	js     jetstream.JetStream
	logger *slog.Logger

	closeOnce sync.Once
	closed    chan struct{}
}

// Option adjusts [Connect].
type Option func(*Client)

// WithLogger reports disconnects, reconnects and asynchronous errors. Nothing
// it is given contains a credential.
func WithLogger(l *slog.Logger) Option { return func(c *Client) { c.logger = l } }

// Connect validates cfg and opens the connection. The first connection is
// retried (see [Config.Connect]) while the broker is not reachable; a failed
// verification or authorization is returned at once.
func Connect(ctx context.Context, cfg Config, opts ...Option) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	c := &Client{cfg: cfg, logger: slog.New(slog.DiscardHandler), closed: make(chan struct{})}
	for _, o := range opts {
		o(c)
	}
	natsOpts, err := c.options()
	if err != nil {
		return nil, err
	}
	err = Retry(ctx, cfg.Connect, func(context.Context) error {
		nc, cerr := nats.Connect(cfg.URL, natsOpts...)
		if cerr != nil {
			return cerr
		}
		c.nc = nc
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("natsclient: connect %s: %w", redactURL(cfg.URL), err)
	}
	c.js, err = jetstream.New(c.nc)
	if err != nil {
		c.nc.Close()
		return nil, fmt.Errorf("natsclient: jetstream: %w", err)
	}
	return c, nil
}

func (c *Client) options() ([]nats.Option, error) {
	cfg := c.cfg
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("natsclient: URL: %w", err)
	}
	opts := []nats.Option{
		nats.Name(cfg.Name),
		nats.Timeout(cfg.ConnectTimeout),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.ReconnectJitter(cfg.ReconnectWait/2, cfg.ReconnectWait),
		nats.PingInterval(cfg.PingInterval),
		nats.DrainTimeout(cfg.DrainTimeout),
		// A broker restart can answer a reconnect before its callout is back, and a
		// rotated token is only read on the next attempt: keep trying instead of
		// giving up on the second identical authorization error.
		nats.IgnoreAuthErrorAbort(),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			c.logger.Warn("nats disconnected", slog.Any("error", err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			c.logger.Info("nats reconnected", slog.String("server", nc.ConnectedServerName()))
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			c.logger.Error("nats error", slog.Any("error", err))
		}),
		nats.ClosedHandler(func(*nats.Conn) { c.closeOnce.Do(func() { close(c.closed) }) }),
	}
	if cfg.tls() {
		opts = append(opts, nats.Secure(c.tlsConfig(u.Hostname())))
	}
	if cfg.TokenFile != "" {
		opts = append(opts, nats.TokenHandler(func() string {
			b, err := os.ReadFile(cfg.TokenFile)
			if err != nil {
				c.logger.Error("nats token file unreadable", slog.String("file", cfg.TokenFile), slog.Any("error", err))
				return ""
			}
			return strings.TrimSpace(string(b))
		}))
	}
	return opts, nil
}

// tlsConfig is verify-full against the CA file, read for every handshake. The
// standard library cannot take a CA pool that changes, so the built-in
// verification is replaced by VerifyConnection, which checks the chain
// against the file's current content and the name against the URL's host.
func (c *Client) tlsConfig(host string) *tls.Config {
	cfg := c.cfg
	t := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: host,
		// Verification is done in VerifyConnection below; it is never skipped.
		InsecureSkipVerify: true, //nolint:gosec // see VerifyConnection
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("natsclient: the server presented no certificate")
			}
			pem, err := os.ReadFile(cfg.CAFile)
			if err != nil {
				return fmt.Errorf("natsclient: read CA file: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return errors.New("natsclient: the CA file holds no certificate")
			}
			inter := x509.NewCertPool()
			for _, ic := range cs.PeerCertificates[1:] {
				inter.AddCert(ic)
			}
			_, err = cs.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: pool, Intermediates: inter, DNSName: host})
			return err
		},
	}
	if cfg.CertFile != "" {
		t.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("natsclient: load client certificate: %w", err)
			}
			return &cert, nil
		}
	}
	return t
}

// Conn is the driver's connection, for what the adapter does not wrap
// (subscriptions, requests). Do not Close it; use [Client.Close].
func (c *Client) Conn() *nats.Conn { return c.nc }

// JetStream is the driver's JetStream context.
func (c *Client) JetStream() jetstream.JetStream { return c.js }

// Health is a round trip to the broker through the real path (TLS,
// authentication), bounded to 2s. It suits a readiness probe.
func (c *Client) Health(ctx context.Context) error {
	if st := c.nc.Status(); st != nats.CONNECTED {
		return fmt.Errorf("natsclient: not connected (%s)", st)
	}
	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	return c.nc.FlushTimeout(time.Until(deadline))
}

// Close drains the connection: subscriptions stop taking new messages, the
// ones already delivered are handled, pending publishes are flushed, then the
// connection closes. It returns when that is done, the drain timeout passes
// or ctx ends.
func (c *Client) Close(ctx context.Context) error {
	if c.nc.IsClosed() {
		return nil
	}
	if err := c.nc.Drain(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
		return fmt.Errorf("natsclient: drain: %w", err)
	}
	select {
	case <-c.closed:
		return nil
	case <-ctx.Done():
		c.nc.Close()
		return ctx.Err()
	}
}
