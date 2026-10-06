package natsclient

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is everything a connection needs. Start from [DefaultConfig] and
// override; a zero Config is invalid (no URL, no credential).
//
// There is deliberately no switch that turns verification off and no free-form
// option string: a parameter handed through a string can be dropped on the way
// to the driver, and a dropped CA turns a verified connection into one that
// does not verify.
type Config struct {
	// URL is the broker, one nats:// or tls:// URL. The server certificate
	// must carry its host name.
	URL string

	// CAFile is the server CA. When set the connection is TLS and trusts this
	// file and nothing else (not the system store). A tls:// URL requires it.
	CAFile string
	// CertFile and KeyFile are the workload certificate and key (a SPIFFE
	// identity the broker maps to a user), both or neither. Read again for
	// every new connection.
	CertFile string
	KeyFile  string
	// TokenFile holds the projected ServiceAccount token the callout accepts as
	// the NATS auth token. Read again for every new connection.
	TokenFile string

	// Name is the connection name shown by the broker. Default: the program name.
	Name string

	ConnectTimeout time.Duration
	ReconnectWait  time.Duration
	PingInterval   time.Duration
	DrainTimeout   time.Duration

	// Connect bounds the retry of the first connection (a startup race with
	// the broker). After the first connection the driver reconnects without
	// limit.
	Connect RetryPolicy
}

// DefaultConfig returns the contract's defaults.
func DefaultConfig() Config {
	return Config{
		ConnectTimeout: 5 * time.Second,
		ReconnectWait:  time.Second,
		PingInterval:   30 * time.Second,
		DrainTimeout:   10 * time.Second,
		Connect:        DefaultRetryPolicy(),
	}
}

// Validate returns every problem it finds, not just the first.
func (c Config) Validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf("natsclient: "+format, a...)) }

	scheme := ""
	if c.URL == "" {
		add("URL is required (NATS_URL)")
	} else {
		if strings.ContainsAny(c.URL, ", ") {
			add("URL %q: exactly one URL is accepted (the broker's Service)", redactURL(c.URL))
		}
		u, err := url.Parse(c.URL)
		switch {
		case err != nil || u.Hostname() == "":
			add("URL is not a nats:// or tls:// URL with a host")
		case u.Scheme != "nats" && u.Scheme != "tls":
			add("URL scheme %q is refused; only nats:// and tls:// are accepted", u.Scheme)
		default:
			scheme = u.Scheme
		}
		if err == nil && u.User != nil {
			add("credentials in the URL are refused; use the token file or the certificate")
		}
	}
	if scheme == "tls" && c.CAFile == "" {
		add("a tls:// URL needs the server CA file (NATS_CA_FILE): verification has nothing to verify against without it")
	}
	if (c.CertFile == "") != (c.KeyFile == "") {
		add("client certificate and key go together")
	}
	cert := c.CertFile != "" || c.KeyFile != ""
	switch {
	case cert && c.TokenFile != "":
		add("a certificate and a token file are two identities; give one")
	case !cert && c.TokenFile == "":
		add("a credential is required: a certificate and key (NATS_CERT_FILE, NATS_KEY_FILE) or a token file (NATS_TOKEN_FILE)")
	}
	if c.CertFile != "" && c.KeyFile != "" && c.CAFile == "" {
		add("a client certificate needs the server CA file: it is only ever sent over a verified connection")
	}
	if c.ConnectTimeout <= 0 || c.ReconnectWait <= 0 || c.PingInterval <= 0 || c.DrainTimeout <= 0 {
		add("ConnectTimeout, ReconnectWait, PingInterval and DrainTimeout must be positive")
	}
	if err := c.Connect.validate(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (c Config) tls() bool { return c.CAFile != "" }

// String shows no credential, only which files are in use.
func (c Config) String() string {
	return fmt.Sprintf("natsclient.Config{url=%s ca=%s cert=%s key=%s token_file=%s name=%s}",
		redactURL(c.URL), c.CAFile, c.CertFile, c.KeyFile, c.TokenFile, c.Name)
}

// GoString keeps %#v as quiet as %v.
func (c Config) GoString() string { return c.String() }

// LogValue is the slog form.
func (c Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("url", redactURL(c.URL)),
		slog.Bool("tls", c.tls()),
		slog.Bool("certificate", c.CertFile != ""),
		slog.Bool("token", c.TokenFile != ""),
		slog.String("name", c.Name),
	)
}

func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw
	}
	u.User = nil
	return u.String()
}

// FromEnv builds a Config from the contract's environment, starting from
// [DefaultConfig]. Pass os.Getenv, or a map lookup in tests. The result is
// validated.
func FromEnv(getenv func(string) string) (Config, error) {
	c := DefaultConfig()
	var errs []error
	str := func(name string, dst *string) {
		if v := getenv(name); v != "" {
			*dst = v
		}
	}
	dur := func(name string, dst *time.Duration) {
		v := getenv(name)
		if v == "" {
			return
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("natsclient: %s=%q: %w", name, v, err))
			return
		}
		*dst = d
	}

	str("NATS_URL", &c.URL)
	str("NATS_CA_FILE", &c.CAFile)
	str("NATS_CERT_FILE", &c.CertFile)
	str("NATS_KEY_FILE", &c.KeyFile)
	str("NATS_TOKEN_FILE", &c.TokenFile)
	str("NATS_CLIENT_NAME", &c.Name)
	dur("NATS_CLIENT_CONNECT_TIMEOUT", &c.ConnectTimeout)
	dur("NATS_CLIENT_RECONNECT_WAIT", &c.ReconnectWait)
	dur("NATS_CLIENT_PING_INTERVAL", &c.PingInterval)
	dur("NATS_CLIENT_DRAIN_TIMEOUT", &c.DrainTimeout)
	if v := getenv("NATS_CLIENT_RETRY_ATTEMPTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("natsclient: NATS_CLIENT_RETRY_ATTEMPTS=%q: %w", v, err))
		} else {
			c.Connect.Attempts = n
		}
	}
	dur("NATS_CLIENT_RETRY_MAX_DELAY", &c.Connect.MaxDelay)
	dur("NATS_CLIENT_RETRY_BUDGET", &c.Connect.Budget)

	if c.Name == "" {
		c.Name = programName()
	}
	if len(errs) > 0 {
		return c, errors.Join(errs...)
	}
	return c, c.Validate()
}

func programName() string {
	if len(os.Args) == 0 {
		return "go"
	}
	name := os.Args[0]
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		name = name[i+1:]
	}
	return name
}
