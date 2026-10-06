package natsclient_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/truvity/nats/clients/go/natsclient"
	callout "github.com/truvity/nats/pkg/nats-auth-callout"
)

// The conformance suite (clients/conformance/cases.txt): each case is a subtest
// of TestConformance named exactly as the case, run against the broker that
// clients/conformance/nats-broker.sh starts. Without NATS_CLIENTS_URL it skips,
// unless NATS_CLIENTS=required, which turns a missing broker into a failure.
// clients/conformance/guard.sh fails unless every case ran and passed.

type env struct {
	url, tlsURL, tlsURLIP, monitor, dir, container, trust string
}

func loadEnv(t *testing.T) env {
	t.Helper()
	e := env{
		url: os.Getenv("NATS_CLIENTS_URL"), tlsURL: os.Getenv("NATS_CLIENTS_TLS_URL"),
		tlsURLIP: os.Getenv("NATS_CLIENTS_TLS_URL_IP"), monitor: os.Getenv("NATS_CLIENTS_MONITOR"),
		dir: os.Getenv("NATS_CLIENTS_DIR"), container: os.Getenv("NATS_CLIENTS_CONTAINER"),
		trust: os.Getenv("NATS_CLIENTS_TRUST_DOMAIN"),
	}
	if e.url == "" {
		if os.Getenv("NATS_CLIENTS") == "required" {
			t.Fatal("NATS_CLIENTS=required but NATS_CLIENTS_URL is not set: no broker")
		}
		t.Skip("NATS_CLIENTS_URL not set: no broker")
	}
	return e
}

func (e env) file(name string) string { return filepath.Join(e.dir, name) }

// work copies files into a per-test directory so a test can rotate them.
func (e env) work(t *testing.T, copies map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for dst, src := range copies {
		b, err := os.ReadFile(e.file(src))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, dst), b, 0o600))
	}
	return dir
}

func (e env) certConfig(t *testing.T, name, dir string, cert string) natsclient.Config {
	t.Helper()
	cfg := natsclient.DefaultConfig()
	cfg.URL = e.tlsURL
	cfg.CAFile = filepath.Join(dir, "ca.crt")
	cfg.CertFile = filepath.Join(dir, cert+".crt")
	cfg.KeyFile = filepath.Join(dir, cert+".key")
	cfg.Name = name
	cfg.ReconnectWait = 200 * time.Millisecond
	return cfg
}

func (e env) tokenConfig(name, tokenFile string) natsclient.Config {
	cfg := natsclient.DefaultConfig()
	cfg.URL = e.url
	cfg.TokenFile = tokenFile
	cfg.Name = name
	cfg.ReconnectWait = 200 * time.Millisecond
	return cfg
}

type connInfo struct {
	Name           string `json:"name"`
	Account        string `json:"account"`
	AuthorizedUser string `json:"authorized_user"`
}

// connz asks the broker's monitor who is connected, with the identity it
// authorized.
func (e env) connz(t *testing.T) []connInfo {
	t.Helper()
	resp, err := http.Get(e.monitor + "/connz?auth=true&state=open")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var out struct {
		Conns []connInfo `json:"connections"`
	}
	require.NoError(t, json.Unmarshal(b, &out), string(b))
	return out.Conns
}

func (e env) find(t *testing.T, name string) (connInfo, bool) {
	for _, c := range e.connz(t) {
		if c.Name == name {
			return c, true
		}
	}
	return connInfo{}, false
}

func (e env) waitConn(t *testing.T, name string, ok func(connInfo) bool) connInfo {
	t.Helper()
	var last connInfo
	require.Eventually(t, func() bool {
		c, found := e.find(t, name)
		last = c
		return found && ok(c)
	}, 30*time.Second, 100*time.Millisecond, "connection %q: last seen %+v", name, last)
	c, _ := e.find(t, name)
	return c
}

func (e env) restart(t *testing.T) {
	t.Helper()
	out, err := exec.Command("docker", "restart", "-t", "1", e.container).CombinedOutput()
	require.NoError(t, err, string(out))
	require.Eventually(t, func() bool {
		resp, err := http.Get(e.monitor + "/healthz")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode == http.StatusOK
	}, 30*time.Second, 100*time.Millisecond, "the broker came back")
	// Up is not ready: the callout reconnects a moment later, and until it does
	// the broker refuses every token. Wait until one is let in.
	require.Eventually(t, func() bool {
		nc, err := nats.Connect(e.url, nats.Token("token-shop-api"), nats.Timeout(2*time.Second))
		if err != nil {
			return false
		}
		nc.Close()
		return true
	}, 30*time.Second, 200*time.Millisecond, "the callout came back")
}

func connect(t *testing.T, cfg natsclient.Config) *natsclient.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := natsclient.Connect(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		_ = c.Close(cctx)
	})
	return c
}

func connectFails(t *testing.T, cfg natsclient.Config) error {
	t.Helper()
	cfg.Connect.Budget = 2 * time.Second
	cfg.Connect.Attempts = 3
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	c, err := natsclient.Connect(ctx, cfg)
	if err == nil {
		_ = c.Close(ctx)
		t.Fatal("the connection was accepted")
	}
	// A verification or authorization failure is not retried.
	assert.Less(t, time.Since(start), 5*time.Second)
	return err
}

func TestConformance(t *testing.T) {
	e := loadEnv(t)
	certs := map[string]string{"ca.crt": "ca.crt", "other-ca.crt": "other-ca.crt",
		"api.crt": "api.crt", "api.key": "api.key", "worker.crt": "worker.crt", "worker.key": "worker.key",
		"foreign.crt": "foreign.crt", "foreign.key": "foreign.key"}
	run := fmt.Sprintf("r%d", time.Now().UnixNano()) // subjects and streams are unique per run: a broker may be reused
	subj := func(s string) string { return "conformance." + run + "." + s }
	apiID := "spiffe://" + e.trust + "/ns/shop/sa/api"
	workerID := "spiffe://" + e.trust + "/ns/shop/sa/worker"

	t.Run("verify-full-connects", func(t *testing.T) {
		dir := e.work(t, certs)
		c := connect(t, e.certConfig(t, "verify-full-connects", dir, "api"))
		require.NoError(t, c.Health(context.Background()))
		got := e.waitConn(t, "verify-full-connects", func(connInfo) bool { return true })
		assert.Equal(t, apiID, got.AuthorizedUser)
		assert.Equal(t, "shop", got.Account)
	})

	t.Run("rejects-unknown-ca", func(t *testing.T) {
		dir := e.work(t, certs)
		cfg := e.certConfig(t, "rejects-unknown-ca", dir, "api")
		cfg.CAFile = filepath.Join(dir, "other-ca.crt")
		err := connectFails(t, cfg)
		assert.Contains(t, strings.ToLower(err.Error()), "certificate")
	})

	t.Run("rejects-hostname-mismatch", func(t *testing.T) {
		dir := e.work(t, certs)
		cfg := e.certConfig(t, "rejects-hostname-mismatch", dir, "api")
		cfg.URL = e.tlsURLIP // the server certificate carries only `localhost`
		err := connectFails(t, cfg)
		assert.Contains(t, strings.ToLower(err.Error()), "certificate")
	})

	t.Run("rejects-foreign-client-cert", func(t *testing.T) {
		dir := e.work(t, certs)
		_ = connectFails(t, e.certConfig(t, "rejects-foreign-client-cert", dir, "foreign"))
	})

	t.Run("refuses-unsafe-config", func(t *testing.T) {
		cfg := natsclient.DefaultConfig()
		cfg.URL = "tls://localhost:4222" // no CA
		cfg.CertFile = "/x.crt"          // no key
		cfg.TokenFile = "/token"         // and a second identity
		err := cfg.Validate()
		require.Error(t, err)
		// Every problem at once, not the first.
		for _, want := range []string{"server CA", "go together", "two identities"} {
			assert.Contains(t, err.Error(), want)
		}
		bad := natsclient.DefaultConfig()
		bad.URL = "http://localhost:4222"
		bad.TokenFile = "/token"
		assert.ErrorContains(t, bad.Validate(), "scheme")
		withCreds := natsclient.DefaultConfig()
		withCreds.URL = "nats://user:pw@localhost:4222"
		withCreds.TokenFile = "/token"
		err = withCreds.Validate()
		require.ErrorContains(t, err, "credentials in the URL")
		assert.NotContains(t, err.Error(), "pw")
		// A broker that does not offer TLS is not talked to in the clear when a CA was given.
		dir := e.work(t, certs)
		tokenFile := filepath.Join(dir, "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("token-shop-api"), 0o600))
		plain := e.tokenConfig("refuses-unsafe-config", tokenFile)
		plain.CAFile = filepath.Join(dir, "ca.crt")
		_ = connectFails(t, plain)
	})

	t.Run("token-connects", func(t *testing.T) {
		dir := e.work(t, nil)
		tokenFile := filepath.Join(dir, "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("token-shop-api\n"), 0o600))
		c := connect(t, e.tokenConfig("token-connects", tokenFile))
		require.NoError(t, c.Health(context.Background()))
		got := e.waitConn(t, "token-connects", func(connInfo) bool { return true })
		assert.Equal(t, "shop", got.Account)
	})

	t.Run("token-account-mapping", func(t *testing.T) {
		dir := e.work(t, nil)
		tokenFile := filepath.Join(dir, "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("token-other-app"), 0o600))
		connect(t, e.tokenConfig("token-account-mapping", tokenFile))
		got := e.waitConn(t, "token-account-mapping", func(connInfo) bool { return true })
		want, err := natsclient.AccountForNamespace("other", []string{"shop", "other"})
		require.NoError(t, err)
		assert.Equal(t, want, got.Account)
	})

	t.Run("rejects-unmapped-namespace", func(t *testing.T) {
		dir := e.work(t, nil)
		tokenFile := filepath.Join(dir, "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("token-kube-system"), 0o600))
		_ = connectFails(t, e.tokenConfig("rejects-unmapped-namespace", tokenFile))
		unknown := filepath.Join(dir, "unknown")
		require.NoError(t, os.WriteFile(unknown, []byte("not-a-token"), 0o600))
		_ = connectFails(t, e.tokenConfig("rejects-unknown-token", unknown))
	})

	t.Run("client-cert-rotation", func(t *testing.T) {
		dir := e.work(t, certs)
		cfg := e.certConfig(t, "client-cert-rotation", dir, "api")
		c := connect(t, cfg)
		assert.Equal(t, apiID, e.waitConn(t, "client-cert-rotation", func(connInfo) bool { return true }).AuthorizedUser)
		// The files change under the running process, as cert-manager renews them.
		for _, ext := range []string{"crt", "key"} {
			b, err := os.ReadFile(e.file("worker." + ext))
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(dir, "api."+ext), b, 0o600))
		}
		e.restart(t)
		got := e.waitConn(t, "client-cert-rotation", func(ci connInfo) bool { return ci.AuthorizedUser == workerID })
		assert.Equal(t, workerID, got.AuthorizedUser)
		require.Eventually(t, func() bool { return c.Health(context.Background()) == nil }, 20*time.Second, 100*time.Millisecond)
	})

	t.Run("token-file-rotation", func(t *testing.T) {
		dir := e.work(t, nil)
		tokenFile := filepath.Join(dir, "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("token-shop-api"), 0o600))
		c := connect(t, e.tokenConfig("token-file-rotation", tokenFile))
		first := e.waitConn(t, "token-file-rotation", func(connInfo) bool { return true })
		require.NoError(t, os.WriteFile(tokenFile, []byte("token-shop-worker"), 0o600))
		e.restart(t)
		got := e.waitConn(t, "token-file-rotation", func(ci connInfo) bool { return ci.Account == "shop" && ci.AuthorizedUser != "" && ci.AuthorizedUser != first.AuthorizedUser })
		assert.NotEqual(t, first.AuthorizedUser, got.AuthorizedUser)
		assert.Equal(t, "shop", got.Account)
		require.Eventually(t, func() bool { return c.Health(context.Background()) == nil }, 20*time.Second, 100*time.Millisecond)
	})

	t.Run("reconnects-after-broker-restart", func(t *testing.T) {
		dir := e.work(t, nil)
		tokenFile := filepath.Join(dir, "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("token-shop-api"), 0o600))
		c := connect(t, e.tokenConfig("reconnects-after-broker-restart", tokenFile))
		var got atomic.Int32
		sub, err := c.Conn().Subscribe("conformance.restart", func(*nats.Msg) { got.Add(1) })
		require.NoError(t, err)
		require.NoError(t, c.Conn().Flush())
		e.restart(t)
		// The subscription is restored by the driver; a publish after the
		// reconnect reaches it without the caller doing anything.
		require.Eventually(t, func() bool {
			if c.Health(context.Background()) != nil {
				return false
			}
			_ = c.Conn().Publish("conformance.restart", []byte("x"))
			_ = c.Conn().Flush()
			return got.Load() > 0
		}, 30*time.Second, 200*time.Millisecond)
		assert.True(t, sub.IsValid())
	})

	t.Run("drain-delivers-inflight", func(t *testing.T) {
		dir := e.work(t, nil)
		tokenFile := filepath.Join(dir, "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("token-shop-api"), 0o600))
		sub := connect(t, e.tokenConfig("drain-sub", tokenFile))
		pub := connect(t, e.tokenConfig("drain-pub", tokenFile))
		const n = 200
		var got atomic.Int32
		_, err := sub.Conn().Subscribe("conformance.drain", func(*nats.Msg) {
			time.Sleep(time.Millisecond)
			got.Add(1)
		})
		require.NoError(t, err)
		require.NoError(t, sub.Conn().Flush())
		for range n {
			require.NoError(t, pub.Conn().Publish("conformance.drain", []byte("x")))
		}
		require.NoError(t, pub.Conn().Flush())
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		require.NoError(t, sub.Close(ctx))
		assert.EqualValues(t, n, got.Load(), "every message delivered to the subscriber before the drain started is handled")
		assert.True(t, sub.Conn().IsClosed())
	})

	t.Run("jetstream-defaults", func(t *testing.T) {
		dir := e.work(t, nil)
		tokenFile := filepath.Join(dir, "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("token-shop-api"), 0o600))
		c := connect(t, e.tokenConfig("jetstream-defaults", tokenFile))
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		name := fmt.Sprintf("DEFAULTS%d", time.Now().UnixNano())
		st, err := c.EnsureStream(ctx, natsclient.StreamSpec{Name: name, Subjects: []string{subj("defaults.>")}})
		require.NoError(t, err)
		info := st.CachedInfo().Config
		assert.Equal(t, jetstream.FileStorage, info.Storage)
		assert.Equal(t, jetstream.LimitsPolicy, info.Retention)
		assert.Equal(t, jetstream.DiscardOld, info.Discard)
		assert.Equal(t, 1, info.Replicas)
		assert.Equal(t, 7*24*time.Hour, info.MaxAge)
		assert.Equal(t, 2*time.Minute, info.Duplicates)
		// Idempotent, and a changed spec updates the stream.
		_, err = c.EnsureStream(ctx, natsclient.StreamSpec{Name: name, Subjects: []string{subj("defaults.>")}})
		require.NoError(t, err)
		st, err = c.EnsureStream(ctx, natsclient.StreamSpec{Name: name, Subjects: []string{subj("defaults.>")}, MaxAge: time.Hour})
		require.NoError(t, err)
		assert.Equal(t, time.Hour, st.CachedInfo().Config.MaxAge)

		cons, err := c.EnsureConsumer(ctx, natsclient.ConsumerSpec{Stream: name, Durable: "worker", FilterSubject: subj("defaults.a")})
		require.NoError(t, err)
		cc := cons.CachedInfo().Config
		assert.Equal(t, "worker", cc.Durable)
		assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
		assert.Equal(t, 30*time.Second, cc.AckWait)
		assert.Equal(t, 5, cc.MaxDeliver)
		assert.Equal(t, 1000, cc.MaxAckPending)
		assert.Equal(t, jetstream.DeliverAllPolicy, cc.DeliverPolicy)
		assert.Equal(t, subj("defaults.a"), cc.FilterSubject)
		_, err = c.EnsureConsumer(ctx, natsclient.ConsumerSpec{Stream: name, Durable: "worker", FilterSubject: subj("defaults.a")})
		require.NoError(t, err)
	})

	t.Run("jetstream-publish-ack-dedup", func(t *testing.T) {
		dir := e.work(t, nil)
		tokenFile := filepath.Join(dir, "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("token-shop-api"), 0o600))
		c := connect(t, e.tokenConfig("jetstream-publish-ack-dedup", tokenFile))
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		name := fmt.Sprintf("DEDUP%d", time.Now().UnixNano())
		_, err := c.EnsureStream(ctx, natsclient.StreamSpec{Name: name, Subjects: []string{subj("dedup.>")}})
		require.NoError(t, err)
		first, err := c.Publish(ctx, subj("dedup.a"), []byte("1"), natsclient.PublishOptions{MsgID: "m-1"})
		require.NoError(t, err)
		assert.False(t, first.Duplicate)
		second, err := c.Publish(ctx, subj("dedup.a"), []byte("1"), natsclient.PublishOptions{MsgID: "m-1"})
		require.NoError(t, err)
		assert.True(t, second.Duplicate)
		assert.Equal(t, first.Sequence, second.Sequence)
		_, err = c.Publish(ctx, subj("dedup.*"), []byte("1"), natsclient.PublishOptions{})
		assert.Error(t, err, "a wildcard subject is refused before it is sent")
	})

	t.Run("cert-identity-publish-limited", func(t *testing.T) {
		// The stream is made by a token client; the certificate identity may only publish.
		dir := e.work(t, certs)
		tokenFile := filepath.Join(dir, "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("token-shop-api"), 0o600))
		admin := connect(t, e.tokenConfig("cert-limited-admin", tokenFile))
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		name := fmt.Sprintf("LIMITED%d", time.Now().UnixNano())
		_, err := admin.EnsureStream(ctx, natsclient.StreamSpec{Name: name, Subjects: []string{subj("limited.>"), "forbidden." + run + ".>"}})
		require.NoError(t, err)

		c := connect(t, e.certConfig(t, "cert-identity-publish-limited", dir, "api"))
		ack, err := c.Publish(ctx, subj("limited.ok"), []byte("x"), natsclient.PublishOptions{})
		require.NoError(t, err)
		assert.Equal(t, name, ack.Stream)
		short, cancel2 := context.WithTimeout(ctx, 2*time.Second)
		defer cancel2()
		_, err = c.Publish(short, "forbidden."+run+".no", []byte("x"), natsclient.PublishOptions{})
		assert.Error(t, err, "the identity may not publish outside its subjects")
	})

	t.Run("propagates-trace-headers", func(t *testing.T) {
		dir := e.work(t, nil)
		tokenFile := filepath.Join(dir, "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("token-shop-api"), 0o600))
		c := connect(t, e.tokenConfig("propagates-trace-headers", tokenFile))
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		name := fmt.Sprintf("TRACE%d", time.Now().UnixNano())
		_, err := c.EnsureStream(ctx, natsclient.StreamSpec{Name: name, Subjects: []string{subj("trace.>")}})
		require.NoError(t, err)

		const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
		_, err = c.Publish(ctx, subj("trace.a"), []byte("x"), natsclient.PublishOptions{Traceparent: tp, Tracestate: "k=v"})
		require.NoError(t, err)

		// What is on the wire: the lower-case keys, exactly, read by the driver without the adapter.
		cons, err := c.JetStream().OrderedConsumer(ctx, name, jetstream.OrderedConsumerConfig{})
		require.NoError(t, err)
		m, err := cons.Next(jetstream.FetchMaxWait(5 * time.Second))
		require.NoError(t, err)
		_, lower := m.Headers()["traceparent"]
		assert.True(t, lower, "traceparent is written lower-case")
		_, canonical := m.Headers()["Traceparent"]
		assert.False(t, canonical, "and not in the HTTP-canonical spelling")
		gotTP, gotTS := natsclient.ExtractTrace(m.Headers())
		assert.Equal(t, tp, gotTP)
		assert.Equal(t, "k=v", gotTS)

		// A message another language wrote with the canonical spelling is still read.
		other := nats.Header{"Traceparent": []string{tp}, "Tracestate": []string{"a=b"}}
		gotTP, gotTS = natsclient.ExtractTrace(other)
		assert.Equal(t, tp, gotTP)
		assert.Equal(t, "a=b", gotTS)
		// Setting replaces every spelling.
		natsclient.InjectTrace(other, "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00", "")
		assert.Equal(t, []string{"traceparent"}, natsclient.HeaderCarrier(other).Keys())
	})

	t.Run("health-check", func(t *testing.T) {
		dir := e.work(t, nil)
		tokenFile := filepath.Join(dir, "token")
		require.NoError(t, os.WriteFile(tokenFile, []byte("token-shop-api"), 0o600))
		c := connect(t, e.tokenConfig("health-check", tokenFile))
		start := time.Now()
		require.NoError(t, c.Health(context.Background()))
		assert.Less(t, time.Since(start), 2*time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, c.Close(ctx))
		assert.Error(t, c.Health(context.Background()), "a closed connection is not healthy")
	})

	t.Run("account-for-namespace-vectors", func(t *testing.T) {
		var v struct {
			ProjectAccounts []string `json:"projectAccounts"`
			Cases           []struct {
				Namespace string `json:"namespace"`
				Account   string `json:"account"`
				Error     bool   `json:"error"`
			} `json:"cases"`
		}
		readVectors(t, "account-for-namespace.json", &v)
		require.NotEmpty(t, v.Cases)
		set := map[string]struct{}{}
		for _, a := range v.ProjectAccounts {
			set[a] = struct{}{}
		}
		for _, c := range v.Cases {
			got, err := natsclient.AccountForNamespace(c.Namespace, v.ProjectAccounts)
			// The callout's own function must agree: the vectors are the one truth.
			want, werr := callout.AccountForNamespace(c.Namespace, set)
			assert.Equal(t, c.Error, err != nil, "adapter: %q", c.Namespace)
			assert.Equal(t, c.Error, werr != nil, "callout: %q", c.Namespace)
			assert.Equal(t, c.Account, got, "adapter: %q", c.Namespace)
			assert.Equal(t, c.Account, want, "callout: %q", c.Namespace)
		}
	})

	t.Run("subject-vectors", func(t *testing.T) {
		var v struct {
			Valid   []string `json:"valid"`
			Invalid []string `json:"invalid"`
			TooLong int      `json:"tooLong"`
		}
		readVectors(t, "subjects.json", &v)
		require.NotEmpty(t, v.Valid)
		for _, s := range v.Valid {
			assert.NoError(t, natsclient.ValidPublishSubject(s), "%q", s)
		}
		for _, s := range v.Invalid {
			assert.Error(t, natsclient.ValidPublishSubject(s), "%q", s)
		}
		assert.NoError(t, natsclient.ValidPublishSubject(strings.Repeat("a", v.TooLong-1)))
		assert.Error(t, natsclient.ValidPublishSubject(strings.Repeat("a", v.TooLong)))
	})
}

func readVectors(t *testing.T, name string, into any) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "conformance", "vectors", name))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, into))
}
