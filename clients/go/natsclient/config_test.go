package natsclient

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestFromEnvToken(t *testing.T) {
	c, err := FromEnv(env(map[string]string{"NATS_URL": "nats://nats.example:4222", "NATS_TOKEN_FILE": "/var/run/token"}))
	require.NoError(t, err)
	assert.Equal(t, "/var/run/token", c.TokenFile)
	assert.False(t, c.tls())
	assert.Equal(t, DefaultConfig().Connect, c.Connect)
	assert.NotEmpty(t, c.Name)
}

func TestFromEnvCert(t *testing.T) {
	c, err := FromEnv(env(map[string]string{
		"NATS_URL": "tls://nats.example:4222", "NATS_CA_FILE": "/ca", "NATS_CERT_FILE": "/crt", "NATS_KEY_FILE": "/key",
		"NATS_CLIENT_NAME": "svc", "NATS_CLIENT_RECONNECT_WAIT": "250ms", "NATS_CLIENT_RETRY_ATTEMPTS": "7",
	}))
	require.NoError(t, err)
	assert.True(t, c.tls())
	assert.Equal(t, "svc", c.Name)
	assert.EqualValues(t, 250_000_000, c.ReconnectWait)
	assert.Equal(t, 7, c.Connect.Attempts)
}

func TestFromEnvListsEveryProblem(t *testing.T) {
	_, err := FromEnv(env(map[string]string{"NATS_URL": "ftp://a,b", "NATS_CLIENT_PING_INTERVAL": "soon"}))
	require.Error(t, err)
	assert.ErrorContains(t, err, "PING_INTERVAL")
	_, err = FromEnv(env(map[string]string{"NATS_URL": "tls://a:4222", "NATS_TOKEN_FILE": "/t"}))
	assert.ErrorContains(t, err, "server CA")
	_, err = FromEnv(env(map[string]string{"NATS_URL": "nats://a:4222"}))
	assert.ErrorContains(t, err, "credential is required")
	_, err = FromEnv(env(map[string]string{"NATS_URL": "nats://a:4222", "NATS_CERT_FILE": "/c", "NATS_KEY_FILE": "/k"}))
	assert.ErrorContains(t, err, "server CA", "a client certificate is only sent over a verified connection")
}

func TestConfigNeverPrintsACredential(t *testing.T) {
	c := DefaultConfig()
	c.URL = "nats://user:s3cret@nats.example:4222"
	c.TokenFile = "/var/run/token"
	for _, s := range []string{c.String(), fmt.Sprintf("%#v", c), fmt.Sprint(c.LogValue())} {
		assert.NotContains(t, s, "s3cret")
	}
}
