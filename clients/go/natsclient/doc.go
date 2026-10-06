// Package natsclient is the Go adapter of the nats client contract
// (clients/README.md): a connection to the shared broker that always verifies
// the server against the given CA file only, authenticates with a reloading
// workload certificate or a ServiceAccount token file (both read again for
// every new connection), reconnects without limit, and ships the JetStream
// defaults, trace-header propagation and orderly drain the estate expects.
//
// It is configured with the environment listed in the contract, see
// [FromEnv].
package natsclient
