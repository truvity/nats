// Command callout is the conformance suites' auth-callout: the real responder
// (pkg/nats-auth-callout: broker login, decision, account mapping, signed user
// JWT) with the Kubernetes TokenReview replaced by a file of known tokens, so a
// suite needs a broker but no API server. It is test support and is not part of
// any release.
//
//	callout keygen                 print export lines for a fresh issuer key
//	callout serve                  run the responder
//
// serve reads NATS_URL, NATS_ISSUER_SEED, NATS_PROJECT_ACCOUNTS (as the
// responder does) and CALLOUT_TOKENS_FILE: a JSON object mapping a token to
// "<namespace>/<serviceaccount>", read again for every review.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/nats-io/nkeys"
	authv1 "k8s.io/api/authentication/v1"

	callout "github.com/truvity/nats/pkg/nats-auth-callout"
)

type fileReviewer struct{ path string }

func (r fileReviewer) Review(_ context.Context, token string, _ []string) (*authv1.TokenReviewStatus, error) {
	raw, err := os.ReadFile(r.path)
	if err != nil {
		return nil, err
	}
	var tokens map[string]string
	if err := json.Unmarshal(raw, &tokens); err != nil {
		return nil, err
	}
	who, ok := tokens[token]
	if !ok {
		return &authv1.TokenReviewStatus{Authenticated: false, Error: "unknown token"}, nil
	}
	ns, sa, _ := strings.Cut(who, "/")
	return &authv1.TokenReviewStatus{
		Authenticated: true,
		User:          authv1.UserInfo{Username: "system:serviceaccount:" + ns + ":" + sa},
	}, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: callout keygen|serve")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "keygen":
		keygen()
	case "serve":
		os.Args = os.Args[:1]
		_ = os.Setenv("NATS_TOKEN_AUDIENCE", "nats")
		if os.Getenv("NATS_HEALTH_ADDR") == "" {
			_ = os.Setenv("NATS_HEALTH_ADDR", "127.0.0.1:0")
		}
		os.Exit(callout.RunWithReviewer(os.Args, "conformance", "conformance", fileReviewer{os.Getenv("CALLOUT_TOKENS_FILE")}))
	default:
		fmt.Fprintln(os.Stderr, "usage: callout keygen|serve")
		os.Exit(2)
	}
}

// keygen prints the issuer account seed, its public key (the broker's
// auth_callout.issuer) and the same key as a user key (auth_users).
func keygen() {
	kp, err := nkeys.CreateAccount()
	must(err)
	seed, err := kp.Seed()
	must(err)
	pub, err := kp.PublicKey()
	must(err)
	_, raw, err := nkeys.DecodeSeed(seed)
	must(err)
	uk, err := nkeys.FromRawSeed(nkeys.PrefixByteUser, raw)
	must(err)
	upub, err := uk.PublicKey()
	must(err)
	fmt.Printf("ISSUER_SEED=%s\nISSUER_PUB=%s\nISSUER_USER_PUB=%s\n", seed, pub, upub)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
