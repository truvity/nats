// Package issuerkey derives the NATS auth-callout issuer identities from one
// ED25519 private key.
//
// One key is the single secret of the callout design: its raw 32-byte seed
// is encoded as an ACCOUNT nkey (the broker's auth_callout issuer, "A..."
// public key, "SA..." seed that signs responses) and re-encoded as a USER
// nkey (the callout's login identity, "U..." public key, which the broker
// lists in auth_callout.auth_users). The package is pure conversion: it
// takes the PKCS#8 PEM that any ED25519 key generator emits and has no
// dependency on how the key is minted or stored.
package issuerkey

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"

	"github.com/nats-io/nkeys"
)

// Keys are the three strings derived from one ED25519 key.
type Keys struct {
	// Seed is the ACCOUNT nkey seed ("SA..."): NATS_ISSUER_SEED of the callout.
	Seed string
	// IssuerPublic is the ACCOUNT public key ("A..."): the broker's
	// auth_callout issuer.
	IssuerPublic string
	// UserPublic is the USER public key ("U...") of the same raw seed: the
	// broker's auth_callout auth_users entry.
	UserPublic string
}

// FromPKCS8PEM derives Keys from a PKCS#8 ED25519 private-key PEM.
func FromPKCS8PEM(pemStr string) (Keys, error) {
	raw, err := rawSeed(pemStr)
	if err != nil {
		return Keys{}, err
	}

	account, err := nkeys.FromRawSeed(nkeys.PrefixByteAccount, raw)
	if err != nil {
		return Keys{}, fmt.Errorf("issuerkey: account nkey from raw seed: %w", err)
	}

	user, err := nkeys.FromRawSeed(nkeys.PrefixByteUser, raw)
	if err != nil {
		return Keys{}, fmt.Errorf("issuerkey: user nkey from raw seed: %w", err)
	}

	seed, err := account.Seed()
	if err != nil {
		return Keys{}, fmt.Errorf("issuerkey: encode seed: %w", err)
	}

	issuerPub, err := account.PublicKey()
	if err != nil {
		return Keys{}, fmt.Errorf("issuerkey: derive issuer public key: %w", err)
	}

	userPub, err := user.PublicKey()
	if err != nil {
		return Keys{}, fmt.Errorf("issuerkey: derive user public key: %w", err)
	}

	return Keys{Seed: string(seed), IssuerPublic: issuerPub, UserPublic: userPub}, nil
}

func rawSeed(pemStr string) ([]byte, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, errors.New("issuerkey: no PEM block in private key")
	}

	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("issuerkey: parse PKCS#8: %w", err)
	}

	priv, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("issuerkey: key is %T, want ed25519.PrivateKey", parsed)
	}

	return priv.Seed(), nil
}
