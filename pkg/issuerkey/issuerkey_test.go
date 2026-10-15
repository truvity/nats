package issuerkey

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"strings"
	"testing"

	"github.com/nats-io/nkeys"
)

func pkcs8PEM(t *testing.T, priv ed25519.PrivateKey) string {
	t.Helper()

	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}

	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

func TestFromPKCS8PEM(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	keys, err := FromPKCS8PEM(pkcs8PEM(t, priv))
	if err != nil {
		t.Fatal(err)
	}

	if !strings.HasPrefix(keys.Seed, "SA") || !nkeys.IsValidPublicAccountKey(keys.IssuerPublic) || !nkeys.IsValidPublicUserKey(keys.UserPublic) {
		t.Fatalf("unexpected shapes: %+v", keys)
	}

	account, err := nkeys.FromSeed([]byte(keys.Seed))
	if err != nil {
		t.Fatal(err)
	}

	if pub, _ := account.PublicKey(); pub != keys.IssuerPublic {
		t.Errorf("seed public %s != IssuerPublic %s", pub, keys.IssuerPublic)
	}

	// The same raw seed must yield both forms.
	user, err := nkeys.FromRawSeed(nkeys.PrefixByteUser, priv.Seed())
	if err != nil {
		t.Fatal(err)
	}

	if pub, _ := user.PublicKey(); pub != keys.UserPublic {
		t.Errorf("user public %s != %s", pub, keys.UserPublic)
	}
}

func TestFromPKCS8PEMRejects(t *testing.T) {
	if _, err := FromPKCS8PEM("not pem"); err == nil {
		t.Error("want error for non-PEM")
	}

	if _, err := FromPKCS8PEM("-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n"); err == nil {
		t.Error("want error for bad PKCS#8")
	}
}
