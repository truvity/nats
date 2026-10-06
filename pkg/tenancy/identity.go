// Package tenancy is the Go side of the nats-broker chart's `tenancy` preset
// (charts/nats-broker/presets/tenancy.yaml): what a deployment's catalogue of
// client-certificate identities has to say to produce the preset's
// `global.tenancy.identities` rows, and the rules it is held to first.
//
// An identity maps one workload certificate, by the SPIFFE ID in its URI SAN,
// to a NATS user in the workload's project account. The broker accepts it in
// addition to the token auth callout, never instead of it.
package tenancy

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/truvity/policy/transport"
)

// Identity is one row of `global.tenancy.identities`.
type Identity struct {
	// Account is the NATS account the mapped user belongs to: the project's own,
	// the account its callout-authenticated clients already land in, which is
	// named for the namespace (the callout's 1:1 rule).
	Account string `json:"account"`
	// SPIFFEID is the certificate URI the broker maps, and the NATS user's name:
	// spiffe://<trust domain>/ns/<account>/sa/<service account>.
	SPIFFEID string `json:"spiffeId"`
	// Publish is the exact subjects the user may publish.
	Publish []string `json:"publish"`
}

var (
	// serviceAccountRE is a Kubernetes DNS-1123 label: what a ServiceAccount
	// name may be, and the only thing a SPIFFE path element built from one can
	// safely be.
	serviceAccountRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	// subjectTokenRE is one token of a concrete NATS subject.
	subjectTokenRE = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
)

// Identities are the rows for the project's account: one per ServiceAccount, in
// the order given, each allowed to publish exactly the given subjects. The
// project's namespace is its account's name, and trustDomain is the
// environment's SPIFFE trust domain, the one the workloads' certificates carry.
func Identities(account, trustDomain string, serviceAccounts, publish []string) []Identity {
	out := make([]Identity, 0, len(serviceAccounts))

	for _, sa := range serviceAccounts {
		out = append(out, Identity{
			Account:  account,
			SPIFFEID: transport.SpiffeID(trustDomain, account, sa),
			Publish:  append([]string(nil), publish...),
		})
	}

	return out
}

// ValidateServiceAccount refuses a name that is not a ServiceAccount's.
func ValidateServiceAccount(name string) error {
	if !serviceAccountRE.MatchString(name) {
		return fmt.Errorf("service_accounts %q is not a ServiceAccount name", name)
	}

	return nil
}

// ValidateSubject refuses anything but a literal subject: a wildcard, an empty
// token, or whitespace would each widen the grant past what was written down.
func ValidateSubject(subject string) error {
	if subject == "" {
		return errors.New("empty subject")
	}

	if strings.ContainsAny(subject, "*> \t\r\n") {
		return errors.New("must be a concrete subject: no wildcard, no whitespace")
	}

	for _, token := range strings.Split(subject, ".") {
		if !subjectTokenRE.MatchString(token) {
			return fmt.Errorf("token %q is empty or carries a character outside [A-Za-z0-9_-]", token)
		}
	}

	return nil
}

// ValidateClient checks what a deployment writes down for one environment's
// identities: at least one ServiceAccount, each a valid name and none twice,
// and at least one subject, each concrete and none twice. The identity exists
// to do one job, and a wildcard would hand it the stream's whole subject space.
func ValidateClient(serviceAccounts, publish []string) error {
	if len(serviceAccounts) == 0 {
		return errors.New("service_accounts is empty")
	}

	seenAccounts := make(map[string]bool, len(serviceAccounts))

	for _, sa := range serviceAccounts {
		if err := ValidateServiceAccount(sa); err != nil {
			return err
		}

		if seenAccounts[sa] {
			return fmt.Errorf("service_accounts lists %q twice", sa)
		}

		seenAccounts[sa] = true
	}

	if len(publish) == 0 {
		return errors.New("publish is empty: an identity that may publish nothing is a user nobody needs")
	}

	seen := make(map[string]bool, len(publish))

	for _, subject := range publish {
		if err := ValidateSubject(subject); err != nil {
			return fmt.Errorf("publish %q: %w", subject, err)
		}

		if seen[subject] {
			return fmt.Errorf("publish lists %q twice", subject)
		}

		seen[subject] = true
	}

	return nil
}
