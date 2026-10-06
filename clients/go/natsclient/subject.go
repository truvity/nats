package natsclient

import (
	"fmt"
	"slices"
	"strings"
)

// AccountForNamespace is the broker's mapping rule: the account a client of a
// namespace lands in. A namespace in projectAccounts, "emp-<slug>",
// "ci-<org>-<repo>" and exactly "ci" each map to the account of the same name;
// anything else has none. The callout applies the same rule; the shared vectors
// (clients/conformance/vectors) keep every language and the callout agreeing.
//
// Subjects are not prefixed with the account: the account boundary is the
// isolation.
func AccountForNamespace(namespace string, projectAccounts []string) (string, error) {
	if slices.Contains(projectAccounts, namespace) {
		return namespace, nil
	}
	if slug, ok := strings.CutPrefix(namespace, "emp-"); ok && slug != "" {
		return namespace, nil
	}
	if rest, ok := strings.CutPrefix(namespace, "ci-"); ok {
		if org, repo, split := strings.Cut(rest, "-"); split && org != "" && repo != "" {
			return namespace, nil
		}
	}
	if namespace == "ci" {
		return namespace, nil
	}
	return "", fmt.Errorf("natsclient: namespace %q has no NATS account mapping", namespace)
}

// maxSubjectLength is the longest publish subject accepted.
const maxSubjectLength = 255

// ValidPublishSubject checks a subject a client publishes to: concrete
// (no wildcards), dot-separated non-empty tokens of letters, digits, '-' and
// '_', and not in the broker's reserved space ('$' prefix, "_INBOX.").
func ValidPublishSubject(subject string) error {
	switch {
	case subject == "":
		return fmt.Errorf("natsclient: subject is empty")
	case len(subject) > maxSubjectLength:
		return fmt.Errorf("natsclient: subject is longer than %d bytes", maxSubjectLength)
	case strings.HasPrefix(subject, "$") || strings.HasPrefix(subject, "_INBOX."):
		return fmt.Errorf("natsclient: subject %q is in the broker's reserved space", subject)
	}
	for _, tok := range strings.Split(subject, ".") {
		if tok == "" {
			return fmt.Errorf("natsclient: subject %q has an empty token", subject)
		}
		for _, r := range tok {
			ok := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_'
			if !ok {
				return fmt.Errorf("natsclient: subject %q: character %q is not allowed (no wildcards, spaces or other punctuation)", subject, r)
			}
		}
	}
	return nil
}
