// Package proof holds the repository's zero-diff gate: the nats-broker chart
// wraps the upstream nats chart, and for every case under tests/cases/nats-broker
// the wrapper must render the same objects as the upstream chart given the same
// values, flattened to the upstream's own shape. That is the claim an adopter
// relies on when moving an installation to the wrapper.
//
// It holds because everything the wrapper adds is either off until asked for
// or is a values file (the presets). The chart's own objects (the alert
// rules, the network policies, the janitor) are the objects it templates
// itself; a case that turns them on is held to two claims: the render with
// them off equals the upstream's, and turning them on only ADDS (no line of
// any other object moves). Where the rules are the only extra, they add
// exactly one object of the kind `alerts.kind` names.
package proof

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/truvity/cd/parity"
)

const (
	chart = "nats-broker"
	key   = "nats"
)

func TestWrapperRendersTheUpstreamObjects(t *testing.T) {
	helm := parity.RequireHelm(t)

	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}

	archives, err := filepath.Glob(filepath.Join(root, "charts", chart, "charts", key+"-*.tgz"))
	if err != nil || len(archives) == 0 {
		t.Fatalf("no vendored %s archive", key)
	}

	cases, err := filepath.Glob(filepath.Join(root, "tests", "cases", chart, "*", "values.yaml"))
	if err != nil || len(cases) == 0 {
		t.Fatal("no cases found")
	}

	for _, values := range cases {
		caseDir := filepath.Dir(values)

		t.Run(filepath.Base(caseDir), func(t *testing.T) {
			layers := presets(t, root, caseDir)

			w := parity.Wrapper{
				Chart:     filepath.Join(root, "charts", chart),
				Upstream:  archives[0],
				Key:       key,
				Release:   fileOr(caseDir, "release", chart),
				Namespace: fileOr(caseDir, "namespace", "default"),
				Layers:    append(layers, values),
			}

			merged := w.Merged(t)
			enabled := func(path ...string) bool {
				v := any(merged)
				for _, part := range path {
					v = parity.Map(v)[part]
				}

				return parity.Truthy(v)
			}

			alerts, policy, janitor := enabled("alerts", "enabled"), enabled("networkPolicy", "enabled"), enabled("janitor", "enabled")
			if !alerts && !policy && !janitor {
				w.AssertZeroDiff(t, helm)

				return
			}

			// The chart's own objects on: with them off the wrapper is the
			// upstream, and turning them on only ADDS.
			off := w
			off.Overlay = map[string]any{
				"alerts":        map[string]any{"enabled": false},
				"networkPolicy": map[string]any{"enabled": false},
				"janitor":       map[string]any{"enabled": false},
			}
			off.AssertZeroDiff(t, helm)

			with, _ := w.Render(t, helm)
			without, _ := off.Render(t, helm)

			assertOnlyAdds(t, without, with)

			// What the alerts add is one object of the kind alerts.kind names
			// (counted only where the rules are the only extra).
			if alerts && !policy && !janitor {
				kind, _ := parity.Map(merged["alerts"])["kind"].(string)
				if kind == "" {
					kind = "VMRule"
				}

				added := strings.Count(with, "\nkind: ") - strings.Count(without, "\nkind: ")
				addedKind := strings.Count(with, "\nkind: "+kind+"\n") - strings.Count(without, "\nkind: "+kind+"\n")

				if added != 1 || addedKind != 1 {
					t.Fatalf("alerts.enabled=true must add exactly one %s and nothing else (added %d objects, %d of that kind)", kind, added, addedKind)
				}
			}
		})
	}
}

// assertOnlyAdds holds the render with the chart's own objects on to the
// render with them off: no line of the other objects moved, so the lines of
// the one are an ordered subsequence of the lines of the other.
func assertOnlyAdds(t *testing.T, without, with string) {
	t.Helper()

	on := strings.Split(with, "\n")
	i := 0

	for _, line := range strings.Split(without, "\n") {
		for i < len(on) && on[i] != line {
			i++
		}

		if i == len(on) {
			t.Fatalf("turning the chart's own objects on changed an object other than theirs; first line lost:\n%s", line)
		}

		i++
	}
}

// presets are the values files a case names (one per line of its presets
// file), layered before the case's own values the way an adopter does.
func presets(t *testing.T, root, caseDir string) []string {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(caseDir, "presets")) //nolint:gosec // fixture path
	if os.IsNotExist(err) {
		return nil
	}

	if err != nil {
		t.Fatal(err)
	}

	var out []string

	for _, name := range strings.Fields(string(raw)) {
		out = append(out, filepath.Join(root, "charts", chart, "presets", name+".yaml"))
	}

	return out
}

func fileOr(dir, name, fallback string) string {
	raw, err := os.ReadFile(filepath.Join(dir, name)) //nolint:gosec // fixture path
	if err != nil || strings.TrimSpace(string(raw)) == "" {
		return fallback
	}

	return strings.TrimSpace(string(raw))
}
