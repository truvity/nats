package tenancy_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/truvity/nats/pkg/tenancy"
)

func TestIdentitiesAreOnePerServiceAccountInTheProjectsAccount(t *testing.T) {
	publish := []string{"urls.created"}
	got := tenancy.Identities("shop", "env.example.internal", []string{"api", "api-v2"}, publish)

	want := []tenancy.Identity{
		{Account: "shop", SPIFFEID: "spiffe://env.example.internal/ns/shop/sa/api", Publish: publish},
		{Account: "shop", SPIFFEID: "spiffe://env.example.internal/ns/shop/sa/api-v2", Publish: publish},
	}

	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	for i := range want {
		if got[i].Account != want[i].Account || got[i].SPIFFEID != want[i].SPIFFEID || !slices.Equal(got[i].Publish, want[i].Publish) {
			t.Errorf("row %d: got %+v, want %+v", i, got[i], want[i])
		}
	}

	got[0].Publish[0] = "changed"

	if publish[0] != "urls.created" {
		t.Error("the rows share the caller's slice")
	}
}

func TestValidateClient(t *testing.T) {
	ok := func(sa, publish []string) {
		t.Helper()

		if err := tenancy.ValidateClient(sa, publish); err != nil {
			t.Errorf("%v %v: %v", sa, publish, err)
		}
	}
	bad := func(want string, sa, publish []string) {
		t.Helper()

		err := tenancy.ValidateClient(sa, publish)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%v %v: err %v, want %q", sa, publish, err, want)
		}
	}

	ok([]string{"api"}, []string{"a.b", "c_d-e"})
	bad("service_accounts is empty", nil, []string{"a"})
	bad("is not a ServiceAccount name", []string{"Bad_Name"}, []string{"a"})
	bad("twice", []string{"a", "a"}, []string{"x"})
	bad("publish is empty", []string{"a"}, nil)
	bad("no wildcard", []string{"a"}, []string{"a.>"})
	bad("no wildcard", []string{"a"}, []string{"a.*"})
	bad("empty", []string{"a"}, []string{""})
	bad("is empty or carries", []string{"a"}, []string{"a..b"})
	bad("twice", []string{"a"}, []string{"x", "x"})
}
