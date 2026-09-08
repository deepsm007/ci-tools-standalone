package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type fakeGroupReader struct {
	users []string
	err   error
}

func (f *fakeGroupReader) Users(context.Context, string) ([]string, error) { return f.users, f.err }

func TestAuthorizationOverridesOnlyCurrentGroupMembers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ids.yaml")
	if err := os.WriteFile(path, []byte("kerberosSlackIDOverrides:\n  alice: UALICE\n  outsider: UOUTSIDE\n"), 0600); err != nil {
		t.Fatal(err)
	}
	a := NewAuthorizer(&fakeGroupReader{users: []string{"alice", "bob"}}, &fakeSlack{users: map[string]string{"bob@redhat.com": "UBOB"}}, "test-platform-ci-admins", path, nil, nil)
	if err := a.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := a.Authorize(context.Background(), "UALICE", "silence"); err != nil {
		t.Fatal(err)
	}
	if err := a.Authorize(context.Background(), "UBOB", "silence"); err != nil {
		t.Fatal(err)
	}
	if err := a.Authorize(context.Background(), "UOUTSIDE", "silence"); err == nil {
		t.Fatal("override outside current OpenShift Group granted authority")
	}
}

func TestAuthorizationFailsClosedPastHardCeiling(t *testing.T) {
	a := &Authorizer{group: &fakeGroupReader{err: errors.New("api down")}, resolver: &fakeSlack{}, groupName: "test-platform-ci-admins", identityPath: "/missing", now: time.Now, users: map[string]bool{"UOLD": true}, refreshedAt: time.Now().Add(-authzHardCeiling - time.Second)}
	if err := a.Authorize(context.Background(), "UOLD", "silence"); err == nil {
		t.Fatal("stale authorization cache was served")
	}
}

func TestIdentityMapRejectsMalformedAndDuplicateSlackIDs(t *testing.T) {
	for name, body := range map[string]string{"malformed": "kerberosSlackIDOverrides:\n  alice: alice\n", "duplicate": "kerberosSlackIDOverrides:\n  alice: U1\n  bob: U1\n"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ids.yaml")
			_ = os.WriteFile(path, []byte(body), 0600)
			if _, err := loadIdentityMap(path, []string{"alice", "bob"}); err == nil {
				t.Fatal("invalid identity map accepted")
			}
		})
	}
}

func TestIdentityMapReportsOverridesOutsideCurrentGroup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ids.yaml")
	if err := os.WriteFile(path, []byte("kerberosSlackIDOverrides:\n  alice: UALICE\n  outsider: UOUTSIDE\n"), 0600); err != nil {
		t.Fatal(err)
	}
	overrides, ignored, err := loadIdentityMapDetailed(path, []string{"alice"})
	if err != nil {
		t.Fatal(err)
	}
	if overrides["alice"] != "UALICE" || len(ignored) != 1 || ignored[0] != "outsider" || overrides["outsider"] != "" {
		t.Fatalf("overrides=%#v ignored=%#v", overrides, ignored)
	}
}
