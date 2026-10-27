package argocd

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestIsTransientGitLockError(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "unrelated",
			err:  errors.New("rpc error: code = InvalidArgument desc = bad application set"),
			want: false,
		},
		{
			name: "index.lock File exists",
			err: errors.New(`failed to checkout revision: failed to create symbolic-ref HEAD Temporary error: ` +
				`failed to run command: /usr/bin/git symbolic-ref HEAD refs/heads/...: ` +
				`error: Unable to create '.../.git/index.lock': File exists.`),
			want: true,
		},
		{
			name: "HEAD.lock File exists",
			err:  errors.New(`error: Unable to create '/tmp/repo/.git/HEAD.lock': File exists.`),
			want: true,
		},
		{
			name: "generic lock File exists",
			err:  errors.New(`Unable to create '/tmp/repo/.git/shallow.lock': File exists.`),
			want: true,
		},
		{
			name: "index.lock permission denied",
			err:  errors.New(`error: Unable to create '.../.git/index.lock': Permission denied.`),
			want: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := isTransientGitLockError(tc.err)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("isTransientGitLockError() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
