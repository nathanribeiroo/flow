package flow_test

import (
	"errors"
	"testing"

	"github.com/nathanribeiroo/flow"
)

var errBoom = errors.New("boom")

func TestCompileErrorError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  *flow.CompileError
		want string
	}{
		{
			name: "single graph issue",
			err: &flow.CompileError{Graph: "g", Issues: []flow.Issue{
				{Reason: "start node not declared"},
			}},
			want: `flow: graph "g": 1 issue: start node not declared`,
		},
		{
			name: "multiple node issues",
			err: &flow.CompileError{Graph: "g", Issues: []flow.Issue{
				{Node: "a", Reason: "no outgoing edge"},
				{Node: "b", Reason: "unreachable from start"},
			}},
			want: `flow: graph "g": 2 issues: node "a": no outgoing edge; node "b": unreachable from start`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNodeError(t *testing.T) {
	t.Parallel()
	err := &flow.NodeError{Node: "a", Step: 2, Attempt: 3, Err: errBoom}

	t.Run("message locates the failure", func(t *testing.T) {
		t.Parallel()
		if got, want := err.Error(), `flow: node "a" failed at step 2 attempt 3: boom`; got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})

	t.Run("unwrap exposes the cause", func(t *testing.T) {
		t.Parallel()
		if !errors.Is(err, errBoom) {
			t.Errorf("errors.Is(err, errBoom) = false, want true")
		}
	})
}
