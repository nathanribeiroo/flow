package flow_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/nathanribeiroo/flow"
)

func TestGraphCompile(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		graph func() *flow.Graph[probe]
		want  []flow.Issue // nil quer dizer que Compile deve passar
	}{
		{
			name: "start not declared",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add("a", visit("a")).Edge("a", flow.End)
			},
			want: []flow.Issue{{Reason: "start node not declared"}},
		},
		{
			name: "start does not exist",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add("a", visit("a")).Start("zz").Edge("a", flow.End)
			},
			want: []flow.Issue{{Reason: `start node "zz" does not exist`}},
		},
		{
			name: "edge to unknown node",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add("a", visit("a")).Start("a").Edge("a", "zz")
			},
			want: []flow.Issue{{Node: "a", Reason: `edge to unknown node "zz"`}},
		},
		{
			name: "edge source does not exist",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add("a", visit("a")).Start("a").Edge("a", flow.End).Edge("zz", "a")
			},
			want: []flow.Issue{{Node: "zz", Reason: "edge source does not exist"}},
		},
		{
			name: "branch without targets",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add("a", visit("a")).Start("a").Branch("a", fixed(flow.End))
			},
			want: []flow.Issue{{Node: "a", Reason: "branch has no targets"}},
		},
		{
			name: "branch to unknown node",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add("a", visit("a")).Start("a").Branch("a", fixed(flow.End), "zz", flow.End)
			},
			want: []flow.Issue{{Node: "a", Reason: `branch to unknown node "zz"`}},
		},
		{
			name: "branch with nil router",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add("a", visit("a")).Start("a").Branch("a", nil, flow.End)
			},
			want: []flow.Issue{{Node: "a", Reason: "branch router is nil"}},
		},
		{
			name: "branch source does not exist",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add("a", visit("a")).Start("a").Edge("a", flow.End).Branch("zz", fixed(flow.End), flow.End)
			},
			want: []flow.Issue{{Node: "zz", Reason: "branch source does not exist"}},
		},
		{
			name: "unreachable node",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add("a", visit("a")).Add("b", visit("b")).Start("a").Edge("a", flow.End).Edge("b", flow.End)
			},
			want: []flow.Issue{{Node: "b", Reason: "unreachable from start"}},
		},
		{
			name: "node without outgoing edge",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add("a", visit("a")).Add("b", visit("b")).Start("a").Edge("a", "b")
			},
			want: []flow.Issue{{Node: "b", Reason: "no outgoing edge"}},
		},
		{
			name: "empty node name",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add("", visit("x")).Add("a", visit("a")).Start("a").Edge("a", flow.End)
			},
			want: []flow.Issue{{Reason: "node name is empty"}},
		},
		{
			name: "reserved node name",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add(flow.End, visit("x")).Add("a", visit("a")).Start("a").Edge("a", flow.End)
			},
			want: []flow.Issue{{Node: flow.End, Reason: "node name is reserved"}},
		},
		{
			name: "nil node function",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add("a", nil).Start("a").Edge("a", flow.End)
			},
			want: []flow.Issue{{Node: "a", Reason: "node function is nil"}},
		},
		{
			name: "retry with zero attempts",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add("a", visit("a"), flow.WithRetry(0, 0)).Start("a").Edge("a", flow.End)
			},
			want: []flow.Issue{{Node: "a", Reason: "retry attempts must be at least 1"}},
		},
		{
			name: "retry with negative backoff",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add("a", visit("a"), flow.WithRetry(2, -time.Second)).Start("a").Edge("a", flow.End)
			},
			want: []flow.Issue{{Node: "a", Reason: "retry backoff must not be negative"}},
		},
		{
			name: "non-positive timeout",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add("a", visit("a"), flow.WithTimeout(0)).Start("a").Edge("a", flow.End)
			},
			want: []flow.Issue{{Node: "a", Reason: "timeout must be positive"}},
		},
		{
			name: "edges and branch on the same node",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).
					Start("a").
					Edge("a", "b").Branch("a", fixed(flow.End), flow.End).
					Edge("b", flow.End)
			},
			want: []flow.Issue{{Node: "a", Reason: "node has both static edges and a branch"}},
		},
		{
			name: "mermaid id collision",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("a-b", visit("a-b")).Add("a_b", visit("a_b")).
					Start("a-b").
					Edge("a-b", "a_b").Edge("a_b", flow.End)
			},
			want: []flow.Issue{{Node: "a_b", Reason: `mermaid id "n_a_b" collides with node "a-b"`}},
		},
		{
			name: "multiple issues reported together",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("a", nil, flow.WithTimeout(-1)).
					Add("b", visit("b")).
					Start("zz").
					Edge("a", "missing").
					Branch("b", fixed(flow.End))
			},
			want: []flow.Issue{
				{Node: "a", Reason: "node function is nil"},
				{Node: "a", Reason: "timeout must be positive"},
				{Reason: `start node "zz" does not exist`},
				{Node: "a", Reason: `edge to unknown node "missing"`},
				{Node: "b", Reason: "branch has no targets"},
			},
		},
		{
			name: "valid cyclic graph compiles",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("a", visit("a"), flow.WithTimeout(time.Second), flow.WithRetry(2, time.Millisecond)).
					Add("b", visit("b")).
					Start("a").
					Edge("a", "b").
					Branch("b", fixed(flow.End), "a", flow.End)
			},
		},
		{
			name: "repeated add keeps a single node",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add("a", visit("a")).Add("a", visit("a2")).Start("a").Edge("a", flow.End)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runner, err := tt.graph().Compile()
			if tt.want == nil {
				if err != nil {
					t.Fatalf("Compile() error = %v, want nil", err)
				}
				if got := runner.Name(); got != "g" {
					t.Errorf("Name() = %q, want %q", got, "g")
				}
				return
			}
			if runner != nil {
				t.Errorf("Compile() runner = %v, want nil", runner)
			}
			var cerr *flow.CompileError
			if !errors.As(err, &cerr) {
				t.Fatalf("Compile() error = %v, want *CompileError", err)
			}
			if cerr.Graph != "g" {
				t.Errorf("CompileError.Graph = %q, want %q", cerr.Graph, "g")
			}
			if !slices.Equal(cerr.Issues, tt.want) {
				t.Errorf("CompileError.Issues = %v, want %v", cerr.Issues, tt.want)
			}
		})
	}
}
