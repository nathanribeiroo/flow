package flow_test

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nathanribeiroo/flow"
)

func TestRunnerMermaid(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		graph  func() *flow.Graph[probe]
		golden string
	}{
		{
			name: "sanitizes ids and labels",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("plan", visit("plan")).
					Add("search-kb", visit("search-kb")).
					Add("call llm", visit("call llm")).
					Add(`say "hi"`, visit("say")).
					Add("audit", visit("audit"), flow.WithTimeout(time.Second)).
					Start("plan").
					Edge("plan", "search-kb").
					Edge("plan", "call llm").
					Edge("search-kb", flow.End).
					Branch("call llm", fixed(flow.End), `say "hi"`, flow.End).
					Edge(`say "hi"`, flow.End).
					Detach("plan", "audit")
			},
			golden: "mermaid_topology.golden",
		},
		{
			name: "omits end when nothing points to it",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).
					Start("a").
					Edge("a", "b").Edge("b", "a")
			},
			golden: "mermaid_cycle.golden",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runner, err := tt.graph().Compile()
			if err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
			want, err := os.ReadFile(filepath.Join("testdata", tt.golden))
			if err != nil {
				t.Fatalf("reading golden: %v", err)
			}
			if got := runner.Mermaid(); got != string(want) {
				t.Errorf("Mermaid() =\n%s\nwant\n%s", got, want)
			}
		})
	}
}

func TestRunnerMermaidTrace(t *testing.T) {
	t.Parallel()
	again := func(_ context.Context, p *probe) []string {
		if len(p.seen()) < 4 {
			return []string{"a"}
		}
		return []string{flow.End}
	}
	runner, err := flow.New[probe]("g").
		Add("a", visit("a")).Add("b", visit("b")).Add("c", visit("c")).
		Start("a").
		Edge("a", "b").
		Branch("b", again, "a", "c", flow.End).
		Edge("c", flow.End).
		Compile()
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}

	t.Run("marks the visited nodes once in declaration order", func(t *testing.T) {
		t.Parallel()
		res, err := runner.Run(t.Context(), &probe{}, flow.WithRunID(testRunID))
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		if want := []string{"a", "b", "a", "b"}; !slices.Equal(res.Path, want) {
			t.Fatalf("Run() path = %v, want %v", res.Path, want)
		}
		want, err := os.ReadFile(filepath.Join("testdata", "mermaid_trace.golden"))
		if err != nil {
			t.Fatalf("reading golden: %v", err)
		}
		if got := runner.MermaidTrace(res); got != string(want) {
			t.Errorf("MermaidTrace() =\n%s\nwant\n%s", got, want)
		}
	})

	t.Run("empty path adds no class line", func(t *testing.T) {
		t.Parallel()
		got := runner.MermaidTrace(flow.Result{})
		if !strings.HasPrefix(got, runner.Mermaid()) || strings.Contains(got, "class n_") {
			t.Errorf("MermaidTrace(empty) =\n%s\nwant the topology plus classDef only", got)
		}
	})
}
