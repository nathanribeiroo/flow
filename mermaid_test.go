package flow_test

import (
	"os"
	"path/filepath"
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
