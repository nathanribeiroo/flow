package flow_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/nathanribeiroo/flow"
)

// probe é o estado dos testes: guarda a ordem em que os nós rodaram.
type probe struct {
	visited []string
}

// visit devolve um nó que registra name no estado.
func visit(name string) flow.Node[probe] {
	return func(_ context.Context, p *probe) error {
		p.visited = append(p.visited, name)
		return nil
	}
}

// fixed devolve um Router que sempre escolhe targets.
func fixed(targets ...string) flow.Router[probe] {
	return func(_ context.Context, _ *probe) []string { return targets }
}

// runProbe compila e executa g sobre um estado novo, falhando o teste em
// qualquer erro. Devolve o Result e os nós que de fato escreveram no estado.
func runProbe(t *testing.T, g *flow.Graph[probe]) (flow.Result, []string) {
	t.Helper()
	runner, err := g.Compile()
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	var p probe
	res, err := runner.Run(t.Context(), &p)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	return res, p.visited
}

func TestGraphClone(t *testing.T) {
	t.Parallel()

	t.Run("clone is independent from the original", func(t *testing.T) {
		t.Parallel()
		original := flow.New[probe]("g").
			Add("a", visit("a")).
			Start("a").
			Branch("a", fixed(flow.End), flow.End)
		clone := original.Clone().
			Add("b", visit("b")).
			Branch("a", fixed("b"), "b", flow.End).
			Edge("b", flow.End)

		res, visited := runProbe(t, original)
		if want := []string{"a"}; !slices.Equal(res.Path, want) || !slices.Equal(visited, want) {
			t.Errorf("original path = %v, visited = %v, want %v", res.Path, visited, want)
		}
		res, visited = runProbe(t, clone)
		if want := []string{"a", "b"}; !slices.Equal(res.Path, want) || !slices.Equal(visited, want) {
			t.Errorf("clone path = %v, visited = %v, want %v", res.Path, visited, want)
		}
	})

	t.Run("add replaces the node and keeps the edges", func(t *testing.T) {
		t.Parallel()
		original := flow.New[probe]("g").
			Add("a", visit("a")).
			Add("b", visit("b")).
			Start("a").
			Edge("a", "b").
			Edge("b", flow.End)
		stubbed := original.Clone().Add("b", visit("stub"))

		_, visited := runProbe(t, original)
		if want := []string{"a", "b"}; !slices.Equal(visited, want) {
			t.Errorf("original visited = %v, want %v", visited, want)
		}
		res, visited := runProbe(t, stubbed)
		if want := []string{"a", "stub"}; !slices.Equal(visited, want) {
			t.Errorf("stubbed visited = %v, want %v", visited, want)
		}
		if want := []string{"a", "b"}; !slices.Equal(res.Path, want) {
			t.Errorf("stubbed path = %v, want %v", res.Path, want)
		}
	})

	t.Run("clone carries builder issues", func(t *testing.T) {
		t.Parallel()
		original := flow.New[probe]("g").Add("", visit("x"))

		_, err := original.Clone().Compile()
		var cerr *flow.CompileError
		if !errors.As(err, &cerr) {
			t.Fatalf("Compile() error = %v, want *CompileError", err)
		}
		if want := (flow.Issue{Reason: "node name is empty"}); !slices.Contains(cerr.Issues, want) {
			t.Errorf("Compile() issues = %v, want to contain %v", cerr.Issues, want)
		}
	})

	t.Run("runner does not see builder changes after compile", func(t *testing.T) {
		t.Parallel()
		g := flow.New[probe]("g").Add("a", visit("a")).Start("a").Edge("a", flow.End)
		runner, err := g.Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		before := runner.Mermaid()

		g.Add("b", visit("b")).Edge("a", "b").Edge("b", flow.End)

		if after := runner.Mermaid(); after != before {
			t.Errorf("Mermaid() after builder change =\n%s\nwant unchanged\n%s", after, before)
		}
		var p probe
		res, err := runner.Run(t.Context(), &p)
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		if want := []string{"a"}; !slices.Equal(res.Path, want) {
			t.Errorf("Run() path = %v, want %v", res.Path, want)
		}
	})
}
