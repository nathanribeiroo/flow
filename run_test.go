package flow_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nathanribeiroo/flow"
)

// failing devolve um nó que falha nas primeiras failures execuções e conta
// todas as chamadas em *calls.
func failing(failures int, calls *int) flow.Node[probe] {
	return func(_ context.Context, _ *probe) error {
		*calls++
		if *calls <= failures {
			return errBoom
		}
		return nil
	}
}

// blocking devolve um nó que só retorna quando o contexto encerra, contando
// as chamadas em *calls.
func blocking(calls *int) flow.Node[probe] {
	return func(ctx context.Context, _ *probe) error {
		*calls++
		<-ctx.Done()
		return ctx.Err()
	}
}

// sleeping devolve um nó que ignora o contexto e demora d.
func sleeping(name string, d time.Duration) flow.Node[probe] {
	return func(ctx context.Context, p *probe) error {
		time.Sleep(d)
		return visit(name)(ctx, p)
	}
}

// diamond monta a -> {b, c} -> d -> End, com opts aplicadas em d.
func diamond(opts ...flow.NodeOption) *flow.Graph[probe] {
	return flow.New[probe]("g").
		Add("a", visit("a")).Add("b", visit("b")).Add("c", visit("c")).
		Add("d", visit("d"), opts...).
		Start("a").
		Edge("a", "b").Edge("a", "c").
		Edge("b", "d").Edge("c", "d").
		Edge("d", flow.End)
}

func TestRunnerRun(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		graph     func() *flow.Graph[probe]
		opts      []flow.RunOption
		wantPath  []string
		wantSteps int
		wantErr   error
	}{
		{
			name: "static edges run in declaration order",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).Add("c", visit("c")).
					Start("a").
					Edge("a", "b").Edge("b", "c").Edge("c", flow.End)
			},
			wantPath:  []string{"a", "b", "c"},
			wantSteps: 3,
		},
		{
			name: "frontier follows node declaration order not edge order",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).Add("c", visit("c")).
					Start("a").
					Edge("a", "c").Edge("a", "b").
					Edge("b", flow.End).Edge("c", flow.End)
			},
			opts:      []flow.RunOption{flow.WithSequential()},
			wantPath:  []string{"a", "b", "c"},
			wantSteps: 2,
		},
		{
			name: "branch loops back and then routes to end",
			graph: func() *flow.Graph[probe] {
				again := func(_ context.Context, p *probe) []string {
					if len(p.seen()) < 3 {
						return []string{"b"}
					}
					return []string{flow.End}
				}
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).
					Start("a").
					Branch("a", again, "b", flow.End).
					Edge("b", "a")
			},
			wantPath:  []string{"a", "b", "a"},
			wantSteps: 3,
		},
		{
			name: "router returning end stops the run",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add("a", visit("a")).Start("a").Branch("a", fixed(flow.End), flow.End)
			},
			wantPath:  []string{"a"},
			wantSteps: 1,
		},
		{
			name: "router returning nothing stops the run",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).
					Start("a").
					Branch("a", fixed(), "b", flow.End).
					Edge("b", flow.End)
			},
			wantPath:  []string{"a"},
			wantSteps: 1,
		},
		{
			name: "router fan-out follows node declaration order",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).Add("c", visit("c")).
					Start("a").
					Branch("a", fixed("c", "b"), "b", "c").
					Edge("b", flow.End).Edge("c", flow.End)
			},
			opts:      []flow.RunOption{flow.WithSequential()},
			wantPath:  []string{"a", "b", "c"},
			wantSteps: 2,
		},
		{
			name: "duplicate targets run once",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).
					Start("a").
					Branch("a", fixed("b", "b"), "b").
					Edge("b", flow.End)
			},
			wantPath:  []string{"a", "b"},
			wantSteps: 2,
		},
		{
			name: "static edge to end alongside another edge",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).
					Start("a").
					Edge("a", flow.End).Edge("a", "b").
					Edge("b", flow.End)
			},
			wantPath:  []string{"a", "b"},
			wantSteps: 2,
		},
		{
			name: "undeclared target fails with ErrUnknownTarget",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").Add("a", visit("a")).Start("a").Branch("a", fixed("zz"), flow.End)
			},
			wantPath: []string{"a"},
			wantErr:  flow.ErrUnknownTarget,
		},
		{
			name: "cycle without exit hits max steps",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).
					Start("a").
					Edge("a", "b").Edge("b", "a")
			},
			opts:      []flow.RunOption{flow.WithMaxSteps(4)},
			wantPath:  []string{"a", "b", "a", "b"},
			wantSteps: 4,
			wantErr:   flow.ErrMaxSteps,
		},
		{
			name: "max steps defaults to fifty",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).
					Start("a").
					Edge("a", "b").Edge("b", "a")
			},
			wantPath:  slices.Repeat([]string{"a", "b"}, 25),
			wantSteps: 50,
			wantErr:   flow.ErrMaxSteps,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runner, err := tt.graph().Compile()
			if err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
			var p probe
			res, err := runner.Run(t.Context(), &p, slices.Concat(tt.opts, []flow.RunOption{flow.WithRunID(testRunID)})...)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Run() error = %v, want %v", err, tt.wantErr)
			}
			if !slices.Equal(res.Path, tt.wantPath) {
				t.Errorf("Run() path = %v, want %v", res.Path, tt.wantPath)
			}
			if visited := p.seen(); !slices.Equal(visited, tt.wantPath) {
				t.Errorf("state visited = %v, want %v", visited, tt.wantPath)
			}
			if res.Steps != tt.wantSteps {
				t.Errorf("Run() steps = %d, want %d", res.Steps, tt.wantSteps)
			}
		})
	}
}

func TestRunnerRunJoin(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		graph     func() *flow.Graph[probe]
		wantPath  []string
		wantSteps int
	}{
		{
			name:      "join all waits for both branches",
			graph:     func() *flow.Graph[probe] { return diamond() },
			wantPath:  []string{"a", "b", "c", "d"},
			wantSteps: 3,
		},
		{
			name:      "join any discards the other arrivals of the same wave",
			graph:     func() *flow.Graph[probe] { return diamond(flow.WithJoin(flow.JoinAny)) },
			wantPath:  []string{"a", "b", "c", "d"},
			wantSteps: 3,
		},
		{
			name: "join any fires on the first arrival and again on the next wave",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).Add("c", visit("c")).
					Add("d", visit("d"), flow.WithJoin(flow.JoinAny)).Add("e", visit("e")).
					Start("a").
					Edge("a", "b").Edge("a", "c").
					Edge("b", "d").Edge("c", "e").Edge("e", "d").
					Edge("d", flow.End)
			},
			wantPath:  []string{"a", "b", "c", "d", "e", "d"},
			wantSteps: 4,
		},
		{
			name: "unbalanced diamond runs the join once",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).Add("c", visit("c")).
					Add("d", visit("d")).Add("e", visit("e")).
					Start("a").
					Edge("a", "b").Edge("a", "c").
					Edge("b", "d").Edge("c", "e").Edge("e", "d").
					Edge("d", flow.End)
			},
			wantPath:  []string{"a", "b", "c", "e", "d"},
			wantSteps: 4,
		},
		{
			name: "branch that does not fire a branch does not block the join",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).Add("c", visit("c")).Add("d", visit("d")).
					Start("a").
					Branch("a", fixed("b"), "b", "c").
					Edge("b", "d").Edge("c", "d").
					Edge("d", flow.End)
			},
			wantPath:  []string{"a", "b", "d"},
			wantSteps: 3,
		},
		{
			// Losango a -> {b, c} -> d com um ciclo interno ao ramo b: b -> e e
			// e volta para b uma vez antes de seguir para d. Enquanto o ciclo
			// gira, e e b alcançam d de ida, então d espera; d roda uma vez.
			name: "cycle inside one branch of a diamond runs the join once",
			graph: func() *flow.Graph[probe] {
				loops := 0
				loopOrJoin := func(_ context.Context, _ *probe) []string {
					loops++
					if loops < 2 {
						return []string{"b"}
					}
					return []string{"d"}
				}
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).Add("c", visit("c")).
					Add("d", visit("d")).Add("e", visit("e")).
					Start("a").
					Edge("a", "b").Edge("a", "c").
					Edge("b", "e").
					Branch("e", loopOrJoin, "b", "d").
					Edge("c", "d").
					Edge("d", flow.End)
			},
			wantPath:  []string{"a", "b", "c", "e", "b", "e", "d"},
			wantSteps: 6,
		},
		{
			// Grafo do README: fan-out dentro de ciclo. b e c disparam no mesmo
			// passo em cada volta, o que os 6 passos para 8 nós provam.
			name: "readme graph runs the fan-out in parallel inside the cycle",
			graph: func() *flow.Graph[probe] {
				again := func(_ context.Context, p *probe) []string {
					if len(p.seen()) < 5 {
						return []string{"a"}
					}
					return []string{flow.End}
				}
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).Add("c", visit("c")).Add("d", visit("d")).
					Start("a").
					Edge("a", "b").Edge("a", "c").
					Edge("b", "d").Edge("c", "d").
					Branch("d", again, "a", flow.End)
			},
			wantPath:  []string{"a", "b", "c", "d", "a", "b", "c", "d"},
			wantSteps: 6,
		},
		{
			name: "candidate reached by a sibling through a direct edge waits for it",
			graph: func() *flow.Graph[probe] {
				again := func(_ context.Context, p *probe) []string {
					if len(p.seen()) < 6 {
						return []string{"a"}
					}
					return []string{flow.End}
				}
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).Add("c", visit("c")).
					Start("a").
					Branch("a", fixed("b", "c"), "b", "c").
					Edge("b", "c").
					Branch("c", again, "a", flow.End)
			},
			wantPath:  []string{"a", "b", "c", "a", "b", "c"},
			wantSteps: 6,
		},
		{
			name: "cycle entered by a fan-out is ordered by the dfs tree",
			graph: func() *flow.Graph[probe] {
				once := func(_ context.Context, p *probe) []string {
					if len(p.seen()) < 3 {
						return []string{"c"}
					}
					return []string{flow.End}
				}
				return flow.New[probe]("g").
					Add("s", visit("s")).Add("a", visit("a")).Add("b", visit("b")).Add("c", visit("c")).
					Start("s").
					Branch("s", fixed("a", "b", "c"), "a", "b", "c").
					Branch("a", once, "c", flow.End).
					Edge("c", "b").
					Edge("b", "a")
			},
			wantPath:  []string{"s", "a", "c", "b", "a"},
			wantSteps: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runner, err := tt.graph().Compile()
			if err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
			var p probe
			res, err := runner.Run(t.Context(), &p, flow.WithRunID(testRunID))
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			if !slices.Equal(res.Path, tt.wantPath) {
				t.Errorf("Run() path = %v, want %v", res.Path, tt.wantPath)
			}
			if visited := p.seen(); !slices.Equal(slices.Sorted(slices.Values(visited)), slices.Sorted(slices.Values(tt.wantPath))) {
				t.Errorf("state visited = %v, want the same nodes as %v", visited, tt.wantPath)
			}
			if res.Steps != tt.wantSteps {
				t.Errorf("Run() steps = %d, want %d", res.Steps, tt.wantSteps)
			}
		})
	}
}

func TestRunnerRunFailure(t *testing.T) {
	t.Parallel()
	fail := func(_ context.Context, _ *probe) error { return errBoom }

	t.Run("abort cancels the siblings", func(t *testing.T) {
		t.Parallel()
		calls := 0
		runner, err := flow.New[probe]("g").
			Add("a", visit("a")).Add("b", fail).Add("c", blocking(&calls)).
			Start("a").
			Edge("a", "b").Edge("a", "c").
			Edge("b", flow.End).Edge("c", flow.End).
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		var p probe
		res, err := runner.Run(t.Context(), &p, flow.WithRunID(testRunID))
		var nerr *flow.NodeError
		if !errors.As(err, &nerr) {
			t.Fatalf("Run() error = %v, want *NodeError", err)
		}
		if want := (flow.NodeError{RunID: testRunID, Node: "b", Step: 2, Attempt: 1, Err: errBoom}); *nerr != want {
			t.Errorf("NodeError = %+v, want %+v", *nerr, want)
		}
		if errors.Is(err, context.Canceled) {
			t.Errorf("Run() error = %v, want the sibling cancellation discarded", err)
		}
		if calls != 1 {
			t.Errorf("sibling calls = %d, want 1", calls)
		}
		if want := []string{"a"}; !slices.Equal(res.Path, want) {
			t.Errorf("Run() path = %v, want %v", res.Path, want)
		}
	})

	t.Run("real error of a sibling during cancellation is kept", func(t *testing.T) {
		t.Parallel()
		errOther := errors.New("other")
		stubborn := func(ctx context.Context, _ *probe) error {
			<-ctx.Done()
			return errOther
		}
		runner, err := flow.New[probe]("g").
			Add("a", visit("a")).Add("b", fail).Add("c", stubborn).
			Start("a").
			Edge("a", "b").Edge("a", "c").
			Edge("b", flow.End).Edge("c", flow.End).
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		_, err = runner.Run(t.Context(), &probe{}, flow.WithRunID(testRunID))
		if !errors.Is(err, errBoom) || !errors.Is(err, errOther) {
			t.Errorf("Run() error = %v, want both %v and %v", err, errBoom, errOther)
		}
	})

	t.Run("abort in sequential mode stops the frontier", func(t *testing.T) {
		t.Parallel()
		calls := 0
		runner, err := flow.New[probe]("g").
			Add("a", visit("a")).Add("b", fail).Add("c", failing(0, &calls)).
			Start("a").
			Edge("a", "b").Edge("a", "c").
			Edge("b", flow.End).Edge("c", flow.End).
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		res, err := runner.Run(t.Context(), &probe{}, flow.WithSequential(), flow.WithRunID(testRunID))
		if !errors.Is(err, errBoom) {
			t.Fatalf("Run() error = %v, want %v", err, errBoom)
		}
		if calls != 0 {
			t.Errorf("sibling calls = %d, want 0", calls)
		}
		if want := []string{"a"}; !slices.Equal(res.Path, want) {
			t.Errorf("Run() path = %v, want %v", res.Path, want)
		}
	})

	t.Run("skip continues and records the error", func(t *testing.T) {
		t.Parallel()
		runner, err := diamond().Add("b", fail, flow.WithOnFailure(flow.FailureSkip)).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		var p probe
		res, err := runner.Run(t.Context(), &p, flow.WithRunID(testRunID))
		if err != nil {
			t.Fatalf("Run() error = %v, want nil", err)
		}
		if want := []string{"a", "b", "c", "d"}; !slices.Equal(res.Path, want) {
			t.Errorf("Run() path = %v, want %v", res.Path, want)
		}
		if len(res.Errors) != 1 {
			t.Fatalf("Run() errors = %v, want one", res.Errors)
		}
		var nerr *flow.NodeError
		if !errors.As(res.Errors[0], &nerr) {
			t.Fatalf("Errors[0] = %v, want *NodeError", res.Errors[0])
		}
		if want := (flow.NodeError{RunID: testRunID, Node: "b", Step: 2, Attempt: 1, Err: errBoom}); *nerr != want {
			t.Errorf("Errors[0] = %+v, want %+v", *nerr, want)
		}
		if visited := p.seen(); slices.Contains(visited, "b") || !slices.Contains(visited, "d") {
			t.Errorf("state visited = %v, want d without b", visited)
		}
	})

	t.Run("all branches skipped still reach the join", func(t *testing.T) {
		t.Parallel()
		runner, err := diamond().
			Add("b", fail, flow.WithOnFailure(flow.FailureSkip)).
			Add("c", fail, flow.WithOnFailure(flow.FailureSkip)).
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		res, err := runner.Run(t.Context(), &probe{}, flow.WithRunID(testRunID))
		if err != nil {
			t.Fatalf("Run() error = %v, want nil", err)
		}
		if want := []string{"a", "b", "c", "d"}; !slices.Equal(res.Path, want) {
			t.Errorf("Run() path = %v, want %v", res.Path, want)
		}
		var first, second *flow.NodeError
		if len(res.Errors) != 2 || !errors.As(res.Errors[0], &first) || !errors.As(res.Errors[1], &second) {
			t.Fatalf("Run() errors = %v, want two *NodeError", res.Errors)
		}
		if first.Node != "b" || second.Node != "c" {
			t.Errorf("Errors nodes = %q, %q, want b, c in declaration order", first.Node, second.Node)
		}
	})

	t.Run("skipped node with a branch does not call the router", func(t *testing.T) {
		t.Parallel()
		called := false
		router := func(_ context.Context, _ *probe) []string {
			called = true
			return []string{flow.End}
		}
		runner, err := flow.New[probe]("g").
			Add("a", fail, flow.WithOnFailure(flow.FailureSkip)).
			Start("a").
			Branch("a", router, flow.End).
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		res, err := runner.Run(t.Context(), &probe{}, flow.WithRunID(testRunID))
		if err != nil {
			t.Fatalf("Run() error = %v, want nil", err)
		}
		if called {
			t.Error("router called for a skipped node, want not called")
		}
		if want := []string{"a"}; !slices.Equal(res.Path, want) || len(res.Errors) != 1 {
			t.Errorf("Run() path = %v, errors = %v, want %v and one error", res.Path, res.Errors, want)
		}
	})

	t.Run("skip does not swallow cancellation", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		cancelAndWait := func(ctx context.Context, _ *probe) error {
			cancel()
			<-ctx.Done()
			return ctx.Err()
		}
		runner, err := flow.New[probe]("g").
			Add("a", cancelAndWait, flow.WithOnFailure(flow.FailureSkip)).
			Start("a").
			Edge("a", flow.End).
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		res, err := runner.Run(ctx, &probe{}, flow.WithRunID(testRunID))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
		}
		if res.Path != nil || res.Errors != nil {
			t.Errorf("Run() result = %+v, want empty", res)
		}
	})
}

func TestRunnerRunBudget(t *testing.T) {
	t.Parallel()

	t.Run("budget expired while a node ignored it stops before the next superstep", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			calls := 0
			runner, err := flow.New[probe]("g").
				Add("a", sleeping("a", 2*time.Second)).
				Add("b", sleeping("b", 2*time.Second)).
				Add("c", failing(0, &calls)).
				Start("a").
				Edge("a", "b").Edge("b", "c").Edge("c", flow.End).
				Compile()
			if err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
			start := time.Now()
			res, err := runner.Run(t.Context(), &probe{}, flow.WithBudget(3*time.Second), flow.WithRunID(testRunID))
			if !errors.Is(err, flow.ErrBudget) {
				t.Fatalf("Run() error = %v, want %v", err, flow.ErrBudget)
			}
			if want := []string{"a", "b"}; !slices.Equal(res.Path, want) {
				t.Errorf("Run() path = %v, want %v", res.Path, want)
			}
			if calls != 0 {
				t.Errorf("node c calls = %d, want 0", calls)
			}
			if got := time.Since(start); got != 4*time.Second {
				t.Errorf("elapsed = %v, want %v", got, 4*time.Second)
			}
		})
	})

	t.Run("budget expired inside a node that honors it", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			calls := 0
			runner, err := flow.New[probe]("g").
				Add("a", blocking(&calls), flow.WithRetry(3, 0)).
				Start("a").
				Edge("a", flow.End).
				Compile()
			if err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
			start := time.Now()
			_, err = runner.Run(t.Context(), &probe{}, flow.WithBudget(time.Second), flow.WithRunID(testRunID))
			if !errors.Is(err, flow.ErrBudget) {
				t.Fatalf("Run() error = %v, want %v", err, flow.ErrBudget)
			}
			var nerr *flow.NodeError
			if errors.As(err, &nerr) {
				t.Errorf("Run() error = %v, want ErrBudget without *NodeError", err)
			}
			if calls != 1 {
				t.Errorf("node calls = %d, want 1 (no retry after budget)", calls)
			}
			if got := time.Since(start); got != time.Second {
				t.Errorf("elapsed = %v, want %v", got, time.Second)
			}
		})
	})

	t.Run("budget wins over a node error in the same step", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			lateFail := func(_ context.Context, _ *probe) error {
				time.Sleep(2 * time.Second)
				return errBoom
			}
			runner, err := flow.New[probe]("g").
				Add("a", lateFail).
				Start("a").
				Edge("a", flow.End).
				Compile()
			if err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
			_, err = runner.Run(t.Context(), &probe{}, flow.WithBudget(time.Second), flow.WithRunID(testRunID))
			if !errors.Is(err, flow.ErrBudget) || errors.Is(err, errBoom) {
				t.Errorf("Run() error = %v, want %v without the node error", err, flow.ErrBudget)
			}
		})
	})

	t.Run("caller deadline is not reported as budget", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			calls := 0
			runner, err := flow.New[probe]("g").Add("a", blocking(&calls)).Start("a").Edge("a", flow.End).Compile()
			if err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			_, err = runner.Run(ctx, &probe{}, flow.WithRunID(testRunID))
			if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, flow.ErrBudget) {
				t.Errorf("Run() error = %v, want %v without ErrBudget", err, context.DeadlineExceeded)
			}
		})
	})
}

func TestRunnerRunRejectsBadInput(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		state *probe
		opts  []flow.RunOption
	}{
		{name: "nil state", state: nil},
		{name: "max steps below one", state: &probe{}, opts: []flow.RunOption{flow.WithMaxSteps(0)}},
		{name: "non-positive budget", state: &probe{}, opts: []flow.RunOption{flow.WithBudget(0)}},
		{name: "nil hook", state: &probe{}, opts: []flow.RunOption{flow.WithHook(nil)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runner, err := flow.New[probe]("g").Add("a", visit("a")).Start("a").Edge("a", flow.End).Compile()
			if err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
			res, err := runner.Run(t.Context(), tt.state, slices.Concat(tt.opts, []flow.RunOption{flow.WithRunID(testRunID)})...)
			if err == nil {
				t.Fatal("Run() error = nil, want error")
			}
			if res.Steps != 0 || res.Path != nil {
				t.Errorf("Run() result = %+v, want empty", res)
			}
		})
	}
}

func TestRunnerRunNodeError(t *testing.T) {
	t.Parallel()
	fail := func(_ context.Context, _ *probe) error { return errBoom }
	runner, err := flow.New[probe]("g").
		Add("a", visit("a")).Add("b", fail).
		Start("a").
		Edge("a", "b").Edge("b", flow.End).
		Compile()
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}

	var p probe
	res, err := runner.Run(t.Context(), &p, flow.WithRunID(testRunID))

	var nerr *flow.NodeError
	if !errors.As(err, &nerr) {
		t.Fatalf("Run() error = %v, want *NodeError", err)
	}
	if want := (flow.NodeError{RunID: testRunID, Node: "b", Step: 2, Attempt: 1, Err: errBoom}); *nerr != want {
		t.Errorf("Run() NodeError = %+v, want %+v", *nerr, want)
	}
	if !errors.Is(err, errBoom) {
		t.Errorf("errors.Is(err, errBoom) = false, want true")
	}
	if want := []string{"a"}; !slices.Equal(res.Path, want) {
		t.Errorf("Run() path = %v, want %v", res.Path, want)
	}
	if res.Steps != 1 {
		t.Errorf("Run() steps = %d, want 1", res.Steps)
	}
}

func TestRunnerRunRetry(t *testing.T) {
	t.Parallel()

	t.Run("succeeds on the second attempt", func(t *testing.T) {
		t.Parallel()
		calls := 0
		runner, err := flow.New[probe]("g").
			Add("a", failing(1, &calls), flow.WithRetry(3, 0)).
			Start("a").
			Edge("a", flow.End).
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		res, err := runner.Run(t.Context(), &probe{}, flow.WithRunID(testRunID))
		if err != nil {
			t.Fatalf("Run() error = %v, want nil", err)
		}
		if calls != 2 {
			t.Errorf("node calls = %d, want 2", calls)
		}
		if want := []string{"a"}; !slices.Equal(res.Path, want) {
			t.Errorf("Run() path = %v, want %v", res.Path, want)
		}
	})

	t.Run("waits the backoff between attempts", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			calls := 0
			runner, err := flow.New[probe]("g").
				Add("a", failing(1, &calls), flow.WithRetry(2, time.Second)).
				Start("a").
				Edge("a", flow.End).
				Compile()
			if err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
			start := time.Now()
			if _, err := runner.Run(t.Context(), &probe{}, flow.WithRunID(testRunID)); err != nil {
				t.Fatalf("Run() error = %v, want nil", err)
			}
			if got := time.Since(start); got != time.Second {
				t.Errorf("elapsed = %v, want %v", got, time.Second)
			}
			if calls != 2 {
				t.Errorf("node calls = %d, want 2", calls)
			}
		})
	})

	t.Run("gives up after the configured attempts", func(t *testing.T) {
		t.Parallel()
		calls := 0
		runner, err := flow.New[probe]("g").
			Add("a", failing(5, &calls), flow.WithRetry(2, 0)).
			Start("a").
			Edge("a", flow.End).
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		_, err = runner.Run(t.Context(), &probe{}, flow.WithRunID(testRunID))
		var nerr *flow.NodeError
		if !errors.As(err, &nerr) {
			t.Fatalf("Run() error = %v, want *NodeError", err)
		}
		if nerr.Attempt != 2 || calls != 2 {
			t.Errorf("attempt = %d, calls = %d, want 2 and 2", nerr.Attempt, calls)
		}
	})

	t.Run("cancellation during backoff returns the context error", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			calls := 0
			runner, err := flow.New[probe]("g").
				Add("a", failing(5, &calls), flow.WithRetry(3, time.Hour)).
				Start("a").
				Edge("a", flow.End).
				Compile()
			if err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			go func() {
				time.Sleep(10 * time.Millisecond)
				cancel()
			}()

			start := time.Now()
			_, err = runner.Run(ctx, &probe{}, flow.WithRunID(testRunID))
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
			}
			var nerr *flow.NodeError
			if errors.As(err, &nerr) {
				t.Errorf("Run() error = %v, want context error without *NodeError", err)
			}
			if got := time.Since(start); got != 10*time.Millisecond {
				t.Errorf("elapsed = %v, want %v", got, 10*time.Millisecond)
			}
			if calls != 1 {
				t.Errorf("node calls = %d, want 1", calls)
			}
		})
	})
}

func TestRunnerRunTimeout(t *testing.T) {
	t.Parallel()

	t.Run("node timeout is wrapped in NodeError", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			calls := 0
			runner, err := flow.New[probe]("g").
				Add("a", blocking(&calls), flow.WithTimeout(time.Second)).
				Start("a").
				Edge("a", flow.End).
				Compile()
			if err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
			start := time.Now()
			_, err = runner.Run(t.Context(), &probe{}, flow.WithRunID(testRunID))
			var nerr *flow.NodeError
			if !errors.As(err, &nerr) {
				t.Fatalf("Run() error = %v, want *NodeError", err)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("errors.Is(err, DeadlineExceeded) = false, want true")
			}
			if nerr.Node != "a" || nerr.Attempt != 1 {
				t.Errorf("NodeError = %+v, want node a attempt 1", *nerr)
			}
			if got := time.Since(start); got != time.Second {
				t.Errorf("elapsed = %v, want %v", got, time.Second)
			}
			if t.Context().Err() != nil {
				t.Errorf("run context canceled = %v, want nil", t.Context().Err())
			}
		})
	})

	t.Run("timeout applies to each attempt", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			calls := 0
			runner, err := flow.New[probe]("g").
				Add("a", blocking(&calls), flow.WithTimeout(time.Second), flow.WithRetry(2, 0)).
				Start("a").
				Edge("a", flow.End).
				Compile()
			if err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
			start := time.Now()
			_, err = runner.Run(t.Context(), &probe{}, flow.WithRunID(testRunID))
			var nerr *flow.NodeError
			if !errors.As(err, &nerr) {
				t.Fatalf("Run() error = %v, want *NodeError", err)
			}
			if nerr.Attempt != 2 || calls != 2 {
				t.Errorf("attempt = %d, calls = %d, want 2 and 2", nerr.Attempt, calls)
			}
			if got := time.Since(start); got != 2*time.Second {
				t.Errorf("elapsed = %v, want %v", got, 2*time.Second)
			}
		})
	})
}

func TestRunnerRunCancellation(t *testing.T) {
	t.Parallel()

	t.Run("cancellation reaches the running node", func(t *testing.T) {
		t.Parallel()
		entered := make(chan struct{})
		wait := func(ctx context.Context, _ *probe) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}
		runner, err := flow.New[probe]("g").Add("a", wait).Start("a").Edge("a", flow.End).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			<-entered
			cancel()
		}()

		res, err := runner.Run(ctx, &probe{}, flow.WithRunID(testRunID))
		<-done
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
		}
		var nerr *flow.NodeError
		if errors.As(err, &nerr) {
			t.Errorf("Run() error = %v, want context error without *NodeError", err)
		}
		if res.Path != nil || res.Steps != 0 {
			t.Errorf("Run() result = %+v, want empty", res)
		}
	})

	t.Run("already canceled context runs nothing", func(t *testing.T) {
		t.Parallel()
		runner, err := flow.New[probe]("g").Add("a", visit("a")).Start("a").Edge("a", flow.End).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		var p probe
		res, err := runner.Run(ctx, &p, flow.WithRunID(testRunID))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
		}
		if visited := p.seen(); visited != nil || res.Path != nil {
			t.Errorf("visited = %v, path = %v, want none", visited, res.Path)
		}
	})

	t.Run("node error under canceled context is reported as the context error", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		calls := 0
		failAndCancel := func(_ context.Context, _ *probe) error {
			calls++
			cancel()
			return errBoom
		}
		runner, err := flow.New[probe]("g").
			Add("a", failAndCancel, flow.WithRetry(3, 0)).
			Start("a").
			Edge("a", flow.End).
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		_, err = runner.Run(ctx, &probe{}, flow.WithRunID(testRunID))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
		}
		if errors.Is(err, errBoom) {
			t.Errorf("Run() error = %v, want context error without the node error", err)
		}
		if calls != 1 {
			t.Errorf("node calls = %d, want 1 (no retry after cancellation)", calls)
		}
	})
}

func TestRunnerRunConcurrent(t *testing.T) {
	t.Parallel()
	runner, err := diamond().Compile()
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	const runs = 1000
	want := []string{"a", "b", "c", "d"}
	var failures atomic.Int32
	var wg sync.WaitGroup
	for range runs {
		wg.Go(func() {
			var p probe
			res, err := runner.Run(t.Context(), &p, flow.WithRunID(testRunID))
			if err != nil || !slices.Equal(res.Path, want) {
				failures.Add(1)
			}
		})
	}
	wg.Wait()
	if n := failures.Load(); n != 0 {
		t.Errorf("concurrent runs with wrong result = %d of %d, want 0", n, runs)
	}
}
