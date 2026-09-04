package flow_test

import (
	"context"
	"errors"
	"slices"
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
			name: "frontier keeps edge declaration order",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).Add("c", visit("c")).
					Start("a").
					Edge("a", "c").Edge("a", "b").
					Edge("b", flow.End).Edge("c", flow.End)
			},
			wantPath:  []string{"a", "c", "b"},
			wantSteps: 2,
		},
		{
			name: "branch loops back and then routes to end",
			graph: func() *flow.Graph[probe] {
				again := func(_ context.Context, p *probe) []string {
					if len(p.visited) < 3 {
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
			name: "router fan-out runs targets in the order returned",
			graph: func() *flow.Graph[probe] {
				return flow.New[probe]("g").
					Add("a", visit("a")).Add("b", visit("b")).Add("c", visit("c")).
					Start("a").
					Branch("a", fixed("c", "b"), "b", "c").
					Edge("b", flow.End).Edge("c", flow.End)
			},
			wantPath:  []string{"a", "c", "b"},
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
			res, err := runner.Run(t.Context(), &p, tt.opts...)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Run() error = %v, want %v", err, tt.wantErr)
			}
			if !slices.Equal(res.Path, tt.wantPath) {
				t.Errorf("Run() path = %v, want %v", res.Path, tt.wantPath)
			}
			if !slices.Equal(p.visited, tt.wantPath) {
				t.Errorf("state visited = %v, want %v", p.visited, tt.wantPath)
			}
			if res.Steps != tt.wantSteps {
				t.Errorf("Run() steps = %d, want %d", res.Steps, tt.wantSteps)
			}
		})
	}
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runner, err := flow.New[probe]("g").Add("a", visit("a")).Start("a").Edge("a", flow.End).Compile()
			if err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
			res, err := runner.Run(t.Context(), tt.state, tt.opts...)
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
	res, err := runner.Run(t.Context(), &p)

	var nerr *flow.NodeError
	if !errors.As(err, &nerr) {
		t.Fatalf("Run() error = %v, want *NodeError", err)
	}
	if want := (flow.NodeError{Node: "b", Step: 2, Attempt: 1, Err: errBoom}); *nerr != want {
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
		res, err := runner.Run(t.Context(), &probe{})
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
			if _, err := runner.Run(t.Context(), &probe{}); err != nil {
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
		_, err = runner.Run(t.Context(), &probe{})
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
			_, err = runner.Run(ctx, &probe{})
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
			_, err = runner.Run(t.Context(), &probe{})
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
			_, err = runner.Run(t.Context(), &probe{})
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

		res, err := runner.Run(ctx, &probe{})
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
		res, err := runner.Run(ctx, &p)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
		}
		if p.visited != nil || res.Path != nil {
			t.Errorf("visited = %v, path = %v, want none", p.visited, res.Path)
		}
	})

	t.Run("node error under canceled context is reported as the context error", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		failAndCancel := func(_ context.Context, _ *probe) error {
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
		_, err = runner.Run(ctx, &probe{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
		}
		if errors.Is(err, errBoom) {
			t.Errorf("Run() error = %v, want context error without the node error", err)
		}
	})
}
