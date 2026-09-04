package flow_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/nathanribeiroo/flow"
)

// ledger é o estado dos testes de checkpoint: um contador exportado por nó,
// porque cada nó escreve só no campo dele e o JSON precisa enxergar.
type ledger struct {
	A, B, C, D, E int
}

// bump devolve um nó que incrementa o campo escolhido por field.
func bump(field func(*ledger) *int) flow.Node[ledger] {
	return func(_ context.Context, l *ledger) error {
		*field(l)++
		return nil
	}
}

var (
	bumpA = bump(func(l *ledger) *int { return &l.A })
	bumpB = bump(func(l *ledger) *int { return &l.B })
	bumpC = bump(func(l *ledger) *int { return &l.C })
	bumpD = bump(func(l *ledger) *int { return &l.D })
	bumpE = bump(func(l *ledger) *int { return &l.E })
)

// failingStore é um Checkpointer cujo Save sempre falha.
type failingStore struct{ err error }

func (s failingStore) Save(context.Context, flow.Checkpoint) error { return s.err }

func (failingStore) Load(context.Context, string) (flow.Checkpoint, error) {
	return flow.Checkpoint{}, flow.ErrCheckpointNotFound
}

// fixedStore devolve o mesmo checkpoint para qualquer id.
type fixedStore struct{ cp flow.Checkpoint }

func (fixedStore) Save(context.Context, flow.Checkpoint) error { return nil }

func (s fixedStore) Load(context.Context, string) (flow.Checkpoint, error) { return s.cp, nil }

// cancelingStore grava em inner e cancela o contexto logo depois de gravar o
// checkpoint do passo at: a queda cai exatamente entre dois supersteps.
type cancelingStore struct {
	inner  flow.Checkpointer
	at     int
	cancel context.CancelFunc
}

func (s cancelingStore) Save(ctx context.Context, cp flow.Checkpoint) error {
	if err := s.inner.Save(ctx, cp); err != nil {
		return err
	}
	if cp.Step == s.at {
		s.cancel()
	}
	return nil
}

func (s cancelingStore) Load(ctx context.Context, runID string) (flow.Checkpoint, error) {
	return s.inner.Load(ctx, runID)
}

// unbalanced monta a -> {b, c}, b -> d, c -> e -> d, d -> End, com e dado
// pelo chamador: é o grafo em que d fica com chegada parcial esperando e.
func unbalanced(e flow.Node[ledger]) *flow.Graph[ledger] {
	return flow.New[ledger]("g").
		Add("a", bumpA).Add("b", bumpB).Add("c", bumpC).
		Add("d", bumpD).Add("e", e).
		Start("a").
		Edge("a", "b").Edge("a", "c").
		Edge("b", "d").Edge("c", "e").Edge("e", "d").
		Edge("d", flow.End)
}

func TestMemoryStore(t *testing.T) {
	t.Parallel()

	t.Run("round trip copies the state in both directions", func(t *testing.T) {
		t.Parallel()
		store := flow.NewMemoryStore()
		raw := []byte(`{"n":1}`)
		cp := flow.Checkpoint{RunID: "r", Version: "v", Step: 2, Pending: []string{"d"}, State: raw}
		if err := store.Save(t.Context(), cp); err != nil {
			t.Fatalf("Save() error = %v", err)
		}
		raw[5] = '9' // o chamador altera o slice depois de gravar

		got, err := store.Load(t.Context(), "r")
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if string(got.State) != `{"n":1}` || got.Step != 2 || !slices.Equal(got.Pending, []string{"d"}) {
			t.Errorf("Load() = %+v, want the checkpoint as saved", got)
		}
		got.State[5] = '7' // e altera o que carregou
		again, err := store.Load(t.Context(), "r")
		if err != nil || string(again.State) != `{"n":1}` {
			t.Errorf("second Load() = %s, %v, want the stored copy untouched", again.State, err)
		}
	})

	t.Run("unknown id is not found", func(t *testing.T) {
		t.Parallel()
		_, err := flow.NewMemoryStore().Load(t.Context(), "nope")
		if !errors.Is(err, flow.ErrCheckpointNotFound) {
			t.Errorf("Load() error = %v, want %v", err, flow.ErrCheckpointNotFound)
		}
	})
}

func TestRunnerResume(t *testing.T) {
	t.Parallel()

	t.Run("crash with a partial arrival resumes and runs the join once", func(t *testing.T) {
		t.Parallel()
		store := flow.NewMemoryStore()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var eCalls atomic.Int32
		e := func(ctx context.Context, l *ledger) error {
			if eCalls.Add(1) == 1 {
				cancel() // o processo cai no meio do passo 3, com d esperando e
				return ctx.Err()
			}
			return bumpE(ctx, l)
		}
		runner, err := unbalanced(e).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}

		var state ledger
		res, err := runner.Run(ctx, &state, flow.WithRunID("crash"), flow.WithCheckpoint(store))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
		}
		if want := []string{"a", "b", "c"}; !slices.Equal(res.Path, want) || res.Steps != 2 {
			t.Fatalf("Run() path = %v, steps = %d, want %v and 2", res.Path, res.Steps, want)
		}
		cp, err := store.Load(t.Context(), "crash")
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cp.Step != 2 || !slices.Equal(cp.Pending, []string{"d", "e"}) || cp.Version != runner.Version() {
			t.Fatalf("checkpoint = %+v, want step 2 with d and e pending", cp)
		}

		var steps []int
		trace := hookFunc(func(_ context.Context, info flow.NodeInfo) { steps = append(steps, info.Step) })
		resumed, res, err := runner.Resume(t.Context(), "crash", flow.WithCheckpoint(store), flow.WithHook(trace))
		if err != nil {
			t.Fatalf("Resume() error = %v", err)
		}
		if want := []string{"e", "d"}; !slices.Equal(res.Path, want) || res.Steps != 4 || res.RunID != "crash" {
			t.Errorf("Resume() result = %+v, want path %v, 4 steps and run id crash", res, want)
		}
		if want := (ledger{A: 1, B: 1, C: 1, D: 1, E: 1}); *resumed != want {
			t.Errorf("resumed state = %+v, want every node once: %+v", *resumed, want)
		}
		if eCalls.Load() != 2 {
			t.Errorf("e ran %d times, want 2: the interrupted attempt and the resumed one", eCalls.Load())
		}
		if want := []int{3, 4}; !slices.Equal(steps, want) {
			t.Errorf("resumed node steps = %v, want %v", steps, want)
		}
		final, err := store.Load(t.Context(), "crash")
		if err != nil || final.Step != 4 || len(final.Pending) != 0 {
			t.Errorf("terminal checkpoint = %+v, %v, want step 4 with nothing pending", final, err)
		}
	})

	t.Run("crash between supersteps runs no node of the next step", func(t *testing.T) {
		t.Parallel()
		inner := flow.NewMemoryStore()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		store := cancelingStore{inner: inner, at: 2, cancel: cancel}
		var eCalls atomic.Int32
		e := func(ctx context.Context, l *ledger) error {
			eCalls.Add(1)
			return bumpE(ctx, l)
		}
		runner, err := unbalanced(e).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}

		res, err := runner.Run(ctx, &ledger{}, flow.WithRunID("between"), flow.WithCheckpoint(store))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
		}
		if want := []string{"a", "b", "c"}; !slices.Equal(res.Path, want) || res.Steps != 2 || eCalls.Load() != 0 {
			t.Fatalf("Run() path = %v, steps = %d, e calls = %d, want %v, 2 and 0", res.Path, res.Steps, eCalls.Load(), want)
		}

		resumed, res, err := runner.Resume(t.Context(), "between", flow.WithCheckpoint(inner))
		if err != nil {
			t.Fatalf("Resume() error = %v", err)
		}
		if want := []string{"e", "d"}; !slices.Equal(res.Path, want) || res.Steps != 4 {
			t.Errorf("Resume() result = %+v, want path %v and 4 steps", res, want)
		}
		if want := (ledger{A: 1, B: 1, C: 1, D: 1, E: 1}); *resumed != want || eCalls.Load() != 1 {
			t.Errorf("resumed state = %+v with %d e calls, want %+v and 1", *resumed, eCalls.Load(), want)
		}
	})

	t.Run("terminal checkpoint resumes without running anything", func(t *testing.T) {
		t.Parallel()
		store := flow.NewMemoryStore()
		var calls atomic.Int32
		count := func(ctx context.Context, l *ledger) error {
			calls.Add(1)
			return bumpA(ctx, l)
		}
		runner, err := flow.New[ledger]("g").Add("a", count).Start("a").Edge("a", flow.End).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		if _, err := runner.Run(t.Context(), &ledger{}, flow.WithRunID("done"), flow.WithCheckpoint(store)); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		events := make(chan flow.Event, 8)
		state, res, err := runner.Resume(t.Context(), "done", flow.WithCheckpoint(store), flow.WithEvents(events))
		if err != nil {
			t.Fatalf("Resume() error = %v", err)
		}
		if calls.Load() != 1 || res.Steps != 1 || res.Path != nil || state.A != 1 {
			t.Errorf("Resume() = %+v, %+v with %d calls, want the final state, 1 step, no path and 1 call", *state, res, calls.Load())
		}
		if got, want := collect(events), []string{"run-end  step=1 err=<nil>"}; !slices.Equal(got, want) {
			t.Errorf("events = %q, want %q", got, want)
		}
	})

	t.Run("max steps counts from the checkpoint", func(t *testing.T) {
		t.Parallel()
		store := flow.NewMemoryStore()
		runner, err := flow.New[ledger]("g").
			Add("a", bumpA).Add("b", bumpB).
			Start("a").
			Edge("a", "b").Edge("b", "a").
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		_, err = runner.Run(t.Context(), &ledger{}, flow.WithRunID("loop"), flow.WithCheckpoint(store), flow.WithMaxSteps(4))
		if !errors.Is(err, flow.ErrMaxSteps) {
			t.Fatalf("Run() error = %v, want %v", err, flow.ErrMaxSteps)
		}
		state, res, err := runner.Resume(t.Context(), "loop", flow.WithCheckpoint(store), flow.WithMaxSteps(6))
		if !errors.Is(err, flow.ErrMaxSteps) {
			t.Fatalf("Resume() error = %v, want %v", err, flow.ErrMaxSteps)
		}
		if res.Steps != 6 || len(res.Path) != 2 || state.A != 3 || state.B != 3 {
			t.Errorf("Resume() = %+v with state %+v, want 6 steps, 2 in this path and 3 runs of each node", res, *state)
		}
	})

	t.Run("version mismatch names both versions", func(t *testing.T) {
		t.Parallel()
		store := flow.NewMemoryStore()
		build := func(opts ...flow.NodeOption) *flow.Runner[ledger] {
			t.Helper()
			runner, err := flow.New[ledger]("g").Add("a", bumpA, opts...).Start("a").Edge("a", flow.End).Compile()
			if err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
			return runner
		}
		old, changed := build(), build(flow.WithOnFailure(flow.FailureSkip))
		if _, err := old.Run(t.Context(), &ledger{}, flow.WithRunID("v"), flow.WithCheckpoint(store)); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		_, _, err := changed.Resume(t.Context(), "v", flow.WithCheckpoint(store))
		if !errors.Is(err, flow.ErrVersionMismatch) {
			t.Fatalf("Resume() error = %v, want %v", err, flow.ErrVersionMismatch)
		}
		if !strings.Contains(err.Error(), old.Version()) || !strings.Contains(err.Error(), changed.Version()) {
			t.Errorf("Resume() error = %q, want both versions %s and %s", err, old.Version(), changed.Version())
		}
	})

	t.Run("save failure aborts the run after the step counted", func(t *testing.T) {
		t.Parallel()
		errStore := errors.New("disk full")
		events := make(chan flow.Event, 16)
		runner, err := flow.New[ledger]("g").
			Add("a", bumpA).Add("b", bumpB).
			Start("a").
			Edge("a", "b").Edge("b", flow.End).
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		res, err := runner.Run(t.Context(), &ledger{}, flow.WithRunID(testRunID),
			flow.WithCheckpoint(failingStore{err: errStore}), flow.WithEvents(events))
		if !errors.Is(err, errStore) || !strings.HasPrefix(err.Error(), "flow: saving checkpoint at step 1:") {
			t.Fatalf("Run() error = %v, want the store error wrapped at step 1", err)
		}
		if res.Steps != 1 || !slices.Equal(res.Path, []string{"a"}) {
			t.Errorf("Run() result = %+v, want step 1 counted and path [a]", res)
		}
		want := []string{
			"node-start a step=1 err=<nil>",
			"node-end a step=1 err=<nil>",
			"run-end  step=1 err=" + err.Error(),
		}
		if got := collect(events); !slices.Equal(got, want) {
			t.Errorf("events = %q, want %q without step-end", got, want)
		}
	})

	t.Run("pending node that does not exist fails before running", func(t *testing.T) {
		t.Parallel()
		store := flow.NewMemoryStore()
		var calls atomic.Int32
		count := func(ctx context.Context, l *ledger) error {
			calls.Add(1)
			return bumpA(ctx, l)
		}
		runner, err := flow.New[ledger]("g").Add("a", count).Start("a").Edge("a", flow.End).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		cp := flow.Checkpoint{RunID: "ghost", Version: runner.Version(), Step: 1, Pending: []string{"a", "zz"}, State: json.RawMessage(`{}`)}
		if err := store.Save(t.Context(), cp); err != nil {
			t.Fatalf("Save() error = %v", err)
		}
		_, _, err = runner.Resume(t.Context(), "ghost", flow.WithCheckpoint(store))
		if err == nil || !strings.Contains(err.Error(), `"zz"`) {
			t.Fatalf("Resume() error = %v, want the unknown pending node named", err)
		}
		if calls.Load() != 0 {
			t.Errorf("node calls = %d, want 0", calls.Load())
		}
	})

	t.Run("bad input is rejected inline", func(t *testing.T) {
		t.Parallel()
		store := flow.NewMemoryStore()
		runner, err := flow.New[ledger]("g").Add("a", bumpA).Start("a").Edge("a", flow.End).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		foreign := fixedStore{cp: flow.Checkpoint{RunID: "someone-else", Version: runner.Version(), State: json.RawMessage(`{}`)}}
		tests := []struct {
			name string
			opts []flow.RunOption
			want error
		}{
			{name: "checkpoint not found", opts: []flow.RunOption{flow.WithCheckpoint(store)}, want: flow.ErrCheckpointNotFound},
			{name: "checkpoint of another run", opts: []flow.RunOption{flow.WithCheckpoint(foreign)}},
			{name: "with run id", opts: []flow.RunOption{flow.WithCheckpoint(store), flow.WithRunID("x")}},
			{name: "without checkpoint store"},
			{name: "nil hook", opts: []flow.RunOption{flow.WithCheckpoint(store), flow.WithHook(nil)}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				state, res, err := runner.Resume(t.Context(), "missing", tt.opts...)
				if err == nil || (tt.want != nil && !errors.Is(err, tt.want)) {
					t.Fatalf("Resume() error = %v, want an error matching %v", err, tt.want)
				}
				if state != nil || res.Steps != 0 {
					t.Errorf("Resume() = %v, %+v, want nil state and empty result", state, res)
				}
			})
		}
	})
}

// opaque é um estado que o JSON não serializa.
type opaque struct{ C chan int }

func TestRunnerRunCheckpointRejectsUnserializableState(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	node := func(context.Context, *opaque) error {
		calls.Add(1)
		return nil
	}
	runner, err := flow.New[opaque]("g").Add("a", node).Start("a").Edge("a", flow.End).Compile()
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	events := make(chan flow.Event, 8)
	_, err = runner.Run(t.Context(), &opaque{C: make(chan int)}, flow.WithRunID(testRunID),
		flow.WithCheckpoint(flow.NewMemoryStore()), flow.WithEvents(events))
	if err == nil || !strings.HasPrefix(err.Error(), "flow: state is not serializable:") {
		t.Fatalf("Run() error = %v, want the inline serialization error", err)
	}
	if calls.Load() != 0 || len(collect(events)) != 0 {
		t.Errorf("node calls = %d, events = %d, want none before the inline failure", calls.Load(), len(events))
	}
}
