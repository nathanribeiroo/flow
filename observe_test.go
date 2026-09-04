package flow_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nathanribeiroo/flow"
)

// ctxKey é a chave que cada recorder grava no contexto no NodeStart.
type ctxKey string

// journal é o registro compartilhado entre hooks. Tem mutex porque nós
// irmãos rodam em paralelo.
type journal struct {
	mu      sync.Mutex
	entries []string
}

func (j *journal) add(entry string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries = append(j.entries, entry)
}

func (j *journal) all() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return slices.Clone(j.entries)
}

// recorder é um Hook que registra cada chamada no journal com o nome do
// hook, o nó, a tentativa e quais chaves de contexto enxergou.
type recorder struct {
	name string
	j    *journal
}

var _ flow.Hook = (*recorder)(nil)

func (h *recorder) NodeStart(ctx context.Context, info flow.NodeInfo) context.Context {
	h.j.add(fmt.Sprintf("%s start %s#%d ctx=%s", h.name, info.Node, info.Attempt, seenKeys(ctx)))
	return context.WithValue(ctx, ctxKey(h.name), true)
}

func (h *recorder) NodeEnd(ctx context.Context, info flow.NodeInfo, err error) {
	h.j.add(fmt.Sprintf("%s end %s#%d ctx=%s err=%v", h.name, info.Node, info.Attempt, seenKeys(ctx), err))
}

// seenKeys lista as chaves h1 e h2 presentes em ctx.
func seenKeys(ctx context.Context) string {
	var keys []string
	for _, k := range []string{"h1", "h2"} {
		if ctx.Value(ctxKey(k)) != nil {
			keys = append(keys, k)
		}
	}
	return strings.Join(keys, "+")
}

// kindName traduz EventKind para os testes.
var kindName = map[flow.EventKind]string{
	flow.EventNodeStart: "node-start",
	flow.EventNodeEnd:   "node-end",
	flow.EventStepEnd:   "step-end",
	flow.EventRunEnd:    "run-end",
}

// collect fecha ch, que é do teste, e devolve os eventos em forma legível.
func collect(ch chan flow.Event) []string {
	close(ch)
	var out []string
	for ev := range ch {
		out = append(out, fmt.Sprintf("%s %s step=%d err=%v", kindName[ev.Kind], ev.Info.Node, ev.Info.Step, ev.Err))
	}
	return out
}

func TestRunnerRunHooks(t *testing.T) {
	t.Parallel()

	t.Run("hooks nest like defer and receive their own context", func(t *testing.T) {
		t.Parallel()
		j := &journal{}
		var nodeSaw string
		node := func(ctx context.Context, _ *probe) error {
			nodeSaw = seenKeys(ctx)
			return nil
		}
		runner, err := flow.New[probe]("g").Add("a", node).Start("a").Edge("a", flow.End).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		_, err = runner.Run(t.Context(), &probe{}, flow.WithRunID(testRunID),
			flow.WithHook(&recorder{name: "h1", j: j}), flow.WithHook(&recorder{name: "h2", j: j}))
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		want := []string{
			"h1 start a#1 ctx=",
			"h2 start a#1 ctx=h1",
			"h2 end a#1 ctx=h1+h2 err=<nil>",
			"h1 end a#1 ctx=h1 err=<nil>",
		}
		if got := j.all(); !slices.Equal(got, want) {
			t.Errorf("hook calls = %q, want %q", got, want)
		}
		if nodeSaw != "h1+h2" {
			t.Errorf("node context keys = %q, want %q", nodeSaw, "h1+h2")
		}
	})

	t.Run("hooks fire once per attempt", func(t *testing.T) {
		t.Parallel()
		j := &journal{}
		calls := 0
		runner, err := flow.New[probe]("g").
			Add("a", failing(2, &calls), flow.WithRetry(3, 0)).
			Start("a").
			Edge("a", flow.End).
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		_, err = runner.Run(t.Context(), &probe{}, flow.WithRunID(testRunID), flow.WithHook(&recorder{name: "h1", j: j}))
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		want := []string{
			"h1 start a#1 ctx=",
			"h1 end a#1 ctx=h1 err=boom",
			"h1 start a#2 ctx=",
			"h1 end a#2 ctx=h1 err=boom",
			"h1 start a#3 ctx=",
			"h1 end a#3 ctx=h1 err=<nil>",
		}
		if got := j.all(); !slices.Equal(got, want) {
			t.Errorf("hook calls = %q, want %q", got, want)
		}
	})

	t.Run("node info carries run graph version and step", func(t *testing.T) {
		t.Parallel()
		var infos []flow.NodeInfo
		capture := hookFunc(func(_ context.Context, info flow.NodeInfo) { infos = append(infos, info) })
		runner, err := flow.New[probe]("g").
			Add("a", visit("a")).Add("b", visit("b")).
			Start("a").
			Edge("a", "b").Edge("b", flow.End).
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		if _, err := runner.Run(t.Context(), &probe{}, flow.WithRunID(testRunID), flow.WithHook(capture)); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		want := []flow.NodeInfo{
			{RunID: testRunID, Graph: "g", Version: runner.Version(), Node: "a", Step: 1, Attempt: 1},
			{RunID: testRunID, Graph: "g", Version: runner.Version(), Node: "b", Step: 2, Attempt: 1},
		}
		if !slices.Equal(infos, want) {
			t.Errorf("NodeStart infos = %+v, want %+v", infos, want)
		}
	})

	t.Run("panic in a hook is not recovered", func(t *testing.T) {
		t.Parallel()
		runner, err := flow.New[probe]("g").Add("a", visit("a")).Start("a").Edge("a", flow.End).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		defer func() {
			if got := recover(); got != "hook exploded" {
				t.Errorf("recovered = %v, want the hook panic", got)
			}
		}()
		_, _ = runner.Run(t.Context(), &probe{}, flow.WithRunID(testRunID), flow.WithHook(panicHook{}))
		t.Fatal("Run() returned, want panic")
	})
}

// hookFunc adapta uma função a Hook, registrando só o NodeStart.
type hookFunc func(ctx context.Context, info flow.NodeInfo)

func (f hookFunc) NodeStart(ctx context.Context, info flow.NodeInfo) context.Context {
	f(ctx, info)
	return ctx
}

func (hookFunc) NodeEnd(context.Context, flow.NodeInfo, error) {}

// panicHook explode no NodeStart.
type panicHook struct{}

func (panicHook) NodeStart(context.Context, flow.NodeInfo) context.Context { panic("hook exploded") }

func (panicHook) NodeEnd(context.Context, flow.NodeInfo, error) {}

func TestRunnerRunEvents(t *testing.T) {
	t.Parallel()

	t.Run("events follow the run", func(t *testing.T) {
		t.Parallel()
		ch := make(chan flow.Event, 32)
		runner, err := flow.New[probe]("g").
			Add("a", visit("a")).Add("b", visit("b")).
			Start("a").
			Edge("a", "b").Edge("b", flow.End).
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		before := time.Now()
		if _, err := runner.Run(t.Context(), &probe{}, flow.WithRunID(testRunID), flow.WithEvents(ch)); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		first := <-ch
		if first.At.Before(before) || first.Info.RunID != testRunID || first.Info.Version != runner.Version() {
			t.Errorf("first event = %+v, want At after start and run id and version stamped", first)
		}
		want := []string{
			"node-end a step=1 err=<nil>",
			"step-end  step=1 err=<nil>",
			"node-start b step=2 err=<nil>",
			"node-end b step=2 err=<nil>",
			"step-end  step=2 err=<nil>",
			"run-end  step=2 err=<nil>",
		}
		if got := collect(ch); !slices.Equal(got, want) {
			t.Errorf("events = %q, want %q", got, want)
		}
	})

	t.Run("failed step emits run end but no step end", func(t *testing.T) {
		t.Parallel()
		ch := make(chan flow.Event, 32)
		fail := func(_ context.Context, _ *probe) error { return errBoom }
		runner, err := flow.New[probe]("g").Add("a", fail).Start("a").Edge("a", flow.End).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		_, err = runner.Run(t.Context(), &probe{}, flow.WithRunID(testRunID), flow.WithEvents(ch))
		if !errors.Is(err, errBoom) {
			t.Fatalf("Run() error = %v, want %v", err, errBoom)
		}
		got := collect(ch)
		want := []string{
			"node-start a step=1 err=<nil>",
			"node-end a step=1 err=boom",
			"run-end  step=0 err=" + err.Error(),
		}
		if !slices.Equal(got, want) {
			t.Errorf("events = %q, want %q", got, want)
		}
	})

	t.Run("slow consumer loses events without blocking the run", func(t *testing.T) {
		t.Parallel()
		ch := make(chan flow.Event, 1)
		runner, err := flow.New[probe]("g").
			Add("a", visit("a")).Add("b", visit("b")).Add("c", visit("c")).
			Start("a").
			Edge("a", "b").Edge("b", "c").Edge("c", flow.End).
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		res, err := runner.Run(t.Context(), &probe{}, flow.WithRunID(testRunID), flow.WithEvents(ch))
		if err != nil || res.Steps != 3 {
			t.Fatalf("Run() = %+v, %v, want 3 steps and nil", res, err)
		}
		if got, want := collect(ch), []string{"node-start a step=1 err=<nil>"}; !slices.Equal(got, want) {
			t.Errorf("events kept = %q, want only the first %q", got, want)
		}
	})

	t.Run("detached node reports through hook and events", func(t *testing.T) {
		t.Parallel()
		j := &journal{}
		ch := make(chan flow.Event, 32)
		fail := func(_ context.Context, _ *probe) error { return errBoom }
		runner, err := flow.New[probe]("g").
			Add("a", visit("a")).
			Add("audit", fail, flow.WithTimeout(time.Second)).
			Start("a").
			Edge("a", flow.End).
			Detach("a", "audit").
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		res, err := runner.Run(t.Context(), &probe{}, flow.WithRunID(testRunID),
			flow.WithHook(&recorder{name: "h1", j: j}), flow.WithEvents(ch))
		if err != nil || len(res.Errors) != 0 {
			t.Fatalf("Run() = %+v, %v, want nil error and no Errors", res, err)
		}
		if err := runner.Wait(t.Context()); err != nil {
			t.Fatalf("Wait() error = %v", err)
		}
		if got := j.all(); !slices.Contains(got, "h1 end audit#1 ctx=h1 err=boom") {
			t.Errorf("hook calls = %q, want the detached failure", got)
		}
		if got := collect(ch); !slices.Contains(got, "node-end audit step=1 err=boom") {
			t.Errorf("events = %q, want the detached failure", got)
		}
	})
}

func TestRunnerRunID(t *testing.T) {
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

	t.Run("generated when not provided", func(t *testing.T) {
		t.Parallel()
		first, _ := runner.Run(t.Context(), &probe{})
		second, _ := runner.Run(t.Context(), &probe{})
		if len(first.RunID) != 32 || len(second.RunID) != 32 || first.RunID == second.RunID {
			t.Errorf("generated run ids = %q, %q, want two distinct 32-hex ids", first.RunID, second.RunID)
		}
	})

	t.Run("stamped on result and node error", func(t *testing.T) {
		t.Parallel()
		res, err := runner.Run(t.Context(), &probe{}, flow.WithRunID("run-42"))
		var nerr *flow.NodeError
		if !errors.As(err, &nerr) {
			t.Fatalf("Run() error = %v, want *NodeError", err)
		}
		if res.RunID != "run-42" || nerr.RunID != "run-42" || res.Version != runner.Version() {
			t.Errorf("result = %+v, node error = %+v, want run-42 and version %q", res, *nerr, runner.Version())
		}
	})
}
