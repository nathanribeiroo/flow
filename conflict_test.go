package flow_test

import (
	"context"
	"errors"
	"math"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nathanribeiroo/flow"
)

// doc é o estado dos testes de conflito: campos exportados de primeiro nível
// e um aninhado, para o JSON enxergar.
type doc struct {
	Docs   []string
	KB     string
	Web    string
	Nested struct{ X, Y int }
}

// write devolve um nó que aplica fn ao estado.
func write(fn func(*doc)) flow.Node[doc] {
	return func(_ context.Context, d *doc) error {
		fn(d)
		return nil
	}
}

// fanOut monta a -> {b, c} -> End com os nós b e c dados.
func fanOut(b, c flow.Node[doc], opts ...flow.NodeOption) *flow.Graph[doc] {
	return flow.New[doc]("g").
		Add("a", write(func(*doc) {})).
		Add("b", b, opts...).
		Add("c", c, opts...).
		Start("a").
		Edge("a", "b").Edge("a", "c").
		Edge("b", flow.End).Edge("c", flow.End)
}

func TestRunnerRunConflictCheck(t *testing.T) {
	t.Parallel()
	appendDoc := func(v string) flow.Node[doc] {
		return write(func(d *doc) { d.Docs = append(d.Docs, v) })
	}

	t.Run("two nodes writing the same field abort with ConflictError", func(t *testing.T) {
		t.Parallel()
		var audits atomic.Int32
		audit := func(_ context.Context, _ *doc) error {
			audits.Add(1)
			return nil
		}
		runner, err := fanOut(appendDoc("b"), appendDoc("c")).
			Add("audit", audit, flow.WithTimeout(time.Second)).
			Detach("b", "audit").
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		res, err := runner.Run(t.Context(), &doc{}, flow.WithRunID(testRunID), flow.WithConflictCheck())
		var cerr *flow.ConflictError
		if !errors.As(err, &cerr) {
			t.Fatalf("Run() error = %v, want *ConflictError", err)
		}
		if cerr.RunID != testRunID || cerr.Step != 2 || cerr.Field != "Docs" || !slices.Equal(cerr.Nodes, []string{"b", "c"}) {
			t.Errorf("ConflictError = %+v, want run %s step 2 field Docs by [b c]", *cerr, testRunID)
		}
		if want := `flow: conflict at step 2: field "Docs" written by [b c]`; err.Error() != want {
			t.Errorf("Error() = %q, want %q", err.Error(), want)
		}
		if want := []string{"a", "b", "c"}; !slices.Equal(res.Path, want) || res.Steps != 1 {
			t.Errorf("Run() result = %+v, want path %v and 1 completed step", res, want)
		}
		if err := runner.Wait(t.Context()); err != nil {
			t.Fatalf("Wait() error = %v", err)
		}
		if n := audits.Load(); n != 0 {
			t.Errorf("detached runs = %d, want 0: the step did not end clean", n)
		}
	})

	t.Run("distinct fields pass", func(t *testing.T) {
		t.Parallel()
		runner, err := fanOut(write(func(d *doc) { d.KB = "kb" }), write(func(d *doc) { d.Web = "web" })).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		var d doc
		if _, err := runner.Run(t.Context(), &d, flow.WithRunID(testRunID), flow.WithConflictCheck()); err != nil {
			t.Fatalf("Run() error = %v, want nil", err)
		}
		if d.KB != "kb" || d.Web != "web" {
			t.Errorf("state = %+v, want both fields written", d)
		}
	})

	t.Run("several conflicting fields join one error per field", func(t *testing.T) {
		t.Parallel()
		both := func(v string) flow.Node[doc] {
			return write(func(d *doc) {
				d.Docs = append(d.Docs, v)
				d.KB = v
			})
		}
		runner, err := fanOut(both("b"), both("c")).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		_, err = runner.Run(t.Context(), &doc{}, flow.WithRunID(testRunID), flow.WithConflictCheck())
		joined, ok := err.(interface{ Unwrap() []error })
		if !ok || len(joined.Unwrap()) != 2 {
			t.Fatalf("Run() error = %v, want two joined conflicts", err)
		}
		var fields []string
		for _, e := range joined.Unwrap() {
			var cerr *flow.ConflictError
			if !errors.As(e, &cerr) {
				t.Fatalf("joined error = %v, want *ConflictError", e)
			}
			fields = append(fields, cerr.Field)
		}
		if want := []string{"Docs", "KB"}; !slices.Equal(fields, want) {
			t.Errorf("conflict fields = %v, want %v in field order", fields, want)
		}
	})

	t.Run("nested writes are reported on the top-level field", func(t *testing.T) {
		t.Parallel()
		runner, err := fanOut(write(func(d *doc) { d.Nested.X = 1 }), write(func(d *doc) { d.Nested.Y = 2 })).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		_, err = runner.Run(t.Context(), &doc{}, flow.WithRunID(testRunID), flow.WithConflictCheck())
		var cerr *flow.ConflictError
		if !errors.As(err, &cerr) || cerr.Field != "Nested" {
			t.Errorf("Run() error = %v, want a conflict on field Nested", err)
		}
	})

	t.Run("write of a skipped node counts", func(t *testing.T) {
		t.Parallel()
		skipper := func(_ context.Context, d *doc) error {
			d.Docs = append(d.Docs, "b")
			return errBoom
		}
		runner, err := fanOut(skipper, appendDoc("c"), flow.WithOnFailure(flow.FailureSkip)).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		_, err = runner.Run(t.Context(), &doc{}, flow.WithRunID(testRunID), flow.WithConflictCheck())
		var cerr *flow.ConflictError
		if !errors.As(err, &cerr) || !slices.Equal(cerr.Nodes, []string{"b", "c"}) {
			t.Errorf("Run() error = %v, want a conflict on Docs by [b c]", err)
		}
	})

	t.Run("node error takes precedence over the conflict", func(t *testing.T) {
		t.Parallel()
		failer := func(_ context.Context, d *doc) error {
			d.Docs = append(d.Docs, "c")
			return errBoom
		}
		runner, err := fanOut(appendDoc("b"), failer).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		_, err = runner.Run(t.Context(), &doc{}, flow.WithRunID(testRunID), flow.WithConflictCheck())
		var cerr *flow.ConflictError
		if !errors.Is(err, errBoom) || errors.As(err, &cerr) {
			t.Errorf("Run() error = %v, want the node error without a conflict", err)
		}
	})

	t.Run("implies sequential execution", func(t *testing.T) {
		t.Parallel()
		var running, overlaps atomic.Int32
		exclusive := func(_ context.Context, _ *doc) error {
			if running.Add(1) > 1 {
				overlaps.Add(1)
			}
			defer running.Add(-1)
			return nil
		}
		runner, err := fanOut(exclusive, exclusive).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		if _, err := runner.Run(t.Context(), &doc{}, flow.WithRunID(testRunID), flow.WithConflictCheck()); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		if n := overlaps.Load(); n != 0 {
			t.Errorf("overlapping nodes = %d, want 0", n)
		}
	})

	t.Run("single node frontier takes no snapshot", func(t *testing.T) {
		t.Parallel()
		type measure struct{ Value float64 }
		poison := func(_ context.Context, m *measure) error {
			m.Value = math.NaN() // JSON não serializa NaN
			return nil
		}
		runner, err := flow.New[measure]("g").
			Add("a", poison).Add("b", func(context.Context, *measure) error { return nil }).
			Start("a").
			Edge("a", "b").Edge("b", flow.End).
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		if _, err := runner.Run(t.Context(), &measure{}, flow.WithRunID(testRunID), flow.WithConflictCheck()); err != nil {
			t.Errorf("Run() error = %v, want nil: linear steps never snapshot", err)
		}
	})

	t.Run("snapshot failure in a fan-out aborts", func(t *testing.T) {
		t.Parallel()
		type measure struct{ Value float64 }
		poison := func(_ context.Context, m *measure) error {
			m.Value = math.NaN()
			return nil
		}
		noop := func(context.Context, *measure) error { return nil }
		runner, err := flow.New[measure]("g").
			Add("a", poison).Add("b", noop).Add("c", noop).
			Start("a").
			Edge("a", "b").Edge("a", "c").
			Edge("b", flow.End).Edge("c", flow.End).
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		res, err := runner.Run(t.Context(), &measure{}, flow.WithRunID(testRunID), flow.WithConflictCheck())
		if err == nil || !strings.HasPrefix(err.Error(), "flow: conflict check at step 2:") {
			t.Errorf("Run() error = %v, want the snapshot failure at step 2", err)
		}
		if want := []string{"a"}; !slices.Equal(res.Path, want) {
			t.Errorf("Run() path = %v, want %v: b never ran", res.Path, want)
		}
	})

	t.Run("state that is not an object fails inline", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int32
		node := func(context.Context, *[]string) error {
			calls.Add(1)
			return nil
		}
		runner, err := flow.New[[]string]("g").Add("a", node).Start("a").Edge("a", flow.End).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		_, err = runner.Run(t.Context(), &[]string{}, flow.WithRunID(testRunID), flow.WithConflictCheck())
		if err == nil || !strings.HasPrefix(err.Error(), "flow: conflict check requires a JSON object state:") || calls.Load() != 0 {
			t.Errorf("Run() error = %v with %d calls, want the inline failure before any node", err, calls.Load())
		}
	})
}
