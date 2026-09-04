package flow_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nathanribeiroo/flow"
)

// ticket é o estado dos testes de destacada. Sem mutex de propósito: a
// destacada recebe uma cópia, e cada nó do fluxo escreve só no campo dele.
type ticket struct {
	plan  string
	notes string
}

// setPlan devolve um nó que escreve plan.
func setPlan(v string) flow.Node[ticket] {
	return func(_ context.Context, s *ticket) error {
		s.plan = v
		return nil
	}
}

// setNotes devolve um nó que escreve notes.
func setNotes(v string) flow.Node[ticket] {
	return func(_ context.Context, s *ticket) error {
		s.notes = v
		return nil
	}
}

// drain compila e executa g, depois drena as destacadas com Wait.
func drain(t *testing.T, g *flow.Graph[ticket], s *ticket) flow.Result {
	t.Helper()
	runner, err := g.Compile()
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	res, err := runner.Run(t.Context(), s, flow.WithRunID(testRunID))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if err := runner.Wait(t.Context()); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	return res
}

func TestRunnerDetach(t *testing.T) {
	t.Parallel()

	t.Run("detached survives the run and its writes are discarded", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			seen := ""
			audit := func(_ context.Context, s *ticket) error {
				time.Sleep(100 * time.Millisecond)
				seen = s.plan
				s.notes = "tampered"
				return nil
			}
			runner, err := flow.New[ticket]("g").
				Add("a", setPlan("p")).
				Add("audit", audit, flow.WithTimeout(time.Second)).
				Start("a").
				Edge("a", flow.End).
				Detach("a", "audit").
				Compile()
			if err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			var s ticket
			start := time.Now()
			res, err := runner.Run(ctx, &s, flow.WithRunID(testRunID))
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			cancel() // o fluxo principal acabou; a destacada não pode morrer junto
			if want := []string{"a"}; !slices.Equal(res.Path, want) {
				t.Errorf("Run() path = %v, want %v", res.Path, want)
			}
			if err := runner.Wait(t.Context()); err != nil {
				t.Fatalf("Wait() error = %v", err)
			}
			if got := time.Since(start); got != 100*time.Millisecond {
				t.Errorf("elapsed = %v, want %v", got, 100*time.Millisecond)
			}
			if seen != "p" {
				t.Errorf("detached saw plan = %q, want %q", seen, "p")
			}
			if s.notes != "" {
				t.Errorf("state notes = %q, want the detached write discarded", s.notes)
			}
		})
	})

	t.Run("detached respects its own timeout", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			var got error
			audit := func(ctx context.Context, _ *ticket) error {
				<-ctx.Done()
				got = ctx.Err()
				return got
			}
			g := flow.New[ticket]("g").
				Add("a", setPlan("p")).
				Add("audit", audit, flow.WithTimeout(time.Second)).
				Start("a").
				Edge("a", flow.End).
				Detach("a", "audit")
			start := time.Now()
			drain(t, g, &ticket{})
			if elapsed := time.Since(start); elapsed != time.Second {
				t.Errorf("elapsed = %v, want %v", elapsed, time.Second)
			}
			if !errors.Is(got, context.DeadlineExceeded) {
				t.Errorf("detached context error = %v, want %v", got, context.DeadlineExceeded)
			}
		})
	})

	t.Run("detached sees what siblings wrote in the same step", func(t *testing.T) {
		t.Parallel()
		seen := ticket{}
		audit := func(_ context.Context, s *ticket) error {
			seen = *s
			return nil
		}
		g := flow.New[ticket]("g").
			Add("a", setPlan("a")).
			Add("b", setPlan("b")).
			Add("c", setNotes("c")).
			Add("audit", audit, flow.WithTimeout(time.Second)).
			Start("a").
			Edge("a", "b").Edge("a", "c").
			Edge("b", flow.End).Edge("c", flow.End).
			Detach("b", "audit")
		drain(t, g, &ticket{})
		if want := (ticket{plan: "b", notes: "c"}); seen != want {
			t.Errorf("detached saw %+v, want %+v", seen, want)
		}
	})

	t.Run("detached fires once per completion of its source", func(t *testing.T) {
		t.Parallel()
		var audits atomic.Int32
		audit := func(_ context.Context, _ *ticket) error {
			audits.Add(1)
			return nil
		}
		rounds := 0
		again := func(_ context.Context, _ *ticket) []string {
			rounds++
			if rounds < 3 {
				return []string{"a"}
			}
			return []string{flow.End}
		}
		g := flow.New[ticket]("g").
			Add("a", setPlan("a")).
			Add("audit", audit, flow.WithTimeout(time.Second)).
			Start("a").
			Branch("a", again, "a", flow.End).
			Detach("a", "audit")
		drain(t, g, &ticket{})
		if n := audits.Load(); n != 3 {
			t.Errorf("detached runs = %d, want 3", n)
		}
	})

	t.Run("detached fires for a skipped node", func(t *testing.T) {
		t.Parallel()
		var audits atomic.Int32
		audit := func(_ context.Context, _ *ticket) error {
			audits.Add(1)
			return nil
		}
		fail := func(_ context.Context, _ *ticket) error { return errBoom }
		g := flow.New[ticket]("g").
			Add("a", fail, flow.WithOnFailure(flow.FailureSkip)).
			Add("audit", audit, flow.WithTimeout(time.Second)).
			Start("a").
			Edge("a", flow.End).
			Detach("a", "audit")
		res := drain(t, g, &ticket{})
		if n := audits.Load(); n != 1 || len(res.Errors) != 1 {
			t.Errorf("detached runs = %d, errors = %v, want 1 and one error", n, res.Errors)
		}
	})

	t.Run("detached does not fire when the step aborts", func(t *testing.T) {
		t.Parallel()
		var audits atomic.Int32
		audit := func(_ context.Context, _ *ticket) error {
			audits.Add(1)
			return nil
		}
		fail := func(_ context.Context, _ *ticket) error { return errBoom }
		runner, err := flow.New[ticket]("g").
			Add("a", setPlan("a")).
			Add("b", fail).
			Add("c", setNotes("c")).
			Add("audit", audit, flow.WithTimeout(time.Second)).
			Start("a").
			Edge("a", "b").Edge("a", "c").
			Edge("b", flow.End).Edge("c", flow.End).
			Detach("c", "audit").
			Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		if _, err := runner.Run(t.Context(), &ticket{}, flow.WithRunID(testRunID)); !errors.Is(err, errBoom) {
			t.Fatalf("Run() error = %v, want %v", err, errBoom)
		}
		if err := runner.Wait(t.Context()); err != nil {
			t.Fatalf("Wait() error = %v", err)
		}
		if n := audits.Load(); n != 0 {
			t.Errorf("detached runs = %d, want 0", n)
		}
	})
}

func TestRunnerWait(t *testing.T) {
	t.Parallel()

	t.Run("returns the context error when it expires first", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			audit := func(ctx context.Context, _ *ticket) error {
				<-ctx.Done()
				return ctx.Err()
			}
			runner, err := flow.New[ticket]("g").
				Add("a", setPlan("a")).
				Add("audit", audit, flow.WithTimeout(time.Hour)).
				Start("a").
				Edge("a", flow.End).
				Detach("a", "audit").
				Compile()
			if err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
			if _, err := runner.Run(t.Context(), &ticket{}, flow.WithRunID(testRunID)); err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			start := time.Now()
			if err := runner.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Wait() error = %v, want %v", err, context.DeadlineExceeded)
			}
			if got := time.Since(start); got != time.Second {
				t.Errorf("elapsed = %v, want %v", got, time.Second)
			}
			if err := runner.Wait(t.Context()); err != nil {
				t.Errorf("second Wait() error = %v, want nil", err)
			}
			if got := time.Since(start); got != time.Hour {
				t.Errorf("elapsed after drain = %v, want %v", got, time.Hour)
			}
		})
	})

	t.Run("returns immediately with nothing detached", func(t *testing.T) {
		t.Parallel()
		runner, err := flow.New[ticket]("g").Add("a", setPlan("a")).Start("a").Edge("a", flow.End).Compile()
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		if err := runner.Wait(t.Context()); err != nil {
			t.Errorf("Wait() error = %v, want nil", err)
		}
	})
}
