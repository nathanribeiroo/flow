package flow_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nathanribeiroo/flow"
)

// empty é um estado sem campos: o que se mede é o runner, não o nó.
type empty struct{}

func noop(context.Context, *empty) error { return nil }

// linear monta 5 nós em sequência.
func linear(b *testing.B) *flow.Runner[empty] {
	b.Helper()
	runner, err := flow.New[empty]("linear").
		Add("n1", noop).Add("n2", noop).Add("n3", noop).Add("n4", noop).Add("n5", noop).
		Start("n1").
		Edge("n1", "n2").Edge("n2", "n3").Edge("n3", "n4").Edge("n4", "n5").Edge("n5", flow.End).
		Compile()
	if err != nil {
		b.Fatalf("Compile() error = %v", err)
	}
	return runner
}

// fanOutGraph monta 1 -> 4 -> 1.
func fanOutGraph(b *testing.B) *flow.Runner[empty] {
	b.Helper()
	runner, err := flow.New[empty]("fanout").
		Add("in", noop).Add("w1", noop).Add("w2", noop).Add("w3", noop).Add("w4", noop).Add("out", noop).
		Start("in").
		Edge("in", "w1").Edge("in", "w2").Edge("in", "w3").Edge("in", "w4").
		Edge("w1", "out").Edge("w2", "out").Edge("w3", "out").Edge("w4", "out").
		Edge("out", flow.End).
		Compile()
	if err != nil {
		b.Fatalf("Compile() error = %v", err)
	}
	return runner
}

func BenchmarkLinear(b *testing.B) {
	runner := linear(b)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := runner.Run(b.Context(), &empty{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFanOut(b *testing.B) {
	runner := fanOutGraph(b)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := runner.Run(b.Context(), &empty{}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkConcurrentRuns dispara 1.000 Run simultâneos no mesmo Runner por
// iteração e reporta ns/run, o custo por execução dentro do lote.
func BenchmarkConcurrentRuns(b *testing.B) {
	const batch = 1000
	runner := fanOutGraph(b)
	var failures atomic.Int32
	b.ReportAllocs()
	for b.Loop() {
		var wg sync.WaitGroup
		for range batch {
			wg.Go(func() {
				if _, err := runner.Run(b.Context(), &empty{}); err != nil {
					failures.Add(1)
				}
			})
		}
		wg.Wait()
	}
	if n := failures.Load(); n != 0 {
		b.Fatalf("failed runs = %d", n)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batch), "ns/run")
}
