package flow_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nathanribeiroo/flow"
)

// Ticket é o estado do exemplo linear: um chamado de suporte.
type Ticket struct {
	Question string
	Plan     string
	Answer   string
}

func plan(_ context.Context, t *Ticket) error {
	t.Plan = "lookup: " + t.Question
	return nil
}

func answer(_ context.Context, t *Ticket) error {
	t.Answer = "answer for " + t.Plan
	return nil
}

// Grafo linear: plan -> answer -> End.
func ExampleRunner_Run() {
	runner, err := flow.New[Ticket]("support").
		Add("plan", plan, flow.WithRetry(3, 100*time.Millisecond)).
		Add("answer", answer, flow.WithTimeout(2*time.Second)).
		Start("plan").
		Edge("plan", "answer").
		Edge("answer", flow.End).
		Compile()
	if err != nil {
		fmt.Println(err)
		return
	}

	ticket := Ticket{Question: "reset password"}
	res, err := runner.Run(context.Background(), &ticket)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(ticket.Answer)
	fmt.Println(res.Path, res.Steps)
	// Output:
	// answer for lookup: reset password
	// [plan answer] 2
}

// Essay é o estado do exemplo cíclico: rascunho revisado até passar.
type Essay struct {
	Draft  string
	Rounds int
	Score  int
}

func draft(_ context.Context, e *Essay) error {
	e.Rounds++
	e.Draft = fmt.Sprintf("draft v%d", e.Rounds)
	return nil
}

func review(_ context.Context, e *Essay) error {
	e.Score = e.Rounds * 40
	return nil
}

func reviseOrFinish(_ context.Context, e *Essay) []string {
	if e.Score < 100 {
		return []string{"draft"}
	}
	return []string{flow.End}
}

// essayGraph monta o grafo com ciclo: draft -> review -> (draft | End).
func essayGraph() *flow.Graph[Essay] {
	return flow.New[Essay]("essay").
		Add("draft", draft).
		Add("review", review).
		Start("draft").
		Edge("draft", "review").
		Branch("review", reviseOrFinish, "draft", flow.End)
}

// Grafo com ciclo: review volta para draft duas vezes e então roteia para End.
func ExampleGraph_Branch() {
	runner, err := essayGraph().Compile()
	if err != nil {
		fmt.Println(err)
		return
	}

	var essay Essay
	res, err := runner.Run(context.Background(), &essay, flow.WithMaxSteps(10))
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(essay.Draft, essay.Score)
	fmt.Println(res.Path)
	// Output:
	// draft v3 120
	// [draft review draft review draft review]
}

// Mermaid do grafo cíclico. Ids são sanitizados; o nome real vai no label.
func ExampleRunner_Mermaid() {
	runner, err := essayGraph().Compile()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Print(runner.Mermaid())
	// Output:
	// flowchart TD
	//     n_draft["draft"]
	//     n_review["review"]
	//     n_end["end"]
	//     n_draft --> n_review
	//     n_review -->|branch| n_draft
	//     n_review -->|branch| n_end
}

// Compile reporta todos os problemas de uma vez.
func ExampleCompileError() {
	noop := func(_ context.Context, _ *Ticket) error { return nil }

	_, err := flow.New[Ticket]("broken").
		Add("fetch", noop).
		Add("orphan", noop).
		Start("fetch").
		Edge("fetch", "missing").
		Compile()

	fmt.Println(err)
	// Output:
	// flow: graph "broken": 3 issues: node "fetch": edge to unknown node "missing"; node "orphan": unreachable from start; node "orphan": no outgoing edge
}

// Support é o estado do exemplo com fan-out: duas buscas em paralelo, join
// e auditoria destacada. Cada nó escreve só no campo dele.
type Support struct {
	Question string
	Plan     string
	KB       []string
	Web      []string
	Answer   string
}

func planSupport(_ context.Context, s *Support) error {
	s.Plan = "lookup: " + s.Question
	return nil
}

func searchKB(_ context.Context, s *Support) error {
	s.KB = []string{"kb hit for " + s.Plan}
	return nil
}

func searchWeb(_ context.Context, _ *Support) error {
	return errors.New("web search down")
}

func compose(_ context.Context, s *Support) error {
	s.Answer = fmt.Sprintf("%d source(s)", len(s.KB)+len(s.Web))
	return nil
}

// Fan-out com join, FailureSkip em uma das buscas e auditoria destacada.
func ExampleRunner_Run_fanOut() {
	var audits atomic.Int32
	audit := func(_ context.Context, s *Support) error {
		audits.Add(1)
		s.Answer = "tampered" // escreve na cópia: o estado do Run não muda
		return nil
	}

	runner, err := flow.New[Support]("support").
		Add("plan", planSupport).
		Add("search_kb", searchKB, flow.WithTimeout(time.Second), flow.WithOnFailure(flow.FailureSkip)).
		Add("search_web", searchWeb, flow.WithTimeout(time.Second), flow.WithOnFailure(flow.FailureSkip)).
		Add("answer", compose, flow.WithJoin(flow.JoinAll)).
		Add("audit", audit, flow.WithTimeout(time.Second)).
		Start("plan").
		Edge("plan", "search_kb").
		Edge("plan", "search_web").
		Edge("search_kb", "answer").
		Edge("search_web", "answer").
		Edge("answer", flow.End).
		Detach("plan", "audit").
		Compile()
	if err != nil {
		fmt.Println(err)
		return
	}

	ctx := context.Background()
	support := Support{Question: "reset password"}
	res, err := runner.Run(ctx, &support, flow.WithBudget(5*time.Second))
	if err != nil {
		fmt.Println(err)
		return
	}
	if err := runner.Wait(ctx); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(support.Answer)
	fmt.Println(res.Path)
	fmt.Println(len(res.Errors), audits.Load())
	// Output:
	// 1 source(s)
	// [plan search_kb search_web answer]
	// 1 1
}
