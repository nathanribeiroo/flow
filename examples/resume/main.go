// Command resume mata a execução no meio de um fan-out e a retoma do
// checkpoint, mostrando que o nó convergente roda uma vez só.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/nathanribeiroo/flow"
)

// Report é o estado da execução. Precisa serializar em JSON, porque é isso
// que vai para o checkpoint, e cada nó escreve só no campo dele.
type Report struct {
	Raw      string `json:"raw"`
	Clean    string `json:"clean"`
	Enriched string `json:"enriched"`
	Score    int    `json:"score"`
	Summary  string `json:"summary"`
}

// counters conta as execuções por nó. Vive fora do estado de propósito: é o
// que sobrevive à queda e mostra o que reexecutou na retomada.
type counters struct {
	fetch, clean, enrich, score, merge atomic.Int32
}

// build monta o losango desbalanceado fetch -> {clean, enrich}, clean -> merge
// e enrich -> score -> merge. merge é o convergente: espera os dois ramos,
// e o ramo de enrich é um passo mais longo que o de clean.
//
// crash é chamado pelo nó enrich na primeira execução e derruba o processo
// simulado no meio do fan-out do passo 2.
func build(c *counters, crash context.CancelFunc) (*flow.Runner[Report], error) {
	fetch := func(_ context.Context, r *Report) error {
		c.fetch.Add(1)
		time.Sleep(10 * time.Millisecond)
		r.Raw = "documento bruto"
		return nil
	}
	// clean não observa o contexto, como um nó que faz trabalho em memória.
	// Ele termina mesmo com a execução já cancelada, e o que escreveu morre
	// junto com o passo: a retomada volta ao estado do checkpoint.
	clean := func(_ context.Context, r *Report) error {
		c.clean.Add(1)
		time.Sleep(30 * time.Millisecond)
		r.Clean = "documento limpo"
		return nil
	}
	enrich := func(ctx context.Context, r *Report) error {
		if c.enrich.Add(1) == 1 {
			time.Sleep(10 * time.Millisecond)
			fmt.Println("  enrich: o processo cai agora, no meio do fan-out")
			crash()
			return ctx.Err()
		}
		time.Sleep(10 * time.Millisecond)
		r.Enriched = "documento enriquecido"
		return nil
	}
	score := func(_ context.Context, r *Report) error {
		c.score.Add(1)
		time.Sleep(10 * time.Millisecond)
		r.Score = len(r.Enriched)
		return nil
	}
	merge := func(_ context.Context, r *Report) error {
		c.merge.Add(1)
		time.Sleep(10 * time.Millisecond)
		r.Summary = fmt.Sprintf("%s + %s (score %d)", r.Clean, r.Enriched, r.Score)
		return nil
	}

	return flow.New[Report]("report").
		Add("fetch", fetch).
		Add("clean", clean).
		Add("enrich", enrich).
		Add("score", score).
		Add("merge", merge).
		Start("fetch").
		Edge("fetch", "clean").
		Edge("fetch", "enrich").
		Edge("clean", "merge").
		Edge("enrich", "score").
		Edge("score", "merge").
		Edge("merge", flow.End).
		Compile()
}

func run() error {
	const runID = "sessao-42"
	store := flow.NewMemoryStore()
	var c counters

	// Primeiro processo: roda até cair.
	doomed, crash := context.WithCancel(context.Background())
	defer crash()
	runner, err := build(&c, crash)
	if err != nil {
		return err
	}

	fmt.Println("processo 1")
	var state Report
	res, err := runner.Run(doomed, &state,
		flow.WithRunID(runID),
		flow.WithCheckpoint(store))
	if !errors.Is(err, context.Canceled) {
		return fmt.Errorf("esperava cancelamento, veio %v", err)
	}
	fmt.Printf("  erro       %v\n", err)
	fmt.Printf("  caminho    %v em %d passo(s) concluído(s)\n", res.Path, res.Steps)
	fmt.Printf("  estado     clean=%q, escrito num passo que não fechou\n", state.Clean)

	// Segundo processo: contexto novo, mesmo store, mesmo id.
	fresh := context.Background()
	cp, err := store.Load(fresh, runID)
	if err != nil {
		return err
	}
	fmt.Printf("\ncheckpoint\n  passo %d, pendentes %v, topologia %s\n", cp.Step, cp.Pending, cp.Version)

	fmt.Println("\nprocesso 2 (Resume)")
	resumed, res, err := runner.Resume(fresh, runID, flow.WithCheckpoint(store))
	if err != nil {
		return err
	}
	fmt.Printf("  caminho    %v\n", res.Path)
	fmt.Printf("  passos     %d, contando a partir do checkpoint\n", res.Steps)
	fmt.Printf("  resumo     %s\n", resumed.Summary)

	fmt.Printf("\nexecuções por nó\n  fetch=%d clean=%d enrich=%d score=%d merge=%d\n",
		c.fetch.Load(), c.clean.Load(), c.enrich.Load(), c.score.Load(), c.merge.Load())
	fmt.Printf("  merge, o convergente, rodou %d vez\n", c.merge.Load())
	fmt.Println("  clean e enrich rodaram duas vezes: o passo interrompido reexecuta, e isso é at-least-once")
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "erro:", err)
		os.Exit(1)
	}
}
