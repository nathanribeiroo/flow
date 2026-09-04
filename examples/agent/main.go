// Command agent demonstra o grafo do README: duas buscas em paralelo, join,
// auditoria em aresta destacada e um ciclo de revisão, com o canal de eventos
// ligado num consumidor que imprime o turno linha a linha.
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

// Support é o estado do turno. Cada nó escreve só nos campos dele, que é o
// contrato da lib para fan-out sem lock: KB é de search_kb, Web é de
// search_web, e nenhum dos dois encosta no campo do outro.
type Support struct {
	Question string   `json:"question"`
	Plan     string   `json:"plan"`
	Round    int      `json:"round"`
	KB       []string `json:"kb"`
	Web      []string `json:"web"`
	Answer   string   `json:"answer"`
}

// webAttempts conta as chamadas de searchWeb. A primeira falha, para mostrar
// FailureSkip seguindo com resultado parcial em vez de abortar o turno.
var webAttempts atomic.Int32

// work simula I/O lento respeitando o contexto, que é o que um nó de verdade
// faz ao chamar um modelo ou um banco.
func work(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// plan abre a rodada e escreve o plano de busca.
func plan(ctx context.Context, s *Support) error {
	if err := work(ctx, 20*time.Millisecond); err != nil {
		return err
	}
	s.Round++
	s.Plan = fmt.Sprintf("rodada %d de %q", s.Round, s.Question)
	return nil
}

// searchKB consulta a base interna.
func searchKB(ctx context.Context, s *Support) error {
	if err := work(ctx, 40*time.Millisecond); err != nil {
		return err
	}
	s.KB = []string{"kb: artigo interno sobre " + s.Question}
	return nil
}

// searchWeb consulta a base externa e falha na primeira tentativa.
func searchWeb(ctx context.Context, s *Support) error {
	if err := work(ctx, 30*time.Millisecond); err != nil {
		return err
	}
	if webAttempts.Add(1) == 1 {
		return errors.New("web search unavailable")
	}
	s.Web = []string{"web: post público sobre " + s.Question}
	return nil
}

// compose junta o que chegou das duas buscas.
func compose(ctx context.Context, s *Support) error {
	if err := work(ctx, 20*time.Millisecond); err != nil {
		return err
	}
	s.Answer = fmt.Sprintf("resposta com %d fonte(s)", len(s.KB)+len(s.Web))
	return nil
}

// audit roda destacado: recebe uma cópia do estado e o que escrever aqui é
// jogado fora. A escrita abaixo existe para provar isso no fim do programa.
func audit(ctx context.Context, s *Support) error {
	if err := work(ctx, 60*time.Millisecond); err != nil {
		return err
	}
	s.Answer = "auditoria sobrescreveu a resposta"
	return nil
}

// reviseOrFinish volta para plan enquanto faltar fonte, no máximo três
// rodadas. Os dois destinos possíveis são declarados no Branch.
func reviseOrFinish(_ context.Context, s *Support) []string {
	if len(s.KB)+len(s.Web) < 2 && s.Round < 3 {
		return []string{"plan"}
	}
	return []string{flow.End}
}

// build compila a topologia do README.
func build() (*flow.Runner[Support], error) {
	return flow.New[Support]("support-agent").
		Add("plan", plan).
		Add("search_kb", searchKB,
			flow.WithTimeout(time.Second),
			flow.WithOnFailure(flow.FailureSkip)).
		Add("search_web", searchWeb,
			flow.WithTimeout(time.Second),
			flow.WithOnFailure(flow.FailureSkip)).
		Add("answer", compose, flow.WithJoin(flow.JoinAll)).
		Add("audit", audit, flow.WithTimeout(2*time.Second)).
		Start("plan").
		Edge("plan", "search_kb").
		Edge("plan", "search_web").
		Edge("search_kb", "answer").
		Edge("search_web", "answer").
		Detach("plan", "audit").
		Branch("answer", reviseOrFinish, "plan", flow.End).
		Compile()
}

// line formata um evento do jeito que uma CLI ao vivo faria.
func line(ev flow.Event) string {
	switch ev.Kind {
	case flow.EventNodeStart:
		return fmt.Sprintf("  passo %d  %-11s início    (tentativa %d)", ev.Info.Step, ev.Info.Node, ev.Info.Attempt)
	case flow.EventNodeEnd:
		if ev.Err != nil {
			return fmt.Sprintf("  passo %d  %-11s falhou    %v", ev.Info.Step, ev.Info.Node, ev.Err)
		}
		return fmt.Sprintf("  passo %d  %-11s concluiu", ev.Info.Step, ev.Info.Node)
	case flow.EventStepEnd:
		return fmt.Sprintf("  passo %d  superstep fechado", ev.Info.Step)
	case flow.EventRunEnd:
		return fmt.Sprintf("  run %s terminou em %d passos", ev.Info.RunID, ev.Info.Step)
	default:
		return fmt.Sprintf("  evento %d", ev.Kind)
	}
}

func run() error {
	runner, err := build()
	if err != nil {
		return err
	}
	ctx := context.Background()

	// O canal é do consumidor: ele cria, ele fecha, e só depois de Run voltar
	// e de Wait drenar as destacadas. O envio da lib é não-bloqueante.
	events := make(chan flow.Event, 256)
	printed := make(chan struct{})
	go func() {
		defer close(printed)
		for ev := range events {
			fmt.Println(line(ev))
		}
	}()

	fmt.Printf("grafo %q, topologia %s\n\n", runner.Name(), runner.Version())

	state := Support{Question: "como troco minha senha"}
	res, err := runner.Run(ctx, &state,
		flow.WithRunID("demo-agent"),
		flow.WithBudget(5*time.Second),
		flow.WithEvents(events))
	if err != nil {
		return err
	}
	if err := runner.Wait(ctx); err != nil {
		return err
	}
	close(events)
	<-printed

	fmt.Printf("\nresposta   %s\n", state.Answer)
	fmt.Printf("caminho    %v\n", res.Path)
	fmt.Printf("passos     %d em %s, dentro do orçamento de 5s\n", res.Steps, res.Elapsed.Round(time.Millisecond))
	fmt.Printf("pulados    %v\n", res.Errors)
	fmt.Printf("fontes     kb=%d web=%d\n", len(state.KB), len(state.Web))
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "erro:", err)
		os.Exit(1)
	}
}
