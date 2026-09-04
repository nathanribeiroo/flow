// Command trace mostra a forma que um hook de tracing tem, sem OpenTelemetry:
// dois hooks aninhados que abrem e fecham um span por tentativa de nó, e o
// diagrama do caminho percorrido no fim.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nathanribeiroo/flow"
)

// Doc é o estado do exemplo. Cada nó escreve só no campo dele.
type Doc struct {
	Text     string
	Grammar  string
	Facts    string
	Verdict  string
	Attempts int
}

// spanDepth é a chave da profundidade do span no contexto. Chave de tipo
// próprio e não exportado é o que impede colisão entre pacotes.
type spanDepth struct{}

// spanStart é a chave do instante em que a tentativa começou.
type spanStart struct{}

// depth lê a profundidade corrente, zero quando ninguém escreveu ainda.
func depth(ctx context.Context) int {
	d, _ := ctx.Value(spanDepth{}).(int)
	return d
}

// indent devolve o recuo de um nível de span.
func indent(level int) string { return strings.Repeat("  ", level) }

// tracer abre e fecha um span por tentativa. NodeStart devolve o contexto
// enriquecido, e é isso que faz o span do nó envolver a execução de verdade:
// tudo que rodar dentro do nó enxerga esse contexto.
type tracer struct{}

var _ flow.Hook = tracer{}

func (tracer) NodeStart(ctx context.Context, info flow.NodeInfo) context.Context {
	d := depth(ctx)
	fmt.Printf("%s┌ span %s · passo %d · tentativa %d\n", indent(d), info.Node, info.Step, info.Attempt)
	return context.WithValue(ctx, spanDepth{}, d+1)
}

func (tracer) NodeEnd(ctx context.Context, info flow.NodeInfo, err error) {
	// O contexto que chega aqui é o que este hook devolveu no NodeStart, não
	// o do fim da cadeia. Sem isso o primeiro hook fecharia o span do último.
	status := "ok"
	if err != nil {
		status = "erro: " + err.Error()
	}
	fmt.Printf("%s└ span %s · %s\n", indent(depth(ctx)-1), info.Node, status)
}

// meter roda por dentro do tracer, porque foi registrado depois: o NodeStart
// dele vem em segundo e o NodeEnd em primeiro, aninhando como defer. Ele
// enxerga a profundidade que o tracer escreveu e mede a tentativa guardando
// o instante inicial no próprio contexto.
type meter struct{}

var _ flow.Hook = meter{}

func (meter) NodeStart(ctx context.Context, info flow.NodeInfo) context.Context {
	fmt.Printf("%s· medindo %s\n", indent(depth(ctx)), info.Node)
	return context.WithValue(ctx, spanStart{}, time.Now())
}

func (meter) NodeEnd(ctx context.Context, info flow.NodeInfo, _ error) {
	started, ok := ctx.Value(spanStart{}).(time.Time)
	if !ok {
		return
	}
	fmt.Printf("%s· %s levou %s\n", indent(depth(ctx)), info.Node, time.Since(started).Round(10*time.Millisecond))
}

// work simula I/O lento e anuncia, a partir do contexto que o nó recebeu, que
// o nó roda mesmo dentro do span aberto pelos hooks.
func work(ctx context.Context, name string, d time.Duration) error {
	fmt.Printf("%s› %s trabalhando\n", indent(depth(ctx)), name)
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// factAttempts conta as chamadas de checkFacts: as duas primeiras falham,
// para o retry render três spans irmãos em vez de um span que esconde tudo.
var factAttempts atomic.Int32

func build() (*flow.Runner[Doc], error) {
	parse := func(ctx context.Context, d *Doc) error {
		if err := work(ctx, "parse", 20*time.Millisecond); err != nil {
			return err
		}
		d.Text = "texto normalizado"
		return nil
	}
	checkGrammar := func(ctx context.Context, d *Doc) error {
		if err := work(ctx, "check_grammar", 20*time.Millisecond); err != nil {
			return err
		}
		d.Grammar = "sem erros"
		return nil
	}
	checkFacts := func(ctx context.Context, d *Doc) error {
		if err := work(ctx, "check_facts", 10*time.Millisecond); err != nil {
			return err
		}
		d.Attempts = int(factAttempts.Add(1))
		if d.Attempts < 3 {
			return errors.New("índice de fatos indisponível")
		}
		d.Facts = "conferido"
		return nil
	}
	report := func(ctx context.Context, d *Doc) error {
		if err := work(ctx, "report", 20*time.Millisecond); err != nil {
			return err
		}
		d.Verdict = fmt.Sprintf("%s, gramática %s, fatos %s", d.Text, d.Grammar, d.Facts)
		return nil
	}

	return flow.New[Doc]("review").
		Add("parse", parse).
		Add("check_grammar", checkGrammar).
		Add("check_facts", checkFacts, flow.WithRetry(3, 10*time.Millisecond)).
		Add("report", report).
		Start("parse").
		Edge("parse", "check_grammar").
		Edge("parse", "check_facts").
		Edge("check_grammar", "report").
		Edge("check_facts", "report").
		Edge("report", flow.End).
		Compile()
}

func run() error {
	runner, err := build()
	if err != nil {
		return err
	}

	var doc Doc
	// WithSequential mantém a fronteira em ordem declarada e o trace legível;
	// sem ele os dois checks escreveriam intercalados na saída.
	res, err := runner.Run(context.Background(), &doc,
		flow.WithRunID("demo-trace"),
		flow.WithSequential(),
		flow.WithHook(tracer{}),
		flow.WithHook(meter{}))
	if err != nil {
		return err
	}

	fmt.Printf("\nveredito   %s\n", doc.Verdict)
	fmt.Printf("caminho    %v em %s\n\n", res.Path, res.Elapsed.Round(10*time.Millisecond))
	fmt.Print(runner.MermaidTrace(res))
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "erro:", err)
		os.Exit(1)
	}
}
