package flow

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"
)

// Result descreve o desfecho de um Run. Path e Errors são determinísticos
// mesmo em execução paralela: dentro de um superstep as entradas seguem o
// índice de declaração do nó, nunca a ordem de conclusão.
type Result struct {
	Steps   int           // supersteps concluídos
	Path    []string      // nós executados, agrupados por superstep
	Errors  []error       // erros de nós com FailureSkip, cada um em *NodeError
	Elapsed time.Duration // duração total, preenchida mesmo em erro
}

// execution é o estado de um Run: configuração, estado do usuário, chegadas
// ainda não consumidas pelo join e o Result em construção.
type execution[S any] struct {
	r       *Runner[S]
	cfg     runConfig
	state   *S
	arrived map[string]bool
	res     Result
}

// outcome é o que um nó produziu dentro de um superstep.
type outcome struct {
	err     error    // nil, *NodeError, ErrUnknownTarget ou erro de contexto
	done    bool     // o nó executou e entra em Path: sucesso, pulado ou Router com erro
	skipped bool     // err é de nó com FailureSkip: registra em Errors e segue
	targets []string // alvos roteados; para nó pulado, as arestas estáticas
}

// fatal diz se o resultado encerra o superstep e cancela os irmãos.
func (o outcome) fatal() bool { return o.err != nil && !o.skipped }

// Run executa o grafo sobre state até a fronteira esvaziar. O chamador é
// dono de state durante a execução. Erro de nó volta embrulhado em
// *NodeError; cancelamento de ctx volta como o erro do próprio contexto e
// estouro de WithBudget como ErrBudget, ambos com precedência sobre erro de
// nó. Result descreve o que foi concluído, mesmo em caso de erro.
func (r *Runner[S]) Run(ctx context.Context, state *S, opts ...RunOption) (res Result, err error) {
	cfg := runConfig{maxSteps: defaultMaxSteps}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.maxSteps < 1 {
		return res, fmt.Errorf("flow: max steps must be at least 1, got %d", cfg.maxSteps)
	}
	if cfg.hasBudget && cfg.budget <= 0 {
		return res, fmt.Errorf("flow: budget must be positive, got %v", cfg.budget)
	}
	if state == nil {
		return res, errors.New("flow: state is nil")
	}

	started := time.Now()
	defer func() { res.Elapsed = time.Since(started) }()

	if cfg.hasBudget {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(ctx, cfg.budget, ErrBudget)
		defer cancel()
	}

	x := &execution[S]{r: r, cfg: cfg, state: state, arrived: make(map[string]bool, len(r.order))}
	frontier := []string{r.start}
	for len(frontier) > 0 {
		if x.res.Steps >= cfg.maxSteps {
			return x.res, fmt.Errorf("%w: limit %d", ErrMaxSteps, cfg.maxSteps)
		}
		if err := x.interrupted(ctx); err != nil {
			return x.res, err
		}
		if err := x.settle(ctx, frontier, x.execute(ctx, frontier)); err != nil {
			return x.res, err
		}
		x.res.Steps++
		frontier = x.advance()
	}
	return x.res, nil
}

// interrupted classifica o encerramento do contexto: budget estourado vira
// ErrBudget; qualquer outro motivo volta como o erro do próprio contexto.
func (x *execution[S]) interrupted(ctx context.Context) error {
	if ctx.Err() == nil {
		return nil
	}
	if errors.Is(context.Cause(ctx), ErrBudget) {
		return fmt.Errorf("%w: %v", ErrBudget, x.cfg.budget)
	}
	return ctx.Err()
}

// execute roda a fronteira e devolve um outcome por nó, na mesma ordem.
// Fronteira de um nó, ou WithSequential, roda inline; do contrário cada nó
// ganha uma goroutine no errgroup e o primeiro erro fatal cancela os irmãos.
// Inline, um erro fatal interrompe a fronteira e a fatia volta truncada.
func (x *execution[S]) execute(ctx context.Context, frontier []string) []outcome {
	out := make([]outcome, len(frontier))
	step := x.res.Steps + 1
	if x.cfg.sequential || len(frontier) == 1 {
		for i, name := range frontier {
			out[i] = x.r.execute(ctx, x.state, name, step)
			if out[i].fatal() {
				return out[:i+1]
			}
		}
		return out
	}
	g, gctx := errgroup.WithContext(ctx)
	for i, name := range frontier {
		g.Go(func() error {
			out[i] = x.r.execute(gctx, x.state, name, step)
			if out[i].fatal() {
				return out[i].err
			}
			return nil
		})
	}
	// O retorno de Wait é só o primeiro erro fatal, que já está em out junto
	// com os demais; aqui ele apenas sincroniza o fim das goroutines.
	_ = g.Wait()
	return out
}

// execute roda um nó e decide o roteamento dele. Nó com FailureSkip que
// falha segue pelas arestas estáticas sem consultar o Router.
func (r *Runner[S]) execute(ctx context.Context, state *S, name string, step int) outcome {
	err := r.runNode(ctx, state, name, step)
	if err == nil {
		targets, err := r.route(ctx, state, name)
		return outcome{err: err, done: true, targets: targets}
	}
	var nerr *NodeError
	if r.nodes[name].cfg.failure == FailureSkip && errors.As(err, &nerr) {
		return outcome{err: err, done: true, skipped: true, targets: r.edges[name]}
	}
	return outcome{err: err}
}

// settle aplica os resultados do superstep: Path, Errors, chegadas para o
// join e disparo das destacadas. Cancelamento do chamador e budget têm
// precedência; depois vêm os erros fatais combinados com errors.Join. Erro
// de contexto de um irmão é consequência do cancelamento, não causa, e é
// descartado. Destacadas só disparam quando o superstep termina limpo.
func (x *execution[S]) settle(ctx context.Context, frontier []string, out []outcome) error {
	var fatal []error
	var done []string
	for i, o := range out {
		name := frontier[i]
		if o.done {
			x.res.Path = append(x.res.Path, name)
			done = append(done, name)
			for _, t := range o.targets {
				if t != End {
					x.arrived[t] = true
				}
			}
		}
		var nerr *NodeError
		switch {
		case o.skipped:
			x.res.Errors = append(x.res.Errors, o.err)
		case errors.As(o.err, &nerr), errors.Is(o.err, ErrUnknownTarget):
			fatal = append(fatal, o.err)
		}
	}
	if err := x.interrupted(ctx); err != nil {
		return err
	}
	switch len(fatal) {
	case 0:
	case 1:
		return fatal[0]
	default:
		return errors.Join(fatal...)
	}
	x.detach(ctx, done)
	return nil
}

// advance aplica a regra de join da seção 4.2 e devolve a próxima fronteira
// em ordem de declaração. Candidato é nó com chegada não consumida; JoinAny
// dispara sempre; JoinAll espera enquanto outro candidato ainda chega a ele
// por caminho de ida. Todo passo com candidatos dispara ao menos um, então
// a execução sempre avança.
func (x *execution[S]) advance() []string {
	var candidates []string
	for _, name := range x.r.order {
		if x.arrived[name] {
			candidates = append(candidates, name)
		}
	}
	var frontier []string
	for _, n := range candidates {
		if x.r.nodes[n].cfg.join == JoinAll && x.blocked(candidates, n) {
			continue
		}
		frontier = append(frontier, n)
	}
	// Candidatos que se bloqueiam em círculo por arestas diretas (a -> c,
	// c -> b, b -> a, todos com chegada) nunca disparariam: o primeiro
	// declarado vence, e os outros seguem na onda seguinte.
	if len(frontier) == 0 && len(candidates) > 0 {
		frontier = candidates[:1]
	}
	for _, n := range frontier {
		delete(x.arrived, n)
	}
	return frontier
}

// blocked diz se o candidato n espera: algum outro candidato chega a ele
// por caminho de ida.
func (x *execution[S]) blocked(candidates []string, n string) bool {
	for _, u := range candidates {
		if u != n && x.r.blockers[n][u] {
			return true
		}
	}
	return false
}

// route devolve os alvos de name: as arestas estáticas ou, havendo Branch,
// o que o Router escolher entre os targets declarados.
func (r *Runner[S]) route(ctx context.Context, state *S, name string) ([]string, error) {
	b, ok := r.branches[name]
	if !ok {
		return r.edges[name], nil
	}
	chosen := b.route(ctx, state)
	for _, t := range chosen {
		if !slices.Contains(b.targets, t) {
			return nil, fmt.Errorf("%w: %q from node %q", ErrUnknownTarget, t, name)
		}
	}
	return chosen, nil
}

// runNode executa um nó respeitando WithTimeout e WithRetry. O timeout vale
// por tentativa. Com o contexto encerrado não há retentativa: falha causada
// pelo encerramento volta como o erro do contexto, e falha própria do nó
// volta em *NodeError para ser reportada mesmo durante um cancelamento.
func (r *Runner[S]) runNode(ctx context.Context, state *S, name string, step int) error {
	n := r.nodes[name]
	for attempt := 1; ; attempt++ {
		err := attemptNode(ctx, n, state)
		if err == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			if errors.Is(err, ctxErr) {
				return ctxErr
			}
			return &NodeError{Node: name, Step: step, Attempt: attempt, Err: err}
		}
		if attempt >= n.cfg.attempts {
			return &NodeError{Node: name, Step: step, Attempt: attempt, Err: err}
		}
		if err := waitBackoff(ctx, n.cfg.backoff); err != nil {
			return err
		}
	}
}

// attemptNode roda uma tentativa do nó com o timeout configurado. Vive fora
// do laço de retry para que o cancel do timeout rode a cada tentativa.
func attemptNode[S any](ctx context.Context, n node[S], state *S) error {
	if n.cfg.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, n.cfg.timeout)
		defer cancel()
	}
	return n.fn(ctx, state)
}

// waitBackoff espera d ou o cancelamento do contexto, o que vier primeiro.
func waitBackoff(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
