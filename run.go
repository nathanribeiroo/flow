package flow

import (
	"context"
	"encoding/json"
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
	RunID   string        // o de WithRunID, ou o gerado pela lib
	Version string        // hash de topologia do Runner
	Steps   int           // supersteps concluídos
	Path    []string      // nós executados, agrupados por superstep
	Errors  []error       // erros de nós com FailureSkip, cada um em *NodeError
	Elapsed time.Duration // duração total, preenchida mesmo em erro
}

// execution é o estado de um Run: configuração, estado do usuário, chegadas
// ainda não consumidas pelo join e o Result em construção. Só o laço
// principal escreve em arrived e res; as destacadas leem apenas r e cfg.
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

// configure aplica as options e valida o que é erro de uso, antes de qualquer
// hook, evento ou nó.
func configure(opts []RunOption) (runConfig, error) {
	cfg := runConfig{maxSteps: defaultMaxSteps}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.maxSteps < 1 {
		return cfg, fmt.Errorf("flow: max steps must be at least 1, got %d", cfg.maxSteps)
	}
	if cfg.hasBudget && cfg.budget <= 0 {
		return cfg, fmt.Errorf("flow: budget must be positive, got %v", cfg.budget)
	}
	if slices.Contains(cfg.hooks, nil) {
		return cfg, errors.New("flow: hook is nil")
	}
	return cfg, nil
}

// newExecution monta o estado de uma execução com o Result já carimbado.
func (r *Runner[S]) newExecution(cfg runConfig, state *S) *execution[S] {
	return &execution[S]{
		r:       r,
		cfg:     cfg,
		state:   state,
		arrived: make(map[string]bool, len(r.order)),
		res:     Result{RunID: cfg.runID, Version: r.version},
	}
}

// Run executa o grafo sobre state até a fronteira esvaziar. O chamador é
// dono de state durante a execução. Erro de nó volta embrulhado em
// *NodeError; cancelamento de ctx volta como o erro do próprio contexto e
// estouro de WithBudget como ErrBudget, ambos com precedência sobre erro de
// nó. Result descreve o que foi concluído, mesmo em caso de erro. Erro de
// uso das options, state nil e estado não serializável com WithCheckpoint
// voltam antes de qualquer hook, evento ou nó.
func (r *Runner[S]) Run(ctx context.Context, state *S, opts ...RunOption) (Result, error) {
	cfg, err := configure(opts)
	if err != nil {
		return Result{}, err
	}
	if state == nil {
		return Result{}, errors.New("flow: state is nil")
	}
	if cfg.conflict {
		if _, err := snapshot(state); err != nil {
			return Result{}, fmt.Errorf("flow: conflict check requires a JSON object state: %w", err)
		}
	} else if cfg.store != nil {
		if _, err := json.Marshal(state); err != nil {
			return Result{}, fmt.Errorf("flow: state is not serializable: %w", err)
		}
	}
	if cfg.runID == "" {
		cfg.runID = newRunID()
	}
	return r.newExecution(cfg, state).loop(ctx, []string{r.start})
}

// loop roda supersteps a partir de frontier até ela esvaziar. Cada passo
// concluído incrementa Steps, resolve o join, grava o checkpoint e emite
// EventStepEnd, nessa ordem. EventRunEnd sai no retorno, com o erro do run.
func (x *execution[S]) loop(ctx context.Context, frontier []string) (res Result, err error) {
	started := time.Now()
	defer func() {
		res.Elapsed = time.Since(started)
		x.emit(EventRunEnd, x.info("", res.Steps, 0), err)
	}()

	if x.cfg.hasBudget {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(ctx, x.cfg.budget, ErrBudget)
		defer cancel()
	}

	for len(frontier) > 0 {
		if x.res.Steps >= x.cfg.maxSteps {
			return x.res, fmt.Errorf("%w: limit %d", ErrMaxSteps, x.cfg.maxSteps)
		}
		if err := x.interrupted(ctx); err != nil {
			return x.res, err
		}
		out, rec := x.execute(ctx, frontier)
		if err := x.settle(ctx, frontier, out, rec); err != nil {
			return x.res, err
		}
		x.res.Steps++
		candidates := x.candidates()
		frontier = x.advance(candidates)
		if err := x.save(ctx, candidates); err != nil {
			return x.res, err
		}
		x.emit(EventStepEnd, x.info("", x.res.Steps, 0), nil)
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

// execute roda a fronteira e devolve um outcome por nó, na mesma ordem, mais
// o registro de conflito quando WithConflictCheck vigia um fan-out.
// Fronteira de um nó, ou WithSequential, roda inline; do contrário cada nó
// ganha uma goroutine no errgroup e o primeiro erro fatal cancela os irmãos.
// Inline, um erro fatal ou uma falha de snapshot interrompe a fronteira e a
// fatia volta truncada.
func (x *execution[S]) execute(ctx context.Context, frontier []string) ([]outcome, *conflictRecord) {
	// Chegada é consumida quando o nó começa a executar, não quando é
	// escalado: é o que deixa Pending do checkpoint conter a fronteira.
	for _, name := range frontier {
		delete(x.arrived, name)
	}
	out := make([]outcome, len(frontier))
	step := x.res.Steps + 1
	if x.cfg.sequential || len(frontier) == 1 {
		var rec *conflictRecord
		if x.cfg.conflict && len(frontier) > 1 {
			rec = &conflictRecord{}
		}
		for i, name := range frontier {
			if rec == nil {
				out[i] = x.perform(ctx, name, step)
			} else if o, ok := x.checked(ctx, rec, name, step); ok {
				out[i] = o
			} else {
				out[i] = o
				return out[:i+1], rec
			}
			if out[i].fatal() {
				return out[:i+1], rec
			}
		}
		return out, rec
	}
	g, gctx := errgroup.WithContext(ctx)
	for i, name := range frontier {
		g.Go(func() error {
			out[i] = x.perform(gctx, name, step)
			if out[i].fatal() {
				return out[i].err
			}
			return nil
		})
	}
	// O retorno de Wait é só o primeiro erro fatal, que já está em out junto
	// com os demais; aqui ele apenas sincroniza o fim das goroutines.
	_ = g.Wait()
	return out, nil
}

// perform roda um nó e decide o roteamento dele. Nó com FailureSkip que
// falha segue pelas arestas estáticas sem consultar o Router.
func (x *execution[S]) perform(ctx context.Context, name string, step int) outcome {
	err := x.runNode(ctx, x.state, name, step)
	if err == nil {
		targets, err := x.r.route(ctx, x.state, name)
		return outcome{err: err, done: true, targets: targets}
	}
	var nerr *NodeError
	if x.r.nodes[name].cfg.failure == FailureSkip && errors.As(err, &nerr) {
		return outcome{err: err, done: true, skipped: true, targets: x.r.edges[name]}
	}
	return outcome{err: err}
}

// settle aplica os resultados do superstep: Path, Errors, chegadas para o
// join e disparo das destacadas. Cancelamento do chamador e budget têm
// precedência; depois vêm os erros fatais combinados com errors.Join; por
// último o conflito de escrita, que é diagnóstico. Erro de contexto de um
// irmão é consequência do cancelamento, não causa, e é descartado.
// Destacadas só disparam quando o superstep termina limpo.
func (x *execution[S]) settle(ctx context.Context, frontier []string, out []outcome, rec *conflictRecord) error {
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
	if err := rec.check(x.cfg.runID, x.res.Steps+1); err != nil {
		return err
	}
	x.detach(ctx, done)
	return nil
}

// candidates lista os nós com chegada não consumida, em ordem de declaração.
func (x *execution[S]) candidates() []string {
	var candidates []string
	for _, name := range x.r.order {
		if x.arrived[name] {
			candidates = append(candidates, name)
		}
	}
	return candidates
}

// advance aplica a regra de join da seção 4.2 sobre candidates e devolve a
// próxima fronteira em ordem de declaração. JoinAny dispara sempre; JoinAll
// espera enquanto outro candidato o alcança de ida. Sobre o DAG sem arestas
// de retorno "espera por" é ordem parcial, então sempre há candidato mínimo
// e a execução avança. Não consome chegada: isso acontece quando o nó
// começa a executar.
func (x *execution[S]) advance(candidates []string) []string {
	var frontier []string
	for _, n := range candidates {
		if x.r.nodes[n].cfg.join == JoinAll && x.blocked(candidates, n) {
			continue
		}
		frontier = append(frontier, n)
	}
	return frontier
}

// blocked diz se o candidato n espera: algum outro candidato o alcança de ida.
func (x *execution[S]) blocked(candidates []string, n string) bool {
	for _, u := range candidates {
		if u != n && x.r.forward[u][n] {
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
func (x *execution[S]) runNode(ctx context.Context, state *S, name string, step int) error {
	n := x.r.nodes[name]
	for attempt := 1; ; attempt++ {
		err := x.attempt(ctx, state, name, n, step, attempt)
		if err == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			if errors.Is(err, ctxErr) {
				return ctxErr
			}
			return &NodeError{RunID: x.cfg.runID, Node: name, Step: step, Attempt: attempt, Err: err}
		}
		if attempt >= n.cfg.attempts {
			return &NodeError{RunID: x.cfg.runID, Node: name, Step: step, Attempt: attempt, Err: err}
		}
		if err := waitBackoff(ctx, n.cfg.backoff); err != nil {
			return err
		}
	}
}

// attempt roda uma tentativa do nó com o timeout configurado, envolvida pelos
// hooks e pelos eventos. Vive fora do laço de retry para que o cancel do
// timeout rode a cada tentativa. Cada hook recebe no NodeEnd o ctx que o
// próprio NodeStart devolveu; o nó recebe o ctx final da cadeia.
func (x *execution[S]) attempt(ctx context.Context, state *S, name string, n node[S], step, attempt int) error {
	if n.cfg.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, n.cfg.timeout)
		defer cancel()
	}
	info := x.info(name, step, attempt)
	ctxs := make([]context.Context, len(x.cfg.hooks))
	for i, h := range x.cfg.hooks {
		ctx = h.NodeStart(ctx, info)
		ctxs[i] = ctx
	}
	x.emit(EventNodeStart, info, nil)
	err := n.fn(ctx, state)
	x.emit(EventNodeEnd, info, err)
	for i := len(x.cfg.hooks) - 1; i >= 0; i-- {
		x.cfg.hooks[i].NodeEnd(ctxs[i], info, err)
	}
	return err
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
