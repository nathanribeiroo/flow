package flow

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
)

// Result descreve o desfecho de um Run.
type Result struct {
	Steps   int           // supersteps concluídos
	Path    []string      // nós concluídos, em ordem de conclusão
	Elapsed time.Duration // duração total, preenchida mesmo em erro
}

// Run executa o grafo sobre state até a fronteira esvaziar. O chamador é
// dono de state durante a execução. Erro de nó volta embrulhado em
// *NodeError; cancelamento de ctx volta como o erro do próprio contexto.
// Result descreve o que foi concluído, mesmo em caso de erro.
func (r *Runner[S]) Run(ctx context.Context, state *S, opts ...RunOption) (res Result, err error) {
	cfg := runConfig{maxSteps: defaultMaxSteps}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.maxSteps < 1 {
		return res, fmt.Errorf("flow: max steps must be at least 1, got %d", cfg.maxSteps)
	}
	if state == nil {
		return res, errors.New("flow: state is nil")
	}

	started := time.Now()
	defer func() { res.Elapsed = time.Since(started) }()

	frontier := []string{r.start}
	for len(frontier) > 0 {
		if res.Steps >= cfg.maxSteps {
			return res, fmt.Errorf("%w: limit %d", ErrMaxSteps, cfg.maxSteps)
		}
		if err = ctx.Err(); err != nil {
			return res, err
		}
		frontier, err = r.step(ctx, state, frontier, &res)
		if err != nil {
			return res, err
		}
		res.Steps++
	}
	return res, nil
}

// step executa a fronteira em ordem, inline, e devolve a próxima: os alvos
// roteados na ordem em que apareceram, sem End e sem repetição.
func (r *Runner[S]) step(ctx context.Context, state *S, frontier []string, res *Result) ([]string, error) {
	var next []string
	for _, name := range frontier {
		if err := r.runNode(ctx, state, name, res.Steps+1); err != nil {
			return nil, err
		}
		res.Path = append(res.Path, name)
		targets, err := r.route(ctx, state, name)
		if err != nil {
			return nil, err
		}
		for _, t := range targets {
			if t != End && !slices.Contains(next, t) {
				next = append(next, t)
			}
		}
	}
	return next, nil
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
// por tentativa. Cancelamento de ctx interrompe o retry e volta como erro de
// contexto, nunca como *NodeError.
func (r *Runner[S]) runNode(ctx context.Context, state *S, name string, step int) error {
	n := r.nodes[name]
	for attempt := 1; ; attempt++ {
		err := attemptNode(ctx, n, state)
		if err == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
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
