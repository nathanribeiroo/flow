package flow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
)

// Checkpoint é a foto de uma execução ao fim de um superstep concluído.
// Step é o número do último passo concluído. Pending são os candidatos
// daquele instante, todos os nós com chegada não consumida em ordem de
// declaração, antes de qualquer consumo: a retomada aplica a regra de join a
// eles e obtém a fronteira, então é a única fonte de verdade da execução.
type Checkpoint struct {
	RunID   string          `json:"run_id"`
	Version string          `json:"version"`
	Step    int             `json:"step"`
	Pending []string        `json:"pending"`
	State   json.RawMessage `json:"state"`
}

// Checkpointer persiste checkpoints por id de execução. Save substitui o
// checkpoint anterior do mesmo RunID. Load de id desconhecido devolve
// ErrCheckpointNotFound, embrulhado ou não; qualquer outro erro de Load
// volta embrulhado pelo Resume, distinguível por errors.Is.
type Checkpointer interface {
	Save(ctx context.Context, cp Checkpoint) error
	Load(ctx context.Context, runID string) (Checkpoint, error)
}

// MemoryStore é um Checkpointer em memória para teste e desenvolvimento.
// Não use em produção: some com o processo. É seguro para uso concorrente.
type MemoryStore struct {
	mu          sync.Mutex
	checkpoints map[string]Checkpoint
}

var _ Checkpointer = (*MemoryStore)(nil)

// NewMemoryStore cria um MemoryStore vazio.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{checkpoints: make(map[string]Checkpoint)}
}

// Save guarda uma cópia de cp. O estado e a lista de pendentes são copiados
// para o chamador não alterar o checkpoint por baixo.
func (m *MemoryStore) Save(_ context.Context, cp Checkpoint) error {
	cp.State = bytes.Clone(cp.State)
	cp.Pending = slices.Clone(cp.Pending)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkpoints[cp.RunID] = cp
	return nil
}

// Load devolve uma cópia do último checkpoint de runID, ou
// ErrCheckpointNotFound sem embrulhar: Resume acrescenta o id.
func (m *MemoryStore) Load(_ context.Context, runID string) (Checkpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp, ok := m.checkpoints[runID]
	if !ok {
		return Checkpoint{}, ErrCheckpointNotFound
	}
	cp.State = bytes.Clone(cp.State)
	cp.Pending = slices.Clone(cp.Pending)
	return cp, nil
}

// save grava o checkpoint do passo que acabou de concluir, com Pending igual
// aos candidatos antes do consumo. Sem store não faz nada. Falha na
// serialização ou no Save volta embrulhada e aborta a execução.
func (x *execution[S]) save(ctx context.Context, pending []string) error {
	if x.cfg.store == nil {
		return nil
	}
	state, err := json.Marshal(x.state)
	if err != nil {
		return fmt.Errorf("flow: saving checkpoint at step %d: %w", x.res.Steps, err)
	}
	cp := Checkpoint{
		RunID:   x.cfg.runID,
		Version: x.r.version,
		Step:    x.res.Steps,
		Pending: slices.Clone(pending),
		State:   state,
	}
	if cp.Pending == nil {
		cp.Pending = []string{}
	}
	if err := x.cfg.store.Save(ctx, cp); err != nil {
		return fmt.Errorf("flow: saving checkpoint at step %d: %w", x.res.Steps, err)
	}
	return nil
}

// Resume retoma a execução runID a partir do último checkpoint do store de
// WithCheckpoint, que é obrigatório; WithRunID é rejeitado porque o id é o
// parâmetro. A ordem de verificação é Load, id do checkpoint igual ao
// pedido, versão, decodificação do estado em um S novo e existência de todo
// nó de Pending; só então roda, a partir
// do passo seguinte ao gravado. O passo interrompido reexecuta. WithMaxSteps
// conta a partir do Step do checkpoint; WithBudget começa do zero. Result.Path
// e Result.Errors são só desta invocação. Checkpoint com Pending vazio é de
// run que terminou: nada roda e o estado final volta como está.
func (r *Runner[S]) Resume(ctx context.Context, runID string, opts ...RunOption) (*S, Result, error) {
	cfg, err := configure(opts)
	if err != nil {
		return nil, Result{}, err
	}
	if cfg.hasRunID {
		return nil, Result{}, errors.New("flow: resume does not accept WithRunID")
	}
	if cfg.store == nil {
		return nil, Result{}, errors.New("flow: resume requires WithCheckpoint")
	}
	cp, err := cfg.store.Load(ctx, runID)
	if err != nil {
		return nil, Result{}, fmt.Errorf("flow: loading checkpoint %q: %w", runID, err)
	}
	if cp.RunID != runID {
		return nil, Result{}, fmt.Errorf("flow: checkpoint %q belongs to run %q", runID, cp.RunID)
	}
	if cp.Version != r.version {
		return nil, Result{}, fmt.Errorf("%w: checkpoint %s, runner %s", ErrVersionMismatch, cp.Version, r.version)
	}
	state := new(S)
	if err := json.Unmarshal(cp.State, state); err != nil {
		return nil, Result{}, fmt.Errorf("flow: decoding checkpoint %q state: %w", runID, err)
	}
	for _, name := range cp.Pending {
		if _, ok := r.nodes[name]; !ok {
			return nil, Result{}, fmt.Errorf("flow: checkpoint %q pending node %q does not exist", runID, name)
		}
	}
	cfg.runID = runID
	x := r.newExecution(cfg, state)
	x.res.Steps = cp.Step
	for _, name := range cp.Pending {
		x.arrived[name] = true
	}
	res, err := x.loop(ctx, x.advance(x.candidates()))
	return state, res, err
}
