package flow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Hook é síncrono e existe para tracing e métricas. NodeStart devolve o
// contexto enriquecido, que é o que faz o span do nó envolver a execução
// real. Os dois disparam por tentativa, para nó comum e destacado. Com mais
// de um hook, NodeStart roda na ordem de registro com o ctx encadeado, o nó
// recebe o ctx final, e NodeEnd roda na ordem inversa recebendo o ctx que o
// próprio NodeStart devolveu. Pânico em hook não é recuperado.
type Hook interface {
	NodeStart(ctx context.Context, info NodeInfo) context.Context
	NodeEnd(ctx context.Context, info NodeInfo, err error)
}

// NodeInfo identifica uma tentativa de nó dentro de uma execução. Em
// EventStepEnd e EventRunEnd, Node e Attempt ficam zerados.
type NodeInfo struct {
	RunID   string
	Graph   string
	Version string
	Node    string
	Step    int
	Attempt int
}

// EventKind é o tipo de um Event.
type EventKind int

const (
	// EventUnknown é o zero value e nunca é emitido.
	EventUnknown EventKind = iota
	// EventNodeStart marca o início de uma tentativa de nó.
	EventNodeStart
	// EventNodeEnd marca o fim de uma tentativa de nó, com o erro dela em Err.
	EventNodeEnd
	// EventStepEnd marca um superstep concluído; Info.Step é o número dele.
	EventStepEnd
	// EventRunEnd marca o fim do Run, com o erro dele em Err e Info.Step
	// igual a Result.Steps.
	EventRunEnd
)

// Event é o que sai pelo canal de WithEvents. O envio é não-bloqueante.
type Event struct {
	Kind EventKind
	Info NodeInfo
	Err  error
	At   time.Time
}

// newRunID gera 16 bytes de crypto/rand em hex. Desde o Go 1.24 rand.Read
// nunca devolve erro; se devolver, o gerador do sistema sumiu, e isso é
// estado impossível para uma biblioteca resolver.
func newRunID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("flow: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// info monta o NodeInfo de uma tentativa desta execução.
func (x *execution[S]) info(node string, step, attempt int) NodeInfo {
	return NodeInfo{
		RunID:   x.cfg.runID,
		Graph:   x.r.name,
		Version: x.r.version,
		Node:    node,
		Step:    step,
		Attempt: attempt,
	}
}

// emit publica um evento sem bloquear: consumidor lento perde o evento e
// nunca segura o runner. A lib nunca fecha o canal, que é de quem o criou.
func (x *execution[S]) emit(kind EventKind, info NodeInfo, err error) {
	if x.cfg.events == nil {
		return
	}
	select {
	case x.cfg.events <- Event{Kind: kind, Info: info, Err: err, At: time.Now()}:
	default:
	}
}
