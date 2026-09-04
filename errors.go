package flow

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrMaxSteps indica que a execução ultrapassou o limite de WithMaxSteps.
	ErrMaxSteps = errors.New("flow: max steps exceeded")

	// ErrUnknownTarget indica que um Router devolveu nome fora dos targets
	// declarados no Branch.
	ErrUnknownTarget = errors.New("flow: router returned an undeclared target")
)

// NodeError localiza a falha de um nó na execução. Step e Attempt começam
// em 1. Unwrap expõe o erro original para errors.Is e errors.As.
type NodeError struct {
	Node    string
	Step    int
	Attempt int
	Err     error
}

var _ error = (*NodeError)(nil)

// Error implementa error.
func (e *NodeError) Error() string {
	return fmt.Sprintf("flow: node %q failed at step %d attempt %d: %v", e.Node, e.Step, e.Attempt, e.Err)
}

// Unwrap devolve o erro original do nó.
func (e *NodeError) Unwrap() error { return e.Err }

// Issue é um problema de topologia encontrado no Compile. Node fica vazio
// em problema do grafo como um todo, como start ausente.
type Issue struct {
	Node   string
	Reason string
}

// CompileError agrega todos os problemas de topologia de uma vez.
type CompileError struct {
	Graph  string
	Issues []Issue
}

var _ error = (*CompileError)(nil)

// Error implementa error. Sai em linha única, com as issues separadas por
// ponto e vírgula, para compor bem quando embrulhado.
func (e *CompileError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "flow: graph %q: %d issue", e.Graph, len(e.Issues))
	if len(e.Issues) != 1 {
		b.WriteByte('s')
	}
	b.WriteByte(':')
	for i, issue := range e.Issues {
		if i > 0 {
			b.WriteByte(';')
		}
		b.WriteByte(' ')
		if issue.Node != "" {
			fmt.Fprintf(&b, "node %q: ", issue.Node)
		}
		b.WriteString(issue.Reason)
	}
	return b.String()
}
