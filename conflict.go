package flow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
)

// ConflictError localiza escrita concorrente detectada por WithConflictCheck:
// dois ou mais nós do mesmo superstep escreveram no mesmo campo de primeiro
// nível do estado. Nodes vem em ordem de declaração.
type ConflictError struct {
	RunID string
	Step  int
	Field string
	Nodes []string
}

var _ error = (*ConflictError)(nil)

// Error implementa error. RunID fica fora da mensagem, como em NodeError.
func (e *ConflictError) Error() string {
	return fmt.Sprintf("flow: conflict at step %d: field %q written by %v", e.Step, e.Field, e.Nodes)
}

// conflictRecord acumula, dentro de um superstep sequencial, quais nós
// escreveram em cada campo de primeiro nível do estado. err guarda a falha
// de snapshot, se houver.
type conflictRecord struct {
	writes map[string][]string
	err    error
}

// snapshot serializa o estado e o separa em campos de primeiro nível. Estado
// que não serializa como objeto JSON é erro: não há campo para comparar.
func snapshot[S any](state *S) (map[string]json.RawMessage, error) {
	raw, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	return fields, nil
}

// record anota name como autor de todo campo cujo valor difere byte a byte
// entre before e after, incluindo os que apareceram ou sumiram.
func (c *conflictRecord) record(name string, before, after map[string]json.RawMessage) {
	if c.writes == nil {
		c.writes = make(map[string][]string)
	}
	for field, value := range after {
		if !slices.Equal(before[field], value) {
			c.writes[field] = append(c.writes[field], name)
		}
	}
	for field := range before {
		if _, kept := after[field]; !kept {
			c.writes[field] = append(c.writes[field], name)
		}
	}
}

// check devolve a falha de snapshot, se houve, ou um *ConflictError por
// campo escrito por mais de um nó, em ordem alfabética de campo e combinados
// com errors.Join quando há mais de um. Receiver nil quer dizer que o passo
// não foi verificado.
func (c *conflictRecord) check(runID string, step int) error {
	if c == nil {
		return nil
	}
	if c.err != nil {
		return fmt.Errorf("flow: conflict check at step %d: %w", step, c.err)
	}
	var conflicts []error
	for _, field := range slices.Sorted(maps.Keys(c.writes)) {
		if nodes := c.writes[field]; len(nodes) > 1 {
			conflicts = append(conflicts, &ConflictError{RunID: runID, Step: step, Field: field, Nodes: nodes})
		}
	}
	switch len(conflicts) {
	case 0:
		return nil
	case 1:
		return conflicts[0]
	default:
		return errors.Join(conflicts...)
	}
}

// checked roda um nó com snapshot antes e depois e anota em rec os campos que
// ele escreveu. Devolve false se um snapshot falhou; rec.err guarda o motivo,
// e o outcome devolvido descreve o que chegou a acontecer com o nó.
func (x *execution[S]) checked(ctx context.Context, rec *conflictRecord, name string, step int) (outcome, bool) {
	before, err := snapshot(x.state)
	if err != nil {
		rec.err = err
		return outcome{}, false
	}
	o := x.perform(ctx, name, step)
	after, err := snapshot(x.state)
	if err != nil {
		rec.err = err
		return o, false
	}
	rec.record(name, before, after)
	return o, true
}
