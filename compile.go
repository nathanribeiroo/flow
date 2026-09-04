package flow

import (
	"fmt"
	"maps"
	"slices"
)

// Runner é o grafo compilado. É imutável: não guarda estado de execução e
// atende quantos Run simultâneos forem necessários sem lock.
type Runner[S any] struct {
	name     string
	start    string
	order    []string
	nodes    map[string]node[S]
	edges    map[string][]string // alvos estáticos por nó, em ordem de declaração
	branches map[string]branch[S]
}

// Name devolve o nome dado em New.
func (r *Runner[S]) Name() string { return r.name }

// Compile valida a topologia inteira e devolve o Runner. Qualquer problema
// devolve *CompileError com a lista completa. O builder continua utilizável
// e independente do Runner devolvido.
func (g *Graph[S]) Compile() (*Runner[S], error) {
	if issues := g.validate(); len(issues) > 0 {
		return nil, &CompileError{Graph: g.name, Issues: issues}
	}
	r := &Runner[S]{
		name:     g.name,
		start:    g.start,
		order:    slices.Clone(g.order),
		nodes:    maps.Clone(g.nodes),
		edges:    make(map[string][]string, len(g.nodes)),
		branches: make(map[string]branch[S], len(g.branches)),
	}
	for _, e := range g.edges {
		r.edges[e.from] = append(r.edges[e.from], e.to)
	}
	for _, b := range g.branches {
		b.targets = slices.Clone(b.targets)
		r.branches[b.from] = b
	}
	return r, nil
}

// validate aplica as validações da seção 4.6 da spec mais a regra de que um
// nó roteia de um jeito só. A ordem das issues é determinística: nós em
// ordem de inserção, arestas e branches em ordem de declaração.
func (g *Graph[S]) validate() []Issue {
	issues := slices.Clone(g.issues)
	report := func(node, format string, args ...any) {
		issues = append(issues, Issue{Node: node, Reason: fmt.Sprintf(format, args...)})
	}
	exists := func(name string) bool {
		_, ok := g.nodes[name]
		return ok
	}
	existsOrEnd := func(name string) bool { return name == End || exists(name) }

	// Adjacência e forma de roteamento por nó, usadas nas regras 4, 5 e 9.
	next := make(map[string][]string, len(g.nodes))
	hasEdge := make(map[string]bool, len(g.nodes))
	hasBranch := make(map[string]bool, len(g.branches))
	for _, e := range g.edges {
		next[e.from] = append(next[e.from], e.to)
		hasEdge[e.from] = true
	}
	for _, b := range g.branches {
		next[b.from] = append(next[b.from], b.targets...)
		hasBranch[b.from] = true
	}

	// 7 e 8: função presente, opções válidas e id de Mermaid único. Nome
	// vazio ou reservado é validado no Add.
	ids := make(map[string]string, len(g.nodes)) // id do Mermaid -> primeiro nó que o gerou
	for _, name := range g.order {
		n := g.nodes[name]
		if n.fn == nil {
			report(name, "node function is nil")
		}
		if id := mermaidID(name); ids[id] != "" {
			report(name, "mermaid id %q collides with node %q", id, ids[id])
		} else {
			ids[id] = name
		}
		if n.cfg.hasTimeout && n.cfg.timeout <= 0 {
			report(name, "timeout must be positive")
		}
		if n.cfg.attempts < 1 {
			report(name, "retry attempts must be at least 1")
		}
		if n.cfg.backoff < 0 {
			report(name, "retry backoff must not be negative")
		}
	}

	// 1: start declarado e existente.
	switch {
	case g.start == "":
		report("", "start node not declared")
	case !exists(g.start):
		report("", "start node %q does not exist", g.start)
	}

	// 2: toda ponta de aresta existe ou é End.
	for _, e := range g.edges {
		if !exists(e.from) {
			report(e.from, "edge source does not exist")
		}
		if !existsOrEnd(e.to) {
			report(e.from, "edge to unknown node %q", e.to)
		}
	}

	// 3: todo Branch tem router, ao menos um target, e todos existem.
	for _, b := range g.branches {
		if !exists(b.from) {
			report(b.from, "branch source does not exist")
		}
		if b.route == nil {
			report(b.from, "branch router is nil")
		}
		if len(b.targets) == 0 {
			report(b.from, "branch has no targets")
		}
		for _, t := range b.targets {
			if !existsOrEnd(t) {
				report(b.from, "branch to unknown node %q", t)
			}
		}
	}

	// 9: um nó roteia de um jeito só, por aresta estática ou por Branch.
	for _, name := range g.order {
		if hasEdge[name] && hasBranch[name] {
			report(name, "node has both static edges and a branch")
		}
	}

	// 4: todo nó é alcançável a partir do start. Pulada quando o start é
	// inválido, senão todo nó viraria "unreachable" e enterraria o erro real.
	if exists(g.start) {
		visited := map[string]bool{g.start: true}
		queue := []string{g.start}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			for _, t := range next[cur] {
				if t == End || visited[t] {
					continue
				}
				visited[t] = true
				queue = append(queue, t)
			}
		}
		for _, name := range g.order {
			if !visited[name] {
				report(name, "unreachable from start")
			}
		}
	}

	// 5: todo nó tem saída, estática ou por Branch.
	for _, name := range g.order {
		if !hasEdge[name] && !hasBranch[name] {
			report(name, "no outgoing edge")
		}
	}

	return issues
}
