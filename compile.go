package flow

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
)

// Runner é o grafo compilado. Não guarda estado de execução e atende
// quantos Run simultâneos forem necessários sem lock. A única exceção é o
// WaitGroup das destacadas, exigido por Wait: o princípio 5 da spec ganha
// do 3 aqui.
type Runner[S any] struct {
	name     string
	version  string
	start    string
	order    []string
	nodes    map[string]node[S]
	edges    map[string][]string // alvos estáticos por nó, em ordem de declaração
	branches map[string]branch[S]
	detaches map[string][]string        // alvos destacados por nó, em ordem de declaração
	forward  map[string]map[string]bool // forward[u][n]: u alcança n sem arestas de retorno
	wg       sync.WaitGroup             // destacadas vivas, drenadas por Wait
}

// Name devolve o nome dado em New.
func (r *Runner[S]) Name() string { return r.name }

// Version devolve o hash de topologia: os primeiros 12 hex do SHA-256 de uma
// serialização canônica com start, nós com Join e Failure, e todas as
// arestas. Nome do grafo, router, retry e timeout ficam de fora: mudá-los não
// muda o que uma fronteira salva significa.
func (r *Runner[S]) Version() string { return r.version }

// Compile valida a topologia inteira e devolve o Runner. Qualquer problema
// devolve *CompileError com a lista completa. O builder continua utilizável
// e independente do Runner devolvido.
func (g *Graph[S]) Compile() (*Runner[S], error) {
	if issues := g.validate(); len(issues) > 0 {
		return nil, &CompileError{Graph: g.name, Issues: issues}
	}
	r := &Runner[S]{
		name:     g.name,
		version:  g.version(),
		start:    g.start,
		order:    slices.Clone(g.order),
		nodes:    maps.Clone(g.nodes),
		edges:    make(map[string][]string, len(g.nodes)),
		branches: make(map[string]branch[S], len(g.branches)),
		detaches: make(map[string][]string, len(g.detaches)),
		forward:  reachability(g.order, forwardEdges(g.start, g.successors())),
	}
	for _, e := range g.edges {
		r.edges[e.from] = append(r.edges[e.from], e.to)
	}
	for _, b := range g.branches {
		b.targets = slices.Clone(b.targets)
		r.branches[b.from] = b
	}
	for _, e := range g.detaches {
		r.detaches[e.from] = append(r.detaches[e.from], e.to)
	}
	return r, nil
}

// successors devolve os alvos de cada nó por aresta estática e Branch, em
// ordem de declaração. Destacadas ficam de fora: não entram na fronteira.
func (g *Graph[S]) successors() map[string][]string {
	next := make(map[string][]string, len(g.nodes))
	for _, e := range g.edges {
		next[e.from] = append(next[e.from], e.to)
	}
	for _, b := range g.branches {
		next[b.from] = append(next[b.from], b.targets...)
	}
	return next
}

// reachability devolve, para cada nó, o conjunto dos nós alcançáveis por
// next. End nunca entra: é terminal.
func reachability(order []string, next map[string][]string) map[string]map[string]bool {
	reaches := make(map[string]map[string]bool, len(order))
	for _, from := range order {
		seen := make(map[string]bool)
		queue := []string{from}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			for _, t := range next[cur] {
				if t == End || seen[t] {
					continue
				}
				seen[t] = true
				queue = append(queue, t)
			}
		}
		reaches[from] = seen
	}
	return reaches
}

// forwardEdges devolve os sucessores sem as arestas de retorno: as que uma
// busca em profundidade a partir de start, seguindo arestas em ordem de
// declaração, encontra apontando para nó ainda na pilha. O que sobra é um
// DAG, e é sobre ele que o join ordena candidatos: aresta de retorno diz
// "o ciclo recomeça", não "este nó depende daquele".
func forwardEdges(start string, next map[string][]string) map[string][]string {
	const (
		unvisited = iota
		onStack
		finished
	)
	state := make(map[string]int, len(next))
	dag := make(map[string][]string, len(next))
	var visit func(u string)
	visit = func(u string) {
		state[u] = onStack
		for _, v := range next[u] {
			if v == End || state[v] == onStack {
				continue
			}
			dag[u] = append(dag[u], v)
			if state[v] == unvisited {
				visit(v)
			}
		}
		state[u] = finished
	}
	visit(start)
	return dag
}

// version calcula o hash de topologia da seção 4.7. Nomes saem com %q para
// nome com ponto e vírgula ou quebra de linha não colidir.
func (g *Graph[S]) version() string {
	var b strings.Builder
	fmt.Fprintf(&b, "start %q\n", g.start)
	for _, name := range slices.Sorted(maps.Keys(g.nodes)) {
		cfg := g.nodes[name].cfg
		fmt.Fprintf(&b, "node %q join=%d failure=%d\n", name, cfg.join, cfg.failure)
	}
	lines := make([]string, 0, len(g.edges)+len(g.branches)+len(g.detaches))
	for _, e := range g.edges {
		lines = append(lines, fmt.Sprintf("edge %q %q", e.from, e.to))
	}
	for _, br := range g.branches {
		for _, t := range br.targets {
			lines = append(lines, fmt.Sprintf("branch %q %q", br.from, t))
		}
	}
	for _, e := range g.detaches {
		lines = append(lines, fmt.Sprintf("detach %q %q", e.from, e.to))
	}
	slices.Sort(lines)
	for _, line := range lines {
		b.WriteString(line + "\n")
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])[:12]
}

// validate aplica as validações da seção 4.6 da spec. A ordem das issues é
// determinística: nós em ordem de inserção, arestas, branches e destacadas
// em ordem de declaração.
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

	// Forma de roteamento por nó, usada nas regras 4, 5, 6 e 9.
	hasEdge := make(map[string]bool, len(g.nodes))
	hasBranch := make(map[string]bool, len(g.branches))
	hasDetach := make(map[string]bool, len(g.detaches))
	isDetached := make(map[string]bool, len(g.detaches))
	for _, e := range g.edges {
		hasEdge[e.from] = true
	}
	for _, b := range g.branches {
		hasBranch[b.from] = true
	}
	for _, e := range g.detaches {
		hasDetach[e.from] = true
		isDetached[e.to] = true
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

	// 6: pontas de Detach existem; o alvo é sink e tem WithTimeout.
	for _, e := range g.detaches {
		if !exists(e.from) {
			report(e.from, "detach source does not exist")
		}
		if !exists(e.to) {
			report(e.from, "detach to unknown node %q", e.to)
		}
	}
	for _, name := range g.order {
		if !isDetached[name] {
			continue
		}
		if hasEdge[name] || hasBranch[name] || hasDetach[name] {
			report(name, "detach target has outgoing edges")
		}
		if !g.nodes[name].cfg.hasTimeout {
			report(name, "detach target has no timeout")
		}
	}

	// 9: um nó roteia de um jeito só, por aresta estática ou por Branch.
	for _, name := range g.order {
		if hasEdge[name] && hasBranch[name] {
			report(name, "node has both static edges and a branch")
		}
	}

	// 4: todo nó é alcançável a partir do start, por roteamento ou por
	// Detach. Pulada quando o start é inválido, senão todo nó viraria
	// "unreachable" e enterraria o erro real.
	if exists(g.start) {
		visited := maps.Clone(reachability(g.order, g.successors())[g.start])
		visited[g.start] = true
		for _, e := range g.detaches {
			if visited[e.from] {
				visited[e.to] = true
			}
		}
		for _, name := range g.order {
			if !visited[name] {
				report(name, "unreachable from start")
			}
		}
	}

	// 5: todo nó tem saída estática ou por Branch, ou é sink destacado.
	for _, name := range g.order {
		if !hasEdge[name] && !hasBranch[name] && !isDetached[name] {
			report(name, "no outgoing edge")
		}
	}

	return issues
}
