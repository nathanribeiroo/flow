package flow

import (
	"context"
	"maps"
	"slices"
)

// Node é a unidade de trabalho do grafo. Recebe o estado por ponteiro e o
// muta no lugar. Erro retornado interrompe a execução.
type Node[S any] func(ctx context.Context, state *S) error

// Router decide os próximos nós a partir do estado. Devolve fatia porque
// roteamento também faz fan-out.
type Router[S any] func(ctx context.Context, state *S) []string

// End é o nó terminal reservado. Rotear para End encerra aquele ramo.
const End = "end"

// node é a definição de um nó: função e configuração.
type node[S any] struct {
	fn  Node[S]
	cfg nodeConfig
}

// edge é uma aresta estática declarada em Edge.
type edge struct {
	from string
	to   string
}

// branch é uma aresta condicional declarada em Branch.
type branch[S any] struct {
	from    string
	route   Router[S]
	targets []string
}

// Graph é o builder do grafo. Os métodos encadeiam e não devolvem erro:
// problemas são acumulados e reportados juntos em Compile. Não é seguro
// para uso concorrente.
type Graph[S any] struct {
	name     string
	start    string
	order    []string // nomes em ordem de inserção; dita a ordem do Mermaid
	nodes    map[string]node[S]
	edges    []edge
	branches []branch[S]
	detaches []edge
	issues   []Issue // problemas detectados no builder
}

// New cria um builder vazio com o nome dado.
func New[S any](name string) *Graph[S] {
	return &Graph[S]{
		name:  name,
		nodes: make(map[string]node[S]),
	}
}

// Add registra um nó. Nome já existente substitui a função e as opções e
// mantém as arestas; é o mecanismo de stub em teste.
func (g *Graph[S]) Add(name string, fn Node[S], opts ...NodeOption) *Graph[S] {
	switch name {
	case "":
		g.issues = append(g.issues, Issue{Reason: "node name is empty"})
		return g
	case End:
		g.issues = append(g.issues, Issue{Node: name, Reason: "node name is reserved"})
		return g
	}
	cfg := nodeConfig{attempts: 1}
	for _, opt := range opts {
		opt(&cfg)
	}
	if _, exists := g.nodes[name]; !exists {
		g.order = append(g.order, name)
	}
	g.nodes[name] = node[S]{fn: fn, cfg: cfg}
	return g
}

// Start define o nó inicial.
func (g *Graph[S]) Start(name string) *Graph[S] {
	g.start = name
	return g
}

// Edge declara uma aresta estática de from para to. Duplicata é ignorada.
// to pode ser End.
func (g *Graph[S]) Edge(from, to string) *Graph[S] {
	e := edge{from: from, to: to}
	if !slices.Contains(g.edges, e) {
		g.edges = append(g.edges, e)
	}
	return g
}

// Branch declara roteamento condicional a partir de from. O Router só pode
// devolver nomes listados em targets; End precisa estar na lista para ser
// rota válida. Um segundo Branch no mesmo nó substitui o primeiro.
func (g *Graph[S]) Branch(from string, route Router[S], targets ...string) *Graph[S] {
	b := branch[S]{from: from, route: route, targets: slices.Clone(targets)}
	idx := slices.IndexFunc(g.branches, func(x branch[S]) bool { return x.from == from })
	if idx >= 0 {
		g.branches[idx] = b
		return g
	}
	g.branches = append(g.branches, b)
	return g
}

// Detach declara uma aresta destacada: to dispara quando from conclui, fora
// da fronteira e sobre uma cópia do estado. É ortogonal ao roteamento: from
// pode ter Edge ou Branch além de Detach. Duplicata é ignorada.
func (g *Graph[S]) Detach(from, to string) *Graph[S] {
	e := edge{from: from, to: to}
	if !slices.Contains(g.detaches, e) {
		g.detaches = append(g.detaches, e)
	}
	return g
}

// Clone devolve um builder independente, com nós, arestas, branches e
// destacadas copiados. Mudanças no clone não afetam o original.
func (g *Graph[S]) Clone() *Graph[S] {
	c := &Graph[S]{
		name:     g.name,
		start:    g.start,
		order:    slices.Clone(g.order),
		nodes:    maps.Clone(g.nodes),
		edges:    slices.Clone(g.edges),
		branches: make([]branch[S], 0, len(g.branches)),
		detaches: slices.Clone(g.detaches),
		issues:   slices.Clone(g.issues),
	}
	for _, b := range g.branches {
		b.targets = slices.Clone(b.targets)
		c.branches = append(c.branches, b)
	}
	return c
}
