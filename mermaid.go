package flow

import (
	"slices"
	"strings"
)

// Mermaid devolve a topologia em sintaxe flowchart. Os ids são sanitizados
// (prefixo n_ e tudo fora de [A-Za-z0-9_] vira _) porque end, graph e class
// são palavras reservadas e nome de nó é string livre; o nome real vai no
// label. Aresta cheia é estática, |branch| é condicional e tracejada é
// destacada. Nós saem em ordem de Add, End por último e só se algum nó
// apontar para ele.
func (r *Runner[S]) Mermaid() string {
	var b strings.Builder
	b.WriteString("flowchart TD\n")
	usesEnd := false
	for _, name := range r.order {
		b.WriteString("    " + mermaidID(name) + mermaidLabel(name) + "\n")
		if slices.Contains(r.edges[name], End) || slices.Contains(r.branches[name].targets, End) {
			usesEnd = true
		}
	}
	if usesEnd {
		b.WriteString("    " + mermaidID(End) + mermaidLabel(End) + "\n")
	}
	for _, name := range r.order {
		from := mermaidID(name)
		for _, to := range r.edges[name] {
			b.WriteString("    " + from + " --> " + mermaidID(to) + "\n")
		}
		for _, to := range r.branches[name].targets {
			b.WriteString("    " + from + " -->|branch| " + mermaidID(to) + "\n")
		}
		for _, to := range r.detaches[name] {
			b.WriteString("    " + from + " -.-> " + mermaidID(to) + "\n")
		}
	}
	return b.String()
}

// mermaidID devolve um id seguro para o Mermaid: prefixo n_ e toda rune fora
// de [A-Za-z0-9_] trocada por _.
func mermaidID(name string) string {
	slug := strings.Map(func(c rune) rune {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_':
			return c
		}
		return '_'
	}, name)
	return "n_" + slug
}

// mermaidLabel devolve o nome entre aspas como label, escapando aspas
// internas com a entidade #quot; do Mermaid.
func mermaidLabel(name string) string {
	return `["` + strings.ReplaceAll(name, `"`, "#quot;") + `"]`
}
