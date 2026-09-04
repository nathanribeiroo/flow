// Package flow executa um grafo de nós que compartilham um estado tipado.
//
// Um nó é uma função Go comum: recebe o contexto e o estado por ponteiro e o
// muta no lugar. Nenhum tipo deste pacote aparece na assinatura dele. O
// builder Graph acumula nós, arestas estáticas, branches condicionais e
// arestas destacadas; Compile valida a topologia inteira de uma vez e devolve
// um Runner imutável, que atende quantas execuções simultâneas forem
// necessárias sem lock.
//
// Exemplo mínimo:
//
//	type State struct {
//		Question string
//		Answer   string
//	}
//
//	answer := func(ctx context.Context, s *State) error {
//		s.Answer = "42, for " + s.Question
//		return nil
//	}
//
//	runner, err := flow.New[State]("qa").
//		Add("answer", answer, flow.WithTimeout(5*time.Second)).
//		Start("answer").
//		Edge("answer", flow.End).
//		Compile()
//	if err != nil {
//		return err
//	}
//
//	state := State{Question: "everything"}
//	res, err := runner.Run(ctx, &state)
//
// A execução anda em supersteps: a fronteira inteira roda em paralelo, os
// roteamentos são coletados, e o join decide a próxima fronteira. Dentro de
// um superstep cada nó escreve só nos campos que são dele; WithConflictCheck
// verifica isso em desenvolvimento. Retry, timeout, budget, aresta
// destacada, hooks, canal de eventos, checkpoint com retomada e diagrama
// Mermaid são options e métodos do Runner, documentados em cada símbolo.
package flow
