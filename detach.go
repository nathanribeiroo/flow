package flow

import "context"

// detach dispara, no fim de um superstep limpo, as destacadas dos nós
// concluídos em done. A cópia rasa do estado é tirada agora, depois de todos
// os irmãos terminarem: copiar no instante da conclusão competiria com
// irmãos ainda escrevendo. Consequência: a destacada enxerga o que o passo
// inteiro escreveu. Cada destacada recebe a própria cópia, e o que ela
// escrever é descartado; ponteiro dentro de S continua compartilhado.
func (x *execution[S]) detach(ctx context.Context, done []string) {
	step := x.res.Steps + 1
	for _, from := range done {
		for _, to := range x.r.detaches[from] {
			snapshot := *x.state
			x.r.wg.Go(func() { x.r.runDetached(ctx, to, &snapshot, step) })
		}
	}
}

// runDetached executa um nó destacado fora do fluxo principal: contexto sem
// o cancelamento do Run, com o timeout obrigatório valendo por tentativa.
func (r *Runner[S]) runDetached(ctx context.Context, name string, state *S, step int) {
	// Descarte deliberado: a seção 4.4 da spec manda o erro da destacada para
	// Hook e canal de eventos, que só existem na v0.3. Ele não entra em
	// Result nem aborta nada.
	_ = r.runNode(context.WithoutCancel(ctx), state, name, step)
}

// Wait espera as destacadas ainda vivas terminarem, ou ctx encerrar, o que
// vier primeiro. Chame depois de parar de aceitar novos Run: registrar uma
// destacada enquanto um Wait que já zerou está em curso é uso indevido do
// WaitGroup, e a ordem do shutdown é responsabilidade de quem chama.
func (r *Runner[S]) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
