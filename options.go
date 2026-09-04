package flow

import "time"

// NodeOption configura um nó em Add.
type NodeOption func(*nodeConfig)

// nodeConfig guarda as opções de um nó. hasTimeout distingue WithTimeout(0),
// que é erro de Compile, de "sem timeout".
type nodeConfig struct {
	timeout    time.Duration
	hasTimeout bool
	attempts   int
	backoff    time.Duration
	join       Join
	failure    Failure
}

// Join define quando um nó que recebeu chegadas dispara. O runner não conta
// predecessores: ao fim de cada superstep ele ordena os candidatos por
// alcançabilidade estática, como descreve a seção 4.2 da spec.
type Join int

const (
	// JoinAll espera enquanto outro candidato ainda alcança o nó, e dispara
	// com tudo que chegou até então. Ramo que o Router não escolheu nunca
	// vira candidato, então não trava o join. É o zero value de propósito:
	// é o default seguro para dois ramos convergindo.
	JoinAll Join = iota
	// JoinAny dispara na primeira chegada; as demais da mesma onda são
	// descartadas.
	JoinAny
)

// Failure define o que acontece quando um nó esgota as tentativas com erro.
type Failure int

const (
	// FailureAbort encerra a execução e cancela os irmãos do superstep. É o
	// zero value de propósito: falha é fatal por default.
	FailureAbort Failure = iota
	// FailureSkip registra o erro em Result.Errors e segue. O nó entra em
	// Path e as arestas estáticas dele disparam normalmente, mas o Router de
	// um Branch não é chamado: rotear sobre um estado que o nó não produziu
	// é decidir em cima de lixo. Na prática, nó pulado com Branch encerra
	// aquele ramo.
	FailureSkip
)

// WithTimeout limita a duração de cada tentativa do nó, não do nó inteiro:
// o pior caso é attempts × (timeout + backoff), e quem contém isso é o
// WithBudget. Precisa ser positivo.
func WithTimeout(d time.Duration) NodeOption {
	return func(c *nodeConfig) {
		c.timeout = d
		c.hasTimeout = true
	}
}

// WithRetry permite até attempts execuções do nó, esperando backoff entre
// elas. attempts é o total, incluindo a primeira execução: WithRetry(3, d)
// executa no máximo três vezes, e WithRetry(1, d) equivale a não usar a
// option. NodeError.Attempt segue a mesma contagem, a partir de 1: a falha
// da primeira execução chega com Attempt: 1. Precisa de attempts >= 1 e
// backoff >= 0.
func WithRetry(attempts int, backoff time.Duration) NodeOption {
	return func(c *nodeConfig) {
		c.attempts = attempts
		c.backoff = backoff
	}
}

// WithJoin define a regra de join do nó. Default JoinAll.
func WithJoin(j Join) NodeOption {
	return func(c *nodeConfig) { c.join = j }
}

// WithOnFailure define o que acontece quando o nó falha. Default FailureAbort.
func WithOnFailure(p Failure) NodeOption {
	return func(c *nodeConfig) { c.failure = p }
}

// RunOption configura uma execução em Run. Não é genérica em S de propósito:
// pode ser guardada em config e reutilizada entre grafos.
type RunOption func(*runConfig)

// defaultMaxSteps é o limite de supersteps quando WithMaxSteps não é usado.
const defaultMaxSteps = 50

// runConfig guarda as opções de uma execução. hasBudget distingue
// WithBudget(0), que é erro de uso, de "sem budget".
type runConfig struct {
	maxSteps   int
	budget     time.Duration
	hasBudget  bool
	sequential bool
}

// WithMaxSteps limita o número de supersteps de um Run. Default 50. Estourar
// devolve ErrMaxSteps. Precisa ser >= 1.
func WithMaxSteps(n int) RunOption {
	return func(c *runConfig) { c.maxSteps = n }
}

// WithBudget limita a duração da execução inteira. Vira deadline do contexto
// que desce até dentro dos nós e é checado antes de cada superstep; estourar
// devolve ErrBudget, distinguível de falha de nó e de deadline que já vinha
// no contexto do chamador. Precisa ser positivo.
func WithBudget(d time.Duration) RunOption {
	return func(c *runConfig) {
		c.budget = d
		c.hasBudget = true
	}
}

// WithSequential executa cada fronteira inline, em ordem de declaração, sem
// goroutine. É o botão de determinismo para teste: Path e Errors já são
// determinísticos em paralelo, mas a ordem das escritas no estado não.
func WithSequential() RunOption {
	return func(c *runConfig) { c.sequential = true }
}
