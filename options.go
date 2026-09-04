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
	hooks      []Hook
	events     chan<- Event
	runID      string
	hasRunID   bool // WithRunID foi passado; Resume rejeita
	store      Checkpointer
	conflict   bool
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

// WithHook registra um Hook. É acumulativo: NodeStart roda na ordem de
// registro, NodeEnd na inversa, como defer. Hook nil é erro de uso,
// reportado no início do Run.
func WithHook(h Hook) RunOption {
	return func(c *runConfig) { c.hooks = append(c.hooks, h) }
}

// WithEvents publica os eventos da execução em ch. O envio é não-bloqueante:
// consumidor lento perde evento e nunca segura o runner. Quem cria o canal
// fecha o canal, e só depois de Run retornar e de Wait drenar as destacadas;
// a lib nunca fecha um canal que não criou.
func WithEvents(ch chan<- Event) RunOption {
	return func(c *runConfig) { c.events = ch }
}

// WithRunID define o id da execução, carimbado em Result, NodeInfo e
// NodeError. Sem ele, ou com id vazio, a lib gera 16 bytes de crypto/rand em
// hex, e Result.RunID é como o chamador descobre o id gerado.
func WithRunID(id string) RunOption {
	return func(c *runConfig) {
		c.runID = id
		c.hasRunID = true
	}
}

// WithCheckpoint grava um Checkpoint em store ao fim de cada superstep
// concluído, inclusive o terminal, e é o que Resume usa para retomar. Falha
// em Save ou na serialização do estado aborta a execução: run que não dá
// para retomar não pode parecer que dá. O estado inicial é serializado antes
// do passo 1, e Run falha inline se não conseguir. Retomar reexecuta o passo
// interrompido, então os nós precisam ser idempotentes: a garantia é
// at-least-once para aquele passo.
func WithCheckpoint(store Checkpointer) RunOption {
	return func(c *runConfig) { c.store = store }
}

// WithConflictCheck detecta dois nós do mesmo superstep escrevendo no mesmo
// campo de primeiro nível do estado e aborta com *ConflictError, um por
// campo. Implica WithSequential: em paralelo não dá para atribuir campo a
// nó. Tira snapshot JSON antes e depois de cada nó de um fan-out, então o
// estado precisa serializar como objeto JSON, validado no início do Run, e o
// custo é alto: ferramenta de desenvolvimento e de CI, nunca de produção.
// Escrita de nó pulado por FailureSkip conta: a ferramenta detecta corrida,
// não intenção.
//
// A granularidade é o campo de primeiro nível, e daí sai um falso positivo
// por desenho: dois nós que escrevem em subcampos distintos de um mesmo
// struct aninhado são reportados como conflito naquele campo. A saída é dar
// a cada nó um campo de primeiro nível só dele.
func WithConflictCheck() RunOption {
	return func(c *runConfig) {
		c.conflict = true
		c.sequential = true
	}
}
