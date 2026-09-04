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
}

// WithTimeout limita a duração de cada tentativa do nó. Precisa ser positivo.
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

// RunOption configura uma execução em Run. Não é genérica em S de propósito:
// pode ser guardada em config e reutilizada entre grafos.
type RunOption func(*runConfig)

// defaultMaxSteps é o limite de supersteps quando WithMaxSteps não é usado.
const defaultMaxSteps = 50

// runConfig guarda as opções de uma execução.
type runConfig struct {
	maxSteps int
}

// WithMaxSteps limita o número de supersteps de um Run. Default 50. Estourar
// devolve ErrMaxSteps. Precisa ser >= 1.
func WithMaxSteps(n int) RunOption {
	return func(c *runConfig) { c.maxSteps = n }
}
