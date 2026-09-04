# SPEC — `flow`

**Status:** draft para implementação
**Alvo:** Go 1.25+
**Escopo:** runtime de grafo de estado para orquestração de agentes, em um único pacote

Este documento é o contrato de implementação. O `README.md` explica a ideia; aqui estão as decisões que precisam sair certas na primeira vez, porque mudar depois quebra quem já usa.

---

## 1. Princípios de projeto

Cinco, em ordem de precedência. Quando duas se chocam, ganha a de cima.

1. **O nó não conhece a lib.** A assinatura de um nó só menciona `context.Context` e o estado do usuário. Nada de tipo da lib vazando para dentro da função de negócio.
2. **Superfície pequena.** Cada símbolo exportado precisa justificar o próprio custo de manutenção. Na dúvida, não exporta — exportar depois é grátis, remover é breaking change.
3. **O grafo compilado é imutável.** Zero estado mutável no `Runner`. É isso que permite N execuções concorrentes sem lock.
4. **Erro de topologia morre no `Compile`.** O que dá para validar antes de rodar, valida antes de rodar, e reporta tudo de uma vez.
5. **Toda goroutine tem dono, saída e forma de ser esperada.** Sem exceção, incluindo aresta destacada.

Desvio registrado no princípio 3: o `Runner` carrega um `sync.WaitGroup` para as goroutines destacadas, porque a seção 4.4 exige `Wait` e goroutine sem forma de ser esperada é proibida pela regra 4 da seção 2.2. É o único estado mutável do `Runner`, e o princípio 5 ganha do 3 aqui. Restrição de uso, obrigatória no doc comment de `Wait`: chame depois de parar de aceitar novos `Run`. `WaitGroup.Add` concorrente com um `Wait` que já zerou é uso indevido documentado pela stdlib, e a sequência de shutdown é responsabilidade do servidor, não da lib.

---

## 2. Convenções de código

O projeto adota a doutrina **golang-master** (`pedronauck/skills`) como piso. O que segue são os pontos que mais afetam este código; a doutrina completa vale para tudo que não está aqui.

### 2.1 Idioma

- **Todo identificador em inglês**: pacote, tipo, função, variável, campo, constante, nome de arquivo, mensagem de erro, nome de teste.
- **Documentação, comentários e README em português**, escritos para serem lidos rápido.
- Doc comment de símbolo exportado começa com o identificador, mesmo em português: `// Run executa o grafo até a fronteira esvaziar.`
- Mensagem de erro segue o padrão Go: minúscula, sem pontuação final, prefixada com o pacote em sentinelas — `errors.New("flow: budget exceeded")`.

### 2.2 O piso inegociável

1. Todo erro é tratado ou carrega justificativa escrita no ponto do descarte. Nunca `_` pelado.
2. Wrap com `fmt.Errorf("contexto: %w", err)`; comparação só com `errors.Is` / `errors.As`. Nunca por string.
3. `ctx context.Context` é o primeiro parâmetro de qualquer função que faz I/O ou cruza fronteira de API. O `ctx` do chamador é propagado; nunca um `context.Background()` no meio do caminho.
4. Toda goroutine tem dono, caminho de saída e forma de ser esperada.
5. `panic` só para estado impossível. Falha esperada retorna erro.
6. Type assertion com comma-ok. Map e slice inicializados antes do uso.
7. Tipo exportado que existe para satisfazer interface carrega `var _ Interface = (*Type)(nil)` ao lado da definição.
8. Teste table-driven com subteste nomeado, rodando sob `-race`.
9. `gofmt`, `go vet ./...` e o linter passam com zero achado antes do commit.
10. Valor operacional (timeout, limite) vem de option, nunca hardcoded.

### 2.3 Nomenclatura

| Elemento | Convenção | Neste projeto |
| --- | --- | --- |
| Pacote | minúsculo, singular | `flow` |
| Interface | verbo + `-er` | `Checkpointer` |
| Sentinela | prefixo `Err` | `ErrBudget` |
| Tipo de erro | sufixo `Error` | `NodeError`, `CompileError` |
| Option | `With` + campo | `WithTimeout` |
| Enum | prefixo do tipo | `JoinAll`, `FailureSkip` |
| Receiver | 1–2 letras, consistente | `func (g *Graph[S])` |

Sem stuttering: o call site já diz `flow.`, então é `flow.New`, não `flow.NewFlow`.

### 2.4 Desvio consciente da doutrina

A doutrina pede sentinela no zero de enum (`StatusUnknown = iota`). Aqui, `JoinAll` e `FailureAbort` **são** o zero, porque são o default desejado e o zero value tem que ser o comportamento seguro. Um `JoinUnset` só adicionaria um ramo morto em todo lugar. Decisão registrada para não ser "corrigida" depois.

### 2.5 Layout

Pacote único na raiz do módulo. Sem `pkg/`, sem `internal/`, sem `cmd/`. Organização por arquivo, que escala muito além do tamanho deste projeto:

```
flow/
├── go.mod
├── README.md
├── SPEC.md
├── flow.go          // Node, Router, Graph, Add/Edge/Branch/Detach/Start/Clone
├── options.go       // NodeOption, RunOption
├── compile.go       // Runner, validação, hash de topologia
├── run.go           // laço de superstep, join, fan-out
├── detach.go        // arestas destacadas
├── errors.go        // sentinelas, NodeError, CompileError
├── observe.go       // Hook, Event, NodeInfo
├── checkpoint.go    // Checkpointer, Checkpoint, MemoryStore
├── mermaid.go       // Mermaid, MermaidTrace
├── example_test.go  // exemplos verificados pelo compilador
└── *_test.go
```

Nenhuma dependência além da stdlib e de `golang.org/x/sync/errgroup`.

Exceção única, e só em teste: `go.uber.org/goleak`. O pacote é dono de goroutines e precisa provar que não vaza nenhuma, e o *module graph pruning* mantém a dependência fora do grafo de quem consome a lib. Qualquer outra dependência, de produção ou de teste, é decisão a discutir antes.

---

## 3. API pública

Este é o contrato. Assinatura que não está aqui não é exportada.

### 3.1 Tipos fundamentais

```go
// Node é a unidade de trabalho do grafo. Recebe o estado por ponteiro e o muta
// no lugar. Erro retornado interrompe a execução, salvo FailureSkip.
type Node[S any] func(ctx context.Context, state *S) error

// Router decide os próximos nós a partir do estado. Devolve fatia porque
// roteamento também faz fan-out.
type Router[S any] func(ctx context.Context, state *S) []string

// End é o nó terminal reservado. Rotear para End encerra aquele ramo.
const End = "end"
```

### 3.2 Builder

```go
type Graph[S any] struct{ /* unexported */ }

func New[S any](name string) *Graph[S]

func (g *Graph[S]) Add(name string, fn Node[S], opts ...NodeOption) *Graph[S]
func (g *Graph[S]) Start(name string) *Graph[S]
func (g *Graph[S]) Edge(from, to string) *Graph[S]
func (g *Graph[S]) Branch(from string, route Router[S], targets ...string) *Graph[S]
func (g *Graph[S]) Detach(from, to string) *Graph[S]
func (g *Graph[S]) Clone() *Graph[S]
func (g *Graph[S]) Compile() (*Runner[S], error)
```

Regras do builder:

- Métodos encadeiam. Erro não é retornado a cada chamada; é **acumulado** e devolvido de uma vez no `Compile`. Configuração ruim morre na construção, não no primeiro uso.
- `Add` com nome já existente **substitui** o nó e mantém as arestas. É o mecanismo de stub em teste e de variação de topologia por domínio. Comportamento documentado, não acidente.
- `Clone` devolve builder novo e independente, com nós e arestas copiados. `Runner` não é clonável — quem varia é o builder.
- `Branch` exige `targets` porque a lib não consegue inspecionar a função de roteamento, e o Mermaid precisa das arestas possíveis. Roteador que devolve alvo não declarado é erro de execução (`ErrUnknownTarget`), não roteamento silencioso.
- Um segundo `Branch` no mesmo nó de origem **substitui** o primeiro, pelo mesmo motivo de `Add`.
- **Um nó roteia de um jeito só.** `Edge` e `Branch` saindo do mesmo nó é erro de `Compile` (validação 9). Somar as duas fontes obrigaria o leitor a juntar duas declarações separadas para saber o que acontece depois daquele nó. Quem precisa de "sempre C, às vezes Z" escreve um roteador que devolve os dois, e o comportamento fica explícito em um lugar só.
- `Detach` é ortogonal ao roteamento: um nó pode ter `Edge` (ou `Branch`) e `Detach` ao mesmo tempo.
- `End` só é rota válida para um nó se `End` estiver entre os `targets` declarados no `Branch` dele.

### 3.3 Options de nó

```go
type NodeOption func(*nodeConfig)

func WithTimeout(d time.Duration) NodeOption
func WithRetry(attempts int, backoff time.Duration) NodeOption
func WithJoin(j Join) NodeOption
func WithOnFailure(p Failure) NodeOption

type Join int

const (
    JoinAll Join = iota // espera todos os predecessores pendentes
    JoinAny             // dispara na primeira chegada
)

type Failure int

const (
    FailureAbort Failure = iota // erro do nó aborta a execução
    FailureSkip                 // erro é registrado, o fluxo continua
)
```

Semântica de `FailureSkip`: o nó conta como **concluído** para efeito de join, não contribui com estado, e o erro vai para `Result.Errors`. É o que faz consulta parcial a múltiplos KBs funcionar sem caso especial.

Roteamento de nó pulado, e a assimetria é proposital: as arestas **estáticas** dele disparam normalmente, que é o que faz `FailureSkip` significar "esse passo era opcional, siga"; o nó entra em `Path`; mas o `Router` de um `Branch` **não é chamado**, porque rotear a partir de um estado que o nó não conseguiu produzir é decidir em cima de lixo. Na prática, nó pulado com `Branch` encerra aquele ramo. Isso precisa estar escrito no doc comment de `FailureSkip`, senão surpreende.

Semântica de `WithRetry`: **`attempts` é o total de execuções, incluindo a primeira.** `WithRetry(3, d)` executa o nó no máximo três vezes; `WithRetry(1, d)` desliga a retentativa e é equivalente a não usar a option. O nome do parâmetro é a documentação, e a validação 8 (`attempts >= 1`) só faz sentido nessa leitura.

`NodeError.Attempt` conta a partir de 1, casando com a mesma contagem: a falha da primeira execução chega com `Attempt: 1`.

A tentativa é do nó, dentro do superstep. Irmão que já terminou não reexecuta. Entre tentativas, o laço checa cancelamento via `select` com `ctx.Done()`.

`WithTimeout` vale **por tentativa**, não pelo nó inteiro: cada tentativa recebe deadline novo. O pior caso de um nó é `attempts × (timeout + backoff)`, e quem limita isso é o `WithBudget` — mais uma razão para os dois coexistirem.

### 3.4 Runner

```go
type Runner[S any] struct{ /* unexported, imutável */ }

func (r *Runner[S]) Run(ctx context.Context, state *S, opts ...RunOption) (Result, error)
func (r *Runner[S]) Resume(ctx context.Context, runID string, opts ...RunOption) (*S, Result, error)
func (r *Runner[S]) Wait(ctx context.Context) error
func (r *Runner[S]) Mermaid() string
func (r *Runner[S]) MermaidTrace(res Result) string
func (r *Runner[S]) Version() string
func (r *Runner[S]) Name() string
```

`Wait` drena as goroutines destacadas ainda vivas, respeitando o `ctx`. Existe para shutdown gracioso, e é o que impede a aresta destacada de virar fire-and-forget.

### 3.5 Options de execução

```go
type RunOption func(*runConfig)

func WithMaxSteps(n int) RunOption        // default 50
func WithBudget(d time.Duration) RunOption
func WithHook(h Hook) RunOption           // acumulativo
func WithEvents(ch chan<- Event) RunOption
func WithCheckpoint(store Checkpointer) RunOption
func WithRunID(id string) RunOption
func WithSequential() RunOption           // determinismo em teste
func WithConflictCheck() RunOption        // caro, desenvolvimento apenas
```

`RunOption` não é genérico em `S` de propósito: nenhuma delas precisa do tipo do estado. Isso mantém as options armazenáveis em config e reutilizáveis entre grafos.

### 3.6 Resultado

```go
type Result struct {
    RunID   string
    Version string
    Steps   int
    Path    []string      // nós executados, agrupados por superstep
    Errors  []error       // erros de nós com FailureSkip
    Elapsed time.Duration
}
```

`Path` e `Errors` são **determinísticos mesmo com execução paralela**: dentro de um superstep as entradas são ordenadas pelo índice de declaração do nó, nunca pela ordem de conclusão. Ordem de conclusão obrigaria todo teste que toca esses campos a ordenar antes de comparar, e faria relatório de bug não reproduzir. O custo é uma ordenação por superstep.

### 3.7 Observabilidade

```go
// Hook é síncrono e existe para tracing e métricas. NodeStart devolve o
// contexto enriquecido, que é o que faz o span do nó envolver a execução real.
type Hook interface {
    NodeStart(ctx context.Context, info NodeInfo) context.Context
    NodeEnd(ctx context.Context, info NodeInfo, err error)
}

type NodeInfo struct {
    RunID   string
    Graph   string
    Version string
    Node    string
    Step    int
    Attempt int
}

type EventKind int

const (
    EventUnknown EventKind = iota
    EventNodeStart
    EventNodeEnd
    EventStepEnd
    EventRunEnd
)

type Event struct {
    Kind EventKind
    Info NodeInfo
    Err  error
    At   time.Time
}
```

Envio no canal é não-bloqueante (`select` com `default`). Consumidor lento perde evento; nunca segura o runner. Está documentado no `WithEvents`.

### 3.8 Checkpoint

```go
type Checkpoint struct {
    RunID    string          `json:"run_id"`
    Version  string          `json:"version"`
    Step     int             `json:"step"`
    Frontier []string        `json:"frontier"`
    State    json.RawMessage `json:"state"`
}

type Checkpointer interface {
    Save(ctx context.Context, cp Checkpoint) error
    Load(ctx context.Context, runID string) (Checkpoint, error)
}

// MemoryStore é implementação em memória para teste e desenvolvimento.
// Não use em produção.
type MemoryStore struct{ /* ... */ }

func NewMemoryStore() *MemoryStore
```

`var _ Checkpointer = (*MemoryStore)(nil)` ao lado da definição.

### 3.9 Erros

```go
var (
    ErrMaxSteps       = errors.New("flow: max steps exceeded")
    ErrBudget         = errors.New("flow: budget exceeded")
    ErrUnknownTarget  = errors.New("flow: router returned an undeclared target")
    ErrVersionMismatch = errors.New("flow: checkpoint belongs to a different graph version")
    ErrCheckpointNotFound = errors.New("flow: checkpoint not found")
)

// NodeError localiza a falha na execução.
type NodeError struct {
    RunID   string
    Node    string
    Step    int
    Attempt int
    Err     error
}

func (e *NodeError) Error() string
func (e *NodeError) Unwrap() error

// CompileError agrega todos os problemas de topologia de uma vez.
type CompileError struct {
    Graph  string
    Issues []Issue
}

type Issue struct {
    Node   string
    Reason string
}

func (e *CompileError) Error() string
```

Erro de nó **sempre** sai embrulhado em `*NodeError`. Falha de fan-out combina com `errors.Join`, e `errors.Is`/`As` atravessam a árvore.

Cancelamento é distinguível de falha de nó: se `ctx.Err() != nil`, o retorno é o erro de contexto, não `*NodeError`. Isso impede que o retry tente de novo algo que o cliente já abandonou.

**Erro de uso da API não ganha sentinela.** `state` nil, `WithMaxSteps` abaixo de 1, `WithBudget` não positivo: são bugs do chamador, não condições que alguém trate com `errors.Is`. Retornam erro inline com `fmt.Errorf`, prefixado com `flow:`, validados no início do `Run` — antes de qualquer hook disparar e de qualquer nó rodar. `panic` fica reservado para estado impossível de verdade; derrubar o processo de um chamador por causa de um ponteiro nil não é postura de biblioteca.

---

## 4. Semântica de execução

### 4.1 O laço

```
frontier := {start}
step := 0

para cada superstep:
    se frontier vazia            -> fim normal
    se step >= maxSteps          -> ErrMaxSteps
    se budget estourado          -> ErrBudget
    se ctx cancelado             -> ctx.Err()

    executa todos os nós da frontier
    coleta as decisões de roteamento
    resolve os joins
    frontier = próximos nós prontos
    checkpoint (se configurado)
    step++

```

**Fast-path obrigatório:** `len(frontier) == 1` executa inline, sem goroutine, sem `errgroup`. O caminho linear não paga pelo paralelismo que não usa. Isso não é otimização prematura; é o caso dominante.

**Fan-out:** `errgroup.WithContext`, **sem `SetLimit`**. A fronteira já é limitada pela topologia, e um limite configurável seria mais uma option para manter sem caso de uso real. `WithSequential` é o único botão.

**Erro no fan-out:** nó com `FailureAbort` cancela os irmãos; nó com `FailureSkip` não. Irmão que retorna erro apenas porque o contexto do grupo foi cancelado — `errors.Is(err, context.Canceled)` com o contexto do grupo já cancelado — é descartado: não é falha, é consequência. Dois abortos reais no mesmo passo são embrulhados cada um em `*NodeError` e combinados com `errors.Join`. Cancelamento do chamador e estouro de budget têm precedência sobre qualquer `*NodeError`.

**Ordem da fronteira:** a fronteira é o conjunto dos alvos do passo, sem repetição, **na ordem em que as arestas foram declaradas no builder**. Ordem de iteração de map é proibida em qualquer ponto que afete comportamento observável: ela quebraria o golden file do Mermaid, o `Path` do `Result` e a promessa de determinismo do `WithSequential`. Guarde as arestas em slice e use map só como índice.

### 4.2 Join

A regra da primeira versão desta spec estava errada e foi substituída. Ela contava predecessores pendentes, mas como todo nó da fronteira termina dentro do próprio superstep, o contador zerava sempre: `JoinAll` virava `JoinAny`, losango desbalanceado executava o convergente duas vezes, e `ErrStuck` nunca era alcançável.

A regra correta não conta predecessores. Ela **ordena candidatos**.

No `Compile`, precompute a matriz de alcançabilidade `reaches[a][b]` sobre o grafo estático — arestas estáticas mais alvos declarados em `Branch`, excluindo destacadas.

Ao fim de cada superstep:

1. `candidates` = nós com ao menos uma chegada ainda não consumida.
2. Candidato com `JoinAny` dispara na hora; as demais chegadas daquela onda são descartadas.
3. Candidato com `JoinAll` dispara **a menos que outro candidato o alcance** — ou seja, exista candidato `u != n` com `reaches[u][n]`. Nesse caso `n` espera: `u` roda antes e a chegada dele entra depois.
4. Alcançabilidade mútua (dois candidatos no mesmo ciclo) desempata por ordem de declaração: o primeiro dispara, o outro espera.
5. A fronteira é o conjunto dos que dispararam, em ordem de declaração.

Três consequências, todas obrigatórias:

- **Progresso é garantido.** Todo passo com candidatos produz ao menos um nó disparando, então fronteira vazia com chegada pendente não existe. Logo `ErrStuck` é inalcançável e **sai da spec**, pela mesma regra de sentinela da seção 6 que vale para todo o resto. Ciclo infinito continua contido por `WithMaxSteps`.
- **`Branch` que não dispara um ramo não trava o join.** O alvo que não recebeu chegada não é candidato, e o convergente roda com o que chegou. Isso é inegociável: `JoinAll` é o default, e "roteia para um de dois e converge" é topologia comum demais para deadlocar por padrão.
- **Losango desbalanceado roda o convergente uma vez.** Em `b → d` e `c → e → d`, no passo em que `b` chega, `e` ainda é candidato e alcança `d`, então `d` espera.

Custo: a matriz é O(V³) uma vez no `Compile`, com V na casa das dezenas, e a checagem por passo é O(candidatos²).

### 4.3 Estado e escrita concorrente

`Run` recebe `*S` e é o dono dele. O `Runner` não guarda estado.

Dentro de um superstep, nós paralelos escrevem no **mesmo** ponteiro. Contrato: cada nó escreve só nos campos dele. Não há lock no caminho quente, porque lock aqui pagaria por todo mundo para proteger a minoria.

`WithConflictCheck()` liga verificação por snapshot JSON antes/depois de cada nó do fan-out, e reporta sobreposição de campo como erro. Custo alto, documentado como ferramenta de desenvolvimento.

Extensão opcional, prevista mas não obrigatória na v0.5:

```go
type Merge[S any] func(dst *S, branches []*S) error
```

Ligada, muda o fan-out para cópia por ramo mais fusão explícita. Fica desligada por padrão — o custo de cópia não deve ser pago por quem segue a disciplina.

### 4.4 Aresta destacada

`Detach(from, to)` dispara `to` quando `from` conclui, sem entrar na fronteira.

Contrato, e cada item existe porque a alternativa é um bug:

- **Contexto:** `context.WithTimeout(context.WithoutCancel(ctx), timeout)`. Sem `WithoutCancel`, o destacado morre junto com o fluxo principal. Sem `WithTimeout`, ele roda para sempre. Timeout é obrigatório: `Detach` para nó sem `WithTimeout` é erro de compilação do grafo.
- **Estado:** recebe cópia rasa de `S`; escritas são descartadas. Ponteiro dentro de `S` continua compartilhado, e isso está documentado como responsabilidade do usuário.
- **Instante da cópia:** no **fim do superstep em que `from` concluiu**, nunca no instante da conclusão. Copiar enquanto irmãos ainda escrevem é corrida garantida, e o `-race` acusa. No caminho linear os dois instantes coincidem. Consequência a documentar: o destacado enxerga também o que os irmãos escreveram naquele passo.
- **Topologia:** o alvo precisa ser sink. Aresta saindo de alvo destacado é erro no `Compile`.
- **Ciclo de vida:** cada destacado registra em um `sync.WaitGroup` do `Runner`, drenado por `Wait`.
- **Erro:** vai para `Hook` e canal de eventos. Não entra em `Result` nem aborta nada. Antes da v0.3, onde nenhum dos dois existe, o erro é descartado com comentário justificando o descarte, como manda a regra 1 da seção 2.2.
- **Retry:** usa o mesmo caminho de tentativa dos demais nós, com timeout por tentativa.
- **Garantia:** nenhuma. Pod escalando para baixo evapora o destacado. Serve para telemetria, aquecimento de cache e pré-busca. **Nunca** para o que precisa acontecer — isso é mensagem em fila.

### 4.5 Cancelamento e orçamento

O `ctx` do `Run` desce até dentro do nó, então cancelamento chega em chamada HTTP em voo. Não é checagem entre supersteps.

`WithBudget(d)` aplica deadline ao contexto da execução **e** é verificado antes de abrir cada superstep. Erro é `ErrBudget`, distinguível de falha de nó, porque é ele que vira SLO.

Implementação: `context.WithTimeoutCause(ctx, d, ErrBudget)`. Estouro dentro de um nó chega lá como `context.DeadlineExceeded` e é reclassificado por `context.Cause` no retorno do `Run`, o que separa budget estourado de deadline que já vinha no contexto do chamador.

Timeout por nó protege o nó. Orçamento protege o turno. Três nós de 2s passam em qualquer timeout individual de 3s e ainda assim estouram uma janela de 5s.

Cancelamento no meio de fan-out deixa o estado parcialmente escrito. O contrato é: **estado de execução cancelada é inválido e deve ser descartado pelo chamador.** A lib não faz rollback; ela garante que você sabe que aconteceu.

### 4.6 Compile

`Compile` valida tudo e devolve `*CompileError` com a lista completa. A coluna de milestone diz quando cada validação passa a existir — uma validação sobre construto que ainda não foi implementado não tem o que validar.

| # | Validação | Milestone |
| --- | --- | --- |
| 1 | `Start` declarado e existente | v0.1 |
| 2 | Toda ponta de aresta existe ou é `End` | v0.1 |
| 3 | Todo `Branch` tem ao menos um target, e todos existem | v0.1 |
| 4 | Todo nó é alcançável a partir do start | v0.1 |
| 5 | Todo nó tem aresta de saída, ou é sink destacado, ou roteia para `End` | v0.1 |
| 6 | Alvo de `Detach` é sink e tem `WithTimeout` | v0.2 |
| 7 | Nome de nó não vazio, único, diferente de `End` | v0.1 |
| 8 | `WithRetry` com `attempts >= 1`; timeouts positivos | v0.1 |
| 9 | Um nó não tem `Edge` e `Branch` de saída ao mesmo tempo | v0.1 |

### 4.7 Hash de topologia

`Version()` é o SHA-256 dos primeiros 12 hex de uma serialização canônica: nós ordenados por nome com as opções que afetam semântica (`Join`, `Failure`, retry, timeout), mais arestas ordenadas. Função de roteamento **não** entra — não dá para hashear closure de forma estável.

O hash é carimbado em `Checkpoint`, `NodeInfo` e `Result`. Resume com hash diferente devolve `ErrVersionMismatch`.

### 4.8 Emissão de Mermaid

**Sanitização de id é obrigatória para todo nó, sem exceção.** Nome de nó é string livre do usuário, e o Mermaid tem palavras reservadas (`end`, `graph`, `subgraph`, `class`, `click`, `style`) além de quebrar com hífen, espaço e acento. Tratar `end` como caso especial resolveria um sintoma e deixaria a classe de bug viva.

A regra: o id emitido é `n_` mais o nome com tudo que não casa `[A-Za-z0-9_]` trocado por `_`; o nome real vai no label, entre aspas duplas. Colisão de id após a sanitização (`a-b` e `a_b`) é erro de `Compile`.

```
n_plan["plan"] --> n_search_kb["search kb"]
n_plan -.-> n_audit["audit"]
n_answer -->|branch| n_end["end"]
```

Convenções de aresta, e só estas três:

| Aresta | Sintaxe |
| --- | --- |
| Estática | `-->` |
| Condicional (`Branch`) | `-->\|branch\|` |
| Destacada (`Detach`) | `-.->` |

O label do nó é declarado uma vez, na primeira aparição; as demais referências usam só o id. `MermaidTrace` acrescenta `classDef` e `class` para os nós de `Result.Path`, sem mudar nenhuma aresta.

---

## 5. Plano de testes

O que precisa estar coberto para a lib ser confiável. Tudo table-driven, com subteste nomeado e `t.Parallel()`.

| Área | Casos obrigatórios |
| --- | --- |
| Compile | Cada uma das 9 validações; múltiplos problemas reportados juntos |
| Roteamento | Aresta estática, branch, alvo não declarado, rota para `End`, ordem de fronteira estável |
| Join | `JoinAll` com dois ramos, `JoinAny`, join com ciclo, losango desbalanceado executando o convergente uma vez, `Branch` que não dispara um ramo não travando o convergente |
| Falha | `FailureAbort` cancela irmãos; `FailureSkip` segue e popula `Result.Errors` |
| Retry | Sucesso na segunda tentativa; cancelamento durante o backoff |
| Budget | Estouro entre supersteps e no meio de um nó |
| Cancelamento | `ctx` cancelado chega dentro do nó; erro não vira `*NodeError` |
| Destacada | Sobrevive ao fim do run; respeita timeout próprio; escrita descartada; `Wait` drena |
| Checkpoint | Round-trip; resume continua na fronteira certa; `ErrVersionMismatch` |
| Mermaid | Golden file para topologia e para trace |
| Clone | Independência do original; `Add` substituindo nó |
| Concorrência | 1.000 `Run` simultâneos no mesmo `Runner` sob `-race` |

Ferramental obrigatório:

- `go test -race ./...` no CI, sempre.
- `goleak.VerifyTestMain(m)` — o pacote é dono de goroutines e não pode vazar nenhuma.
- `testing/synctest` para tudo que envolve timer: budget, timeout, backoff. Sleep como sincronização é proibido, e deadline real curta também: sob carga de CI ela vira teste intermitente. É por causa disso que a diretiva `go` do módulo é 1.25 — `synctest` é a única razão de não ficarmos em 1.24.
- `Example*` com `// Output:` para o quickstart, verificado pelo compilador. Documentação que quebra o build quando mente.
- Benchmark: `BenchmarkLinear` (5 nós sequenciais) e `BenchmarkFanOut` (1→4→1), com `b.ReportAllocs()`. O objetivo é vigiar o overhead do runner, não competir com ninguém.

Restrição temporal, válida até a v0.2: **nenhum teste da v0.1 pode depender de dois ramos convergindo no mesmo nó.** Sem join implementado, o que acontece nessa convergência é acidente de implementação, e a v0.2 muda o default para `JoinAll`. Teste escrito agora vira contrato errado, e contrato errado é mais caro de remover do que de nunca ter escrito.

---

## 6. Milestones

| Versão | Entrega | Pronto quando |
| --- | --- | --- |
| v0.1 | Builder sem `Detach`, `Compile` com as validações 1–5 e 7–9, runner sequencial, `Branch`, `WithTimeout`, `WithRetry`, `NodeError`, `CompileError`, `ErrMaxSteps`, `ErrUnknownTarget`, `Mermaid` | Um grafo linear com ciclo roda e desenha |
| v0.2 | Superstep paralelo, `Join`, `OnFailure`, `Detach` com a validação 6, `WithBudget`, `ErrBudget`, `WithSequential`, `Result.Errors` | Fan-out de 2 KBs com join e destacada de auditoria roda sob `-race` |
| v0.3 | `Hook`, canal de eventos, `MermaidTrace` | Trace OTel de ponta a ponta e CLI ao vivo funcionando |
| v0.4 | `Checkpointer`, `Resume`, hash de topologia | Mata o processo no meio, sobe e retoma |
| v0.5 | `WithConflictCheck`, `Merge` opcional, benchmarks, congelamento da API | Benchmarks publicados no README |

Antes de escrever o runner, escrever o `example_test.go` montando o grafo real de uso. Se a API for desconfortável de ler ali, o problema é a API, e este é o momento mais barato de descobrir isso.

Duas regras que atravessam todos os milestones:

- **Sentinela só existe quando o código consegue retorná-la.** Declarar `ErrBudget` na v0.1, onde não há budget, é documentação mentindo com aval do compilador. Cada uma entra no milestone que a produz.
- **Adiar construto é seguro; adiar forma de erro não é.** Método novo no builder é aditivo e não quebra ninguém, então `Detach` pode esperar. `NodeError` e `CompileError` não podem: todo teste asserta em cima do formato do erro, e mudar isso na v0.3 obrigaria a reescrever os testes de roteamento e de compilação inteiros. É por isso que eles estão na v0.1 mesmo sendo, no papel, assunto de observabilidade.
- **Campo de struct de saída segue a mesma regra da sentinela.** `Result` e `NodeError` são preenchidos pela lib e lidos pelo usuário, então acrescentar campo depois é aditivo e não quebra ninguém — desde que ninguém use literal posicional, o que a seção 2.2 já proíbe. Campo declarado e sempre zerado é o inverso: mente com aval do compilador. Cada campo entra no milestone que consegue preenchê-lo.

---

## 7. Orçamento de complexidade

Alvo: **800 a 1.200 linhas de código de produção**, com volume equivalente de teste.

Passar de 2.000 linhas significa que escopo entrou pela porta dos fundos. Nesse ponto, a pergunta certa não é "como organizo isso", é "o que eu removo".

Sinais de que a lib está se perdendo, para revisar a cada milestone:

- Um tipo da lib apareceu na assinatura de um nó de negócio.
- Foi preciso adicionar campo no estado do usuário para o runner funcionar (`state.NextNode`, `state.Retries`).
- Uma option precisou de outra option para fazer sentido.
- Um `if` de controle de fluxo do grafo vazou para dentro de um nó.