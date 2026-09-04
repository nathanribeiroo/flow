# resume

Losango desbalanceado: `fetch` abre `clean` e `enrich` em paralelo, o ramo de
`enrich` ainda passa por `score`, e `merge` espera os dois. O processo morre no
meio do fan-out e volta do checkpoint.

O que dá para ver na saída:

- o checkpoint parado no passo 1 com `[clean enrich]` em `Pending`, que é a
  lista de candidatos e não a fronteira: quem retoma aplica a regra de join
  sobre ela e chega na mesma fronteira a que o runner chegaria;
- `clean` e `enrich` rodando duas vezes, porque o passo interrompido
  reexecuta — a garantia é *at-least-once* e os nós precisam ser idempotentes;
- `merge`, o convergente, rodando uma vez só;
- o que `clean` escreveu no passo que não fechou sumindo na retomada, porque o
  estado volta do checkpoint: estado de execução cancelada não vale.

A queda é uma `context.CancelFunc` chamada de dentro de um nó, e o `Resume`
roda com um contexto novo, como faria um processo novo.

Sem rede e sem credencial: o store é o `MemoryStore` e a latência é
`time.Sleep`.

```bash
go run ./examples/resume
```
