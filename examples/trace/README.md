# trace

Dois `Hook` aninhados, sem OpenTelemetry, para mostrar a forma que um hook de
tracing tem: `tracer` abre e fecha um span por tentativa de nó e `meter` roda
por dentro dele.

O que dá para ver na saída:

- `NodeStart` na ordem de registro e `NodeEnd` na inversa, aninhando como
  `defer`: `meter` foi registrado depois, então abre depois e fecha antes;
- o contexto encadeado — `meter` enxerga a profundidade que `tracer` escreveu,
  e o nó recebe o contexto do fim da cadeia, que é o que faz o span do nó
  envolver a chamada lenta de verdade;
- cada hook recebendo no `NodeEnd` o contexto que o próprio `NodeStart`
  devolveu, sem o que o primeiro hook fecharia o span do último;
- três spans irmãos para `check_facts`, um por tentativa, em vez de um span só
  escondendo as duas falhas;
- o `MermaidTrace` no fim, com os nós percorridos marcados.

`WithSequential()` mantém a fronteira em ordem declarada e a saída legível;
sem ele os dois checks escreveriam intercalados.

Sem rede e sem credencial: a latência é `time.Sleep` e a falha é um contador.

```bash
go run ./examples/trace
```
