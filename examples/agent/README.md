# agent

O grafo do README da lib, rodando de verdade: `plan` dispara `search_kb` e
`search_web` em paralelo, `answer` espera as duas (`JoinAll`), `audit` roda
numa aresta destacada que não segura o fluxo, e um `Branch` volta para `plan`
até a resposta ter fonte suficiente. Tudo com o canal de eventos ligado num
consumidor que imprime o turno linha a linha.

O que dá para ver na saída:

- os dois `search_*` abrindo no mesmo superstep, que é o fan-out acontecendo;
- `search_web` falhando na primeira rodada e o turno seguindo mesmo assim,
  porque ela é `FailureSkip`; o erro reaparece em `Result.Errors` no fim;
- `audit` carregando o número do passo que a disparou e imprimindo no meio do
  passo seguinte, porque destacada não entra na fronteira;
- a resposta final intacta, provando que o que a destacada escreveu na cópia
  do estado foi descartado;
- o ciclo: seis passos para quatro nós, duas rodadas de revisão.

O canal de eventos é de quem o cria. O programa fecha o canal só depois de
`Run` voltar **e** de `Wait` drenar as destacadas.

Sem rede e sem credencial: a latência é `time.Sleep` e a falha é um contador.

```bash
go run ./examples/agent
```
