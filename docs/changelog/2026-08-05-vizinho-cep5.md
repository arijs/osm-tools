# 2026-08-05 — recuperação por vizinhança CEP-5 (`vizinho_cep5`)

## Prompt original

> Comite tudo numa nova branch e faça o push; depois, siga com a próxima fase
> (vizinhança CEP-5 / proximidade para fechar lacuna `fora_do_footprint`, com auditoria).

## O que foi implementado

### Fase 5e no `dne-geo-join.js`

Após envelope e **antes** da exclusão multi-município:

1. Índice de vias já `ok` por `loc_nu|CEP-5` e por `bai_nu`
2. Para cada `ambiguo` / `fora_do_footprint` com candidatos de nome:
   - exige ≥ `--vizinho-cep5-min` (default 3) âncoras no CEP-5; senão tenta o bairro
   - filtra candidatos com dist(vizinho mais próximo) ≤ `--vizinho-cep5-tol-km` (default 1)
   - aceita se sobrar **exatamente 1** e extensão ≤ `max-extent-km`
3. `geo_regra=vizinho_cep5`
4. Relatório: `vizinho_cep5_recuperados`, `vizinho_cep5_exemplos` (fonte, distâncias, top-3 vizinhas, `nome_regra`)

Flags: `--sem-vizinho-cep5`, `--vizinho-cep5-tol-km=N`, `--vizinho-cep5-min=N`.

Helpers exportados: `digitsCep5`, `nearestDistKm`.

## Comparação antes / depois

| | Antes | Depois |
|--|-------|--------|
| Nome ok, fora da pegada, 1 cluster perto de vias do mesmo CEP-5 | `ambiguo` / vazio | `ok` + `vizinho_cep5` + evidência |
| 2+ clusters perto do bolsão CEP-5 | — | continua `ambiguo` (não chuta) |
| Âncora local (centroide+raio) | removida (raio explodia) | não volta; usa vizinho-mais-próximo |

## Como foi testado

- `npm run test:join` → **25 testes, 25 pass**
  - fixture CEP-5: recupera alvo sul, escolhe cluster perto (não o de 150 km)
  - `--sem-vizinho-cep5` deixa `ambiguo`
  - `digitsCep5` / `nearestDistKm` / parseCli das flags novas

## Testes criados/alterados

- `test/dne-geo-join.test.js` — `setupVizinhoCep5Dirs`, 2 testes de recuperação + helpers + CLI
