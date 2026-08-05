# 2026-08-05 — match por strip de título (`Doutor` / `Prof` / …)

## Prompt original

> No OSM existe uma rua chamada 'Rua Doutor Olímpio Carr Ribeiro', e no DNE o logradouro
> dela é 'Olímpio Carr Ribeiro', com tipo 'Rua' e CEP '04775120'. O nosso dne-geo-join não
> deu match nessa via por causa do 'Doutor' apenas?
>
> (seguido de pedido de estratégia robusta com CEP-5 / proximidade e strip de títulos;
> implementação da Fase 0+1.)

## O que foi implementado

### 1. Strip de títulos (`name-keys.js`)

- Lista fechada `TITULOS` (doutor/dr, professor/prof, engenheiro/eng, patentes, cargos, …).
- `stripTitulos(norm)` → `{ bare, removed }` — só no **início** do núcleo; nunca esvazia.
- `coreBare(norm)` = strip de título após `coreName`.
- Nobreza (barão, visconde…) e santo/são **fora** da lista (identidade do nome / topônimo).

### 2. Cascata no `dne-geo-join.js`

Novos degraus **depois** de `fonetico`:

| `geo_regra` | Chave |
|-------------|--------|
| `titulo` | núcleo sem tipo e sem título |
| `titulo_fonetico` | fonética do núcleo bare |

Índices derivados: `byTitle`, `byTitlePhon` (além de `byCore` / `byPhon`).

O footprint, envelope e exclusão multi-município **não mudam** — o título só alarga a geração de
candidatos de nome; a guarda espacial continua igual.

### 3. Auditoria no relatório JSON

- `titulo_exemplos` — até 30 matches novos com `dne_nucleo`, `osm_nucleo`, `tokens_removidos_*`, CEP
- `sem_nome_osm_exemplos` — até 30 residual sem match (`cep5`, núcleo bare) para a próxima fase
  (vizinhança CEP-5)

## Comparação antes / depois

| | Antes | Depois |
|--|-------|--------|
| DNE `Olímpio Carr Ribeiro` + OSM `Rua Doutor Olímpio…` | `sem_nome_osm` | `ok` / `geo_regra=titulo` |
| Cascata | … → nucleo → fonetico | … → fonetico → **titulo** → **titulo_fonetico** |
| Relatório | só `ambiguo_exemplos` | + `titulo_exemplos` + `sem_nome_osm_exemplos` |

## Como foi testado

- `npm run test:join` → **22 testes, 22 pass, 0 fail**
  - `stripTitulos` / `coreBare` (casos Doutor, Dr, Prof, nobreza preservada)
  - fixture join: logradouro `116` casa com `geo_regra=titulo`; relatório com exemplos

## Testes criados/alterados

- `test/name-keys.test.js` — strip e coreBare
- `test/dne-geo-join.test.js` — linha DNE 116 + way OSM com Doutor; contagens 16 linhas

## Próximo (não neste PR)

Fase 2 da estratégia: guarda por vizinhança **CEP-5** (e bairro) para match fraco / residual
`fora_do_footprint`, com score por número de vias vizinhas já resolvidas.
