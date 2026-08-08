# 2026-08-07 — `vizinho_cep5` após `conflito_municipio` (fase 5f)

## Prompt

> Creio que os bairros/cep-5 vizinhos seriam mais do que suficiente pra resolver
> este conflito e outros parecidos. Crie uma nova branch e implemente uma solução,
> testando que este caso fica resolvido.

Caso motivador: `log_nu` 606476 *Rua Neuchatel*, São Paulo / Capela do Socorro —
`ambiguo` no re-join 2026-08 enquanto a via existe no OSM (cluster oeste órfão;
cluster leste ficou com São Bernardo `564372`).

## O que foi implementado

### Fase 5f no `dne-geo-join.js`

Após a exclusão multi-município (5d):

1. Reindexa âncoras ok (CEP-5 / bairro) e o dono atual de cada cluster
2. Para cada `ambiguo` / `conflito_municipio` com candidatos de nome:
   - ignora cluster cujo dono é **outro** `loc_nu`
   - mesma regra da 5e (≥ min âncoras, ≤ tol km ao vizinho mais próximo, candidato único)
3. `geo_regra=vizinho_cep5`
4. Relatório: `vizinho_cep5_pos_conflito_recuperados`, `vizinho_cep5_pos_conflito_exemplos`

Helpers extraídos (reuso 5e/5f): `indexVizinhoAnchors`, `anchorsForRow`,
`pickVizinhoUnico`, `topVizinhos`.

## Medição SP (`G:\osm-geo-br-sudeste`)

| | Antes (re-join ago) | Depois (5f) |
|--|--------------------:|------------:|
| `ok` | 256 249 (75,0 %) | **260 338 (76,2 %)** |
| `vizinho_cep5` (total) | 317 | **4 406** |
| pós-conflito | — | **4 089** |
| `606476` Neuchatel SP | `ambiguo` vazio | `ok` oeste (−23,6886 / −46,7026), ways `42743280+939074274+1263062634` |
| `564372` Neuchatel SBC | `ok` leste | `ok` leste (inalterado) |

## Testes

- `npm run test:join` — fixture Neuchatel (recupera oeste via CEP-5; `--sem-vizinho-cep5` deixa `ambiguo`)
- Join real SP conferido linha a linha nos dois `log_nu`
