# Cache de CEP externo (AwesomeAPI e afins)

Artefato intermediário no **mesmo espírito DNE**: texto UTF-8, delimitador **`@`**, sem header,
regenerável/appendable. A chave é o **CEP** (8 dígitos) — unidade de consulta da API e de
deduplicação. Vários `log_nu` do DNE compartilham o mesmo CEP; o cache evita reconsultar.

## Por que não JSONL por logradouro

| Abordagem anterior (`results.jsonl`) | Cache `CEP_EXTERNO.TXT` |
|--------------------------------------|-------------------------|
| 1 linha por logradouro amostrado | 1 linha por CEP consultado |
| Reconsulta se o mesmo CEP reaparece | Lookup O(1) por CEP |
| Mistura amostra + resposta | Só o resultado da API (join usa depois) |
| Fora do padrão do pipeline | Igual `OSM_*` / `DNE_GEO_*` |

O vínculo com o logradouro fica no **join** (`DNE_GEO_LOGRADOURO` tem `cep` na coluna 8):  
`log_nu` sem geo → CEP → `CEP_EXTERNO` → validar footprint → `geo_regra=cep_externo`.

## Arquivo

```text
G:\dne-geo-local\CEP_EXTERNO.TXT     # canônico (ou --cache=…)
# ou pasta de trabalho:
_ignore/cep-externo/CEP_EXTERNO.TXT
```

Um único arquivo **nacional** (CEP já é único no Brasil). Não precisa `_{UF}`.

## Contrato — 13 colunas

```
 1 cep              8 dígitos, zero à esquerda (chave)
 2 http_status      200 · 404 · 400 · 0 (rede/parse) · 429 …
 3 status           ok | not_found | invalid | error | empty_coords
 4 lat              vazio se não houver ponto útil
 5 lng
 6 api_city
 7 api_state        UF
 8 api_district     bairro na API
 9 api_ibge         city_ibge (7 dígitos quando houver)
10 api_address      address completo da API
11 api_address_type address_type (Rua, Avenida, …)
12 consultado_em    ISO-8601 UTC (ex. 2026-07-30T22:38:00Z)
13 fonte            awesomeapi (reserva para outras fontes)
```

### `status`

| Valor | Significado | Reconsultar? |
|-------|-------------|--------------|
| `ok` | HTTP 200 + lat/lng numéricos | **Não** (salvo `--force`) |
| `empty_coords` | 200 mas sem lat/lng | Não (API não tem geom) |
| `not_found` | HTTP 404 | Não |
| `invalid` | HTTP 400 | Não |
| `error` | rede, 429, corpo ilegível | **Sim** em nova rodada (opcional; default **não** para não martelar) |

Política default do script: **nunca** reconsultar CEP que já tem linha no arquivo.  
`--force` refaz todos; `--retry-errors` só reconsulta `status=error`.

### Exemplos

```
01001000@200@ok@-23.5502784@-46.6342179@São Paulo@SP@Sé@3550308@Praça da Sé@Praça@2026-07-30T22:38:01Z@awesomeapi
20010070@200@ok@-14.235004@-51.92528@Rio de Janeiro@RJ@Centro@3304557@…@…@2026-07-30T22:38:10Z@awesomeapi
00000000@404@not_found@@@@@@2026-07-30T22:38:11Z@awesomeapi
```

(O segundo tem cidade certa e ponto no **centro do Brasil** — o join deve rejeitar pelo footprint.)

## CLI

```bash
# Converte a amostra JSONL antiga → CEP_EXTERNO.TXT (uma vez)
node scripts/cep-externo-from-jsonl.mjs \
  --in=_ignore/awesomeapi-sample/results.jsonl \
  --out=G:\dne-geo-local\CEP_EXTERNO.TXT

# Consulta só CEPs ainda não cacheados
node scripts/sample-awesomeapi-cep.mjs \
  --dir=G:\dne-geo-local \
  --cache=G:\dne-geo-local\CEP_EXTERNO.TXT \
  --n=1000
```

O script de amostra:

1. Carrega o cache existente (`cep` → linha).
2. Monta a lista de candidatos sem geo no DNE.
3. **Remove CEPs já presentes no cache.**
4. Consulta só o restante; **append** (ou rewrite estável ordenado por CEP).

## Relatórios de qualidade (por bucket)

Cada lote de consultas gera (ou reprocessa) markdown + JSON em:

- `G:\dne-geo-local\qualidade\bucket-NNN-….md` (canônico ao lado do cache)
- espelho: `docs/geo/cep-externo-qualidade/`

Conteúdo: % dentro do bbox municipal (vias `ok` do join), colapso de CEPs no mesmo ponto,
hits no centro do Brasil (−14,235 / −51,925), percentis de distância ao centroide.

```bash
# reprocessar todos os buckets já no cache (gap de 90 s em consultado_em)
node scripts/cep-externo-quality.mjs

# só o último bucket
node scripts/cep-externo-quality.mjs --last
```

O `sample-awesomeapi-cep.mjs` chama a qualidade automaticamente após cada lote (`--no-quality` desliga).

`CEP_EXTERNO_RELATORIO.json` ao lado do cache: contagens da última execução (hit de cache, ok/404…).

## Uso futuro no `dne-geo-join`

Fase opcional após cascata OSM:

1. Linha `sem_nome_osm` ou `ambiguo` com CEP.
2. Lookup em `CEP_EXTERNO` por CEP.
3. Se `status=ok`: validar lat/lng no footprint do `loc_nu` (e opcionalmente IBGE/UF).
4. Aceitar com `geo_regra=cep_externo`, **bbox vazia** (é ponto de CEP, não traçado).
5. Rejeitado → continua vazio.

Sem consulta HTTP dentro do join em volume — só leitura do TXT.
