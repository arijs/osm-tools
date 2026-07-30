# Estado atual do pipeline (Sudeste BR)

**Última consolidação:** 2026-07-30 (join DNE↔OSM + envelope + exclusão multi-município + load prejoined no ddsoft).

## Fonte OSM

| Item | Valor |
|------|--------|
| Arquivo | `G:\sudeste-260725.osm.pbf` (~813 MB, osmium) |
| Nodes | ~141 M |
| Ways | ~9,3 M |
| Relations | ~157 k |
| Bbox header | lon ~−53…−28, lat ~−25…−14 |
| Layout coords | ~93 % saltos pequenos (bem sequencial no arquivo) |

Inventário: `node index-pbf.js` → `…-pbf-stats.json` / `…-pbf-results.json`.  
Por default **não** grava `coordLayout.blocks` (evita JSON de ~10 MB). Flags: `--layout-full-blocks`, `--layout-max-blocks=N`.

## Fase concluída: estado + município em `locais`

### Extract (pasta típica `G:\osm-geo-se`)

Rodado com `--datasets=estado,municipio` (sem bairro/logradouro nessa pasta).

| Arquivo | Ordem de grandeza |
|---------|-------------------|
| `OSM_ESTADO.TXT` | ~15 linhas (4 UFs SE com ponto + relations de borda) |
| `OSM_MUNICIPIO.TXT` | ~2,9 k linhas; ~1 768 códigos IBGE de 7 dígitos |
| `extract-checkpoint.json` | fim do arquivo (`eof: true`) |

Two-pass de **`admin_centre`** em relations de município: sem isso, a maioria das relations admin tinha IBGE e **zero** lat/lng.

### Enrich ddsoft: `osm:locais:enrich-geo`

Resultado de referência (apply, só `lat IS NULL`):

```text
Estado:     candidates=9 updated=0 already=4 no_geom=5 not_in_db=0
Município:  lines=2904 ibge7=1768 name_city=1875 geom_from_name=0
            skipped_district_ibge=0
            updated=1038 already=635 matched_ibge=1038 matched_name=0
            no_geom=95 not_in_db=0
```

| Leitura | |
|---------|--|
| **Estados SE** | SP/RJ/MG/ES com geo (`already=4` se reexecutar) — confirmado em teste manual |
| **Municípios** | +1038 updates + 635 já preenchidos ≈ **95 %** dos IBGE do extract com ponto |
| **`no_geom=95`** | residual OSM (admin sem centro útil) |
| **`not_in_db=0`** | todo IBGE 7 dígitos do extract existe em `locais` |

Validação do produto: **todos os estados e municípios do Sudeste no cadastro tiveram coordenadas com sucesso.**

## Fase concluída: extract de logradouro, bairro e addr

Pasta `G:\osm-geo-se-streets2` (2026-07-30, `eof: true`). `G:\osm-geo-se-streets` fica como
**linha de base** para comparar ganhos — não apagar.

| Dataset | Volume |
|---------|-------:|
| Logradouro | **1 265 470** (SP 793 906 · RJ 204 235 · MG 190 674 · ES 76 653) |
| — dos quais área (`square`/`park`) | 24 980 |
| — com `name_alt` | ~139 k |
| Bairro | 30 906 (28 206 com geom; 2 696 em `XX`) |
| Addr point (`addr:street`) | 205 660 |

## Fase concluída: `dne-geo-join.js` (OSM ↔ DNE)

Implementado e rodado no Sudeste. Spec: [dne-geo-join.md](./dne-geo-join.md).

| UF | `ok` | % | `ambiguo` | `sem_nome_osm` |
|----|-----:|--:|----------:|---------------:|
| SP | 263 478 | **77,1 %** | 22 281 | 56 054 |
| ES | 23 972 | **74,9 %** | 735 | 7 286 |
| RJ | 74 588 | **71,9 %** | 8 001 | 21 134 |
| MG | 70 582 | **54,7 %** | 13 812 | 44 535 |

Capital paulista: **88,6 %**. Saída: `DNE_GEO_LOGRADOURO_{UF}.TXT`, `DNE_GEO_BAIRRO_{UF}.TXT`,
`DNE_GEO_RELATORIO_{UF}.json`.

Pós-2026-07-30 no join:

- **Envelope** (fase 5c): recupera `fora_do_footprint` com 1 candidato a ≤1 km da mancha (buraco na grade).
- **Exclusão multi-município** (fase 5d): cluster usado por 2+ `loc_nu` → dono único; perdedores `conflito_municipio`.

## Fase pronta no código: load no índice DNE (ddsoft)

`osm:dne:enrich-geo` prefere `DNE_GEO_*` por `log_nu` / `bai_nu` quando o arquivo existe na `--dir`.
Match por nome em `OSM_*` fica como legado (`--legacy-match`).

```bash
php bin/console osm:dne:enrich-geo --dir=G:\dne-geo-local --dataset=logradouro --dry-run
php bin/console osm:dne:enrich-geo --dir=G:\dne-geo-local --dataset=logradouro
php bin/console osm:dne:enrich-geo --dir=G:\dne-geo-local --dataset=bairro
# opcional: --uf=SP para uma UF só
```

## O que **não** está feito

| Item | Status |
|------|--------|
| **Apply em volume** do prejoined no MySQL de dev/prod | Código pronto; rodar dry-run → apply por UF |
| `geo_origem` / `geo_status` / precisão no schema | Sem migration — útil para reprocessar regra fraca |
| Consumidor de `OSM_ADDR_POINT_*` (postcode / nomear way) | Nenhum; 205 660 pontos com CEP sem uso |
| Materialização em massa bairro/logradouro em `locais` | Lazy no accept; não é pré-requisito para geo no índice |
| Expor lat/lng na `BuscaEnderecoService` | Parcial / futuro |
| Match legado por nome no PHP | **Não usar** contra `streets2` (2 584 colisões kind) |

## Dimensão do índice DNE (contexto)

| Tabela | Total BR | SE (SP/RJ/MG/ES) |
|--------|---------:|-----------------:|
| `dne_idx_bairro` | ~63 k | ~39 k |
| `dne_idx_logradouro` | ~1,2 M | ~606 k |
| `dne_idx_localidade` | ~11 k | — |

## Lições já pagas (não reaprender)

1. **JSON de inventário ~10 MB** ≠ dados de geocoder — era `coordLayout.blocks`.  
2. **99 % logradouros em `XX`**: ways depois de 141 M nodes; LRU de 500 k não serve → **two-pass**.  
3. **Município sem geo**: relation tem IBGE, node `place=city` tem ponto sem IBGE → cruzar nome+UF **ou** resolver `admin_centre`.  
4. **IBGE 9 dígitos** = distrito; truncar a 7 e casar como município **erra o ponto**.  
5. **`updated=0` em estado** com coords no TXT costuma ser `already` (onlyEmpty), não falha de match.  
6. **Rua famosa que “não existe no OSM” quase sempre existe** — é ambiguidade, não ausência.  
7. **Praça não é `highway`** — é `leisure=park` ou `place=square`.  
8. **`LOG_VAR_LOG` do DNE não serve** para variantes. O lado OSM (`alt_name`) rende ~10 % das linhas.  
9. **`addr:street` como fonte de nome rende quase nada** (+0,1 pp).  
10. **Match por nome no PHP não escala** — join no osm-tools; PHP só carrega por chave.  
11. **Dilatar footprint uniforme** mistura cidades vizinhas 1:1 com o ganho — preferir envelope seletivo.  
12. **Cluster multi-município em `ok`** é pior que vazio — exclusão pós-casamento.
