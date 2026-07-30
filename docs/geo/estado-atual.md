# Estado atual do pipeline (Sudeste BR)

**Última consolidação:** 2026-07-30 (extract de logradouro/bairro/addr com `kind` + `name_alt`).

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

Ganho medido na capital paulista com o extract novo: **81,1 % → 84,3 %** de linhas do DNE com
candidato, e **87,8 %** com a cascata determinística completa. `Praça` saiu de 35,9 % para 76,1 %.
Detalhe em [melhoria-extracao-coordenadas.md](./melhoria-extracao-coordenadas.md) §10.6.

## O que **não** está feito

| Item | Status |
|------|--------|
| **Match logradouro em volume** | CLI existe, mas a chave `UF\|nome` só resolve nome único no estado — `Avenida Paulista` fica ambígua. Ver [bairro-logradouro.md](./bairro-logradouro.md) §Match |
| **`dne-geo-join.js`** (footprint municipal + cascata) | Não existe — é o próximo passo |
| **Guarda kind-aware no `DneOsmGeoEnricher`** | **Bloqueia** rodar o enricher contra `streets2`: 2 584 colisões medidas (`Rua Dois` × `Praça Dois`) |
| `name_alt` no enricher PHP | Não lido (+1,1 pp parados) |
| Consumidor de `OSM_ADDR_POINT_*` | Nenhum; 205 660 pontos com CEP sem uso |
| `geo_origem` / `geo_status` / precisão | Sem migration — virou pré-requisito, não mais “recomendado” |
| Materialização em massa bairro/logradouro em `locais` | Lazy no accept; não é pré-requisito para geo no índice |
| Expor lat/lng na `BuscaEnderecoService` | Parcial / futuro |

## Dimensão do índice DNE (contexto para a próxima fase)

Valores observados no MySQL do ddsoft (ordem de grandeza):

| Tabela | Total BR | SE (SP/RJ/MG/ES) |
|--------|---------:|-----------------:|
| `dne_idx_bairro` | ~63 k | ~39 k |
| `dne_idx_logradouro` | ~1,2 M | ~606 k |
| `dne_idx_localidade` | ~11 k | — |

Ver [bairro-logradouro.md](./bairro-logradouro.md).

## Lições já pagas (não reaprender)

1. **JSON de inventário ~10 MB** ≠ dados de geocoder — era `coordLayout.blocks`.  
2. **99 % logradouros em `XX`**: ways depois de 141 M nodes; LRU de 500 k não serve → **two-pass**.  
3. **Município sem geo**: relation tem IBGE, node `place=city` tem ponto sem IBGE → cruzar nome+UF **ou** resolver `admin_centre`.  
4. **IBGE 9 dígitos** = distrito; truncar a 7 e casar como município **erra o ponto** (ex. subúrbio → código do município).  
5. **`updated=0` em estado** com coords no TXT costuma ser `already` (onlyEmpty), não falha de match.  
6. **Rua famosa que “não existe no OSM” quase sempre existe** — é ambiguidade, não ausência. `Avenida Paulista`: 207 ways em SP, 19 `loc_nu` no DNE. Medir dentro de um município antes de concluir que falta dado.  
7. **Praça não é `highway`** — é `leisure=park` (17 032 em SP) ou `place=square` (283). Filtrar só por `highway` perdia 2/3 das praças do DNE.  
8. **`LOG_VAR_LOG` do DNE não serve** para variantes: 3 nomes casam em SP inteiro. O lado OSM (`alt_name` etc.) rende ~10 % das linhas.  
9. **`addr:street` como fonte de nome rende quase nada** (+0,1 pp). O valor dele é `addr:postcode` e nomear way sem `name`, e isso não foi testado.
