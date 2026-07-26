# Estado atual do pipeline (Sudeste BR)

**Última consolidação:** 2026-07-26 (após re-extract município + enrich corrigido).

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

## O que **não** está feito

| Item | Status |
|------|--------|
| Extract bairro / logradouro no Sudeste | Pendente (pasta separada recomendada) |
| Colunas `lat`/`lng` em `dne_idx_bairro` / `dne_idx_logradouro` | **Não existem** (só nome, CEP, chaves DNE) |
| CLI match bairro/logradouro | Não existe (`osm:locais:enrich-geo` recusa esses datasets) |
| Materialização em massa bairro/logradouro em `locais` | Lazy no accept; não é pré-requisito para geo no índice |
| `geo_origem` / precisão em `locais` | Recomendado no doc de estrutura §11.2; ainda sem migration |
| Expor lat/lng na `BuscaEnderecoService` | Parcial / futuro |
| Logradouro two-pass no extract real | Código pronto; **não** rodado no dump SE completo nesta pasta |

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
