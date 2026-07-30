# Objetivo e contexto do projeto

## Meta de negócio (por que este repositório existe)

O objetivo de longo prazo é **geocoding** e **geocoding reverso** em cima de dados OpenStreetMap:

| Direção | Pergunta | Entrada típica | Saída desejada |
|---------|----------|----------------|----------------|
| **Geocoding** | “Onde fica este endereço?” | texto: rua, número, cidade, país… | coordenada (`lat`, `lon`) |
| **Geocoding reverso** | “O que há neste ponto?” | `lat`, `lon` | endereço / nome / POI |

A hipótese de trabalho é: **algum dump OSM** (ex.: recorte regional `.osm.pbf`) contém os elementos e tags necessários para alimentar o cadastro de endereços do produto (ddsoft).

**Escopo da fase atual (2026-07):** coordenadas de **município / bairro / logradouro** no cadastro DNE (`dne_idx_*`) e em `locais` (estado/município). Geometria por **via inteira**, não por número de porta. Geocoding de número e reverso com POI ficam depois.

## Pipeline geo (documentação principal)

A documentação operacional e de desenho do caminho **OSM → TXT → banco** está em:

### → [**docs/geo/README.md**](./geo/README.md)

| Arquivo | Conteúdo |
|---------|----------|
| [geo/estado-atual.md](./geo/estado-atual.md) | Sudeste: estados/municípios/join feitos; números reais |
| [geo/extract-e-artefatos.md](./geo/extract-e-artefatos.md) | Extract, formatos `@`, two-pass, **resume/wipe** |
| [geo/match-estado-municipio.md](./geo/match-estado-municipio.md) | CLI `osm:locais:enrich-geo`, IBGE, lições |
| [geo/bairro-logradouro.md](./geo/bairro-logradouro.md) | Join feito; load PHP por `log_nu` |
| [geo/dne-geo-join.md](./geo/dne-geo-join.md) | Especificação do join OSM↔DNE |
| [geo/operacao-comandos.md](./geo/operacao-comandos.md) | Receitas de CLI |

## Fases sugeridas (roadmap)

| Fase | Estado | Entrega |
|------|--------|---------|
| **0 — Inventário XML** | `index0.js` (`.osm.bz2`) | árvore de tags/attrs, geoSignals, coordLayout |
| **0b — Inventário PBF** | `index-pbf.js` (`.osm.pbf`) | mesmos sinais/layout; extract regional (ex. Sudeste) |
| **1 — Detectar sinais de endereço** | parcial | `geo`/`GEO` + `geocodeSignals.hints` |
| **2a — Extrair geo → TXT `@`** | **feito** (SE) | `extract-geocode-pbf.js` |
| **2b — Match `locais` estado/município** | **feito** no Sudeste | ddsoft `osm:locais:enrich-geo` |
| **2c — Join OSM↔DNE logradouro/bairro** | **feito** no Sudeste | `dne-geo-join.js` → `DNE_GEO_*` |
| **2d — Load no índice DNE** | **código pronto** (ddsoft) | `osm:dne:enrich-geo` lê `DNE_GEO_*` por `log_nu` |
| **3 — Expor na busca** | futura | coords no candidato de endereço |

**Produto (ddsoft):** estado/município em `locais`; bairro/logradouro no índice DNE até materialização lazy.

**Nota sobre stats ~10 MB:** inventário com milhares de `coordLayout.blocks` — **não** é payload de geocoder. Default: omitir blocks no JSON (`--layout-full-blocks` / `--layout-max-blocks=N`).

## O que o script faz *na prática* (fase 0)

`index0.js` / `index-pbf.js` **inventariam** estrutura e sinais; **não** resolvem endereço.

- Contam elementos, tags, amostras  
- `geocodeSignals` / `coordLayout`  
- Resume e soft-stop  

Detalhe: [o-que-o-script-grava.md](./o-que-o-script-grava.md), [o-que-o-script-pbf-grava.md](./o-que-o-script-pbf-grava.md).

## Documentação relacionada

- [**geo/**](./geo/README.md) — pipeline completo OSM → coordenadas  
- [plans/pbf-inventory.md](./plans/pbf-inventory.md) — plano inventário PBF  
- [plans/osm-para-locais-geo.md](./plans/osm-para-locais-geo.md) — plano histórico extract → locais  
- [README principal](../README.md)  
