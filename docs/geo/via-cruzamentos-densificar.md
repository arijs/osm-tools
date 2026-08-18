# Cruzamentos e densificação de vias DNE

> Índice de **proximidade ao longo do traçado** e **grafo rudimentar** entre `log_nu`, a partir das ways OSM casadas no join (`osm_way_ids` + `OSM_LOGRADOURO_GEOM_*`).

## Problema

A busca por proximidade usa só o centróide do cluster. Em via longa, um ponto longe do centróide não “vê” a rua. Para roteamento rudimentar também falta saber **com quais `log_nu` cada via se liga** e **onde**.

## Pré-requisito

- `DNE_GEO_LOGRADOURO_{UF}.TXT` com **26 colunas** (`osm_way_ids`). Joins antigos (25 cols) não servem — re-join ou use pasta com join recente (ex. `G:\dne-geo-conectores-fuzzy`).
- `OSM_LOGRADOURO_GEOM_{UF}` da fatia correspondente ([extrair-geom-brasil.md](./extrair-geom-brasil.md)).

## Script

```bash
cd D:\dev\github\osm-tools
set NODE_OPTIONS=--max-old-space-size=8192
node scripts/dne-via-cruzamentos.js ^
  --dne-geo=G:\dne-geo-conectores-fuzzy ^
  --geom=G:\osm-geo-br-geom\sp ^
  --out=G:\dne-geo-via-sp ^
  --uf=SP

# recorte RMSP (piloto):
node scripts/dne-via-cruzamentos.js ^
  --dne-geo=G:\dne-geo-conectores-fuzzy ^
  --geom=G:\osm-geo-br-geom\sp ^
  --out=G:\dne-geo-via-rmsp ^
  --uf=SP ^
  --bbox=-47.20,-24.05,-46.30,-23.20
```

| Flag | Default | Nota |
|------|---------|------|
| `--dne-geo` | — | pasta do join |
| `--geom` | — | pasta com `OSM_LOGRADOURO_GEOM_{UF}` (flat ou shard) |
| `--out` | — | pasta dos artefatos |
| `--uf` | — | uma UF por vez |
| `--bbox` | — | `minLon,minLat,maxLon,maxLat` — só ways cujo envelope intersecta |
| `--cell` | `0.002` | ~220 m — grade espacial |
| `--max-seg-km` | `0.111` | teto geodésico ≈ 111 m |
| `--touch-km` | `0.0015` | T-junction / toque (~1,5 m) |
| `--only-ok` | ligado | só `geo_status=ok` |

Helpers em `via-geom.js` (interseção, projeção, densificar). Codec: `geo-polyline.js`.
Sentido (`oneway` 0–3) vem do GEOM novo (`osm-oneway.js`); GEOM legado (2 cols) lê como `0`.

## Algoritmo (por UF)

1. Coletar `osm_id` ↔ `log_nu` das linhas ok com col. 26.
2. Carregar só as polylines GEOM desses ids.
3. Segmentos + grade (~0.002°).
4. Conexões entre ways do set: **vértice compartilhado** (nó OSM em qualquer posição da polyline), endpoint sobre interior de segmento (T), segmento×segmento (cruzamento).
5. Âncoras = vértices OSM ∪ pontos de cruzamento/conexão; densificar trechos `> 0.111` km.
6. Agregar a `log_nu` (pontos e arestas do grafo). Self-loop mesmo `log_nu` omitido nas ligações (continuidade da rua já está nos pontos/`conexao` na way).

## Artefatos (`@`, UTF-8)

**`DNE_GEO_VIA_PONTO_{UF}.TXT`** — proximidade  
`log_nu@lat@lng@origem@osm_id`  
`origem` ∈ `vertice` | `cruzamento` | `conexao` | `amostra`

**`DNE_GEO_VIA_LIGACAO_{UF}.TXT`** — grafo  
`log_nu_a@log_nu_b@lat@lng@tipo@osm_id_a@osm_id_b`  
`tipo` ∈ `cruzamento` | `conexao`  
(`log_nu_a ≤ log_nu_b`)

**`DNE_GEO_VIA_RELATORIO_{UF}.json`** — contagens e tempo.

## Escala

UF grande (SP): centenas de milhares de ways usadas; preferir `--max-old-space-size=8192`. Retomável **por UF** (rodar de novo sobrescreve a saída da UF).

Pares de segmentos na grade **não** usam um `Set` global (mesmo `RangeError: Set maximum size exceeded` do extract BR) — stamp `Int32Array` + dedup de hits em `Map`→`Set` pequeno por par de ways.

## Fora de escopo (osm-tools)

- Dijkstra / roteamento em si — só o grafo de ligações.
- Cruzamento com ways OSM **não** casadas no DNE.

## Load no ddsoft

Migration + CLI já no monorepo ddsoft-online:

```powershell
cd D:\dev\ddsoft\ddsoft-online
php bin/console doctrine:migrations:migrate
php bin/console osm:dne:load-via `
  --dir=G:\dne-geo-conectores-fuzzy `
  --geom-dir=G:\osm-geo-br-geom\sp `
  --via-dir=G:\dne-geo-via-rmsp `
  --uf=SP --dataset=all
```

Detalhe das tabelas: [geometria-via-destaque.md §5](./geometria-via-destaque.md).

## Ligações

- [geometria-via-destaque.md](./geometria-via-destaque.md) — GEOM + `osm_way_ids` + load ddsoft
- [dne-geo-join.md](./dne-geo-join.md) — contrato do join
- [operacao-comandos.md](./operacao-comandos.md) — receitas CLI
- Testes: `test/via-cruzamentos.test.js`
