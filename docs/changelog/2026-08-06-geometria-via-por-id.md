# 2026-08-06 — traçado da via por id exato (`--way-geom` + `osm_way_ids`)

## Prompt original

> Veja este repo […]. Aqui já temos uma vasta infraestrutura que pega os nós do OSM e tenta
> casar eles com a base do DNE. Veja o que podemos aproveitar daí pra pegar as vias pelo ID
> exato no OSM e fazer o destaque no mapa, sem depender do nome no runtime.
>
> Pode escrever a spec e já começar no osm-tools

Contexto: o mapa do ddsoft destaca o **ponto** do logradouro escolhido; queria-se destacar a
**via**. Filtrar o basemap por nome é o beco que este pipeline já abandonou no PHP.

## A descoberta que decidiu o desenho

Os tiles do Protomaps **não expõem id do OSM** (a layer `roads` tem `kind`, `kind_detail`,
`ref`, `network`, `oneway`, `service`, `is_link`, `is_tunnel`, `is_bridge` e os campos de
nome — nada de identificador de origem). Guardar o `osm_id` para apontar para o basemap não
funcionaria.

Então o id passa a ser a chave da **nossa** geometria, servida e desenhada numa camada
própria — o que é melhor: exato, sem nome em runtime, funciona fora do recorte do PMTiles e
com largura sob nosso controle em qualquer zoom.

## O que foi implementado

O pipeline já sabia tudo; faltava não jogar fora.

### `geo-polyline.js` (novo)

Codec da geometria dentro do TXT `@`. Pontos separados por `;`, cada um `lat,lng` em
unidades de **1e-6 grau**; o primeiro absoluto, os demais em **delta**.

- **Não é o polyline do Google**: o alfabeto dele inclui `@` (ASCII 64), que o
  `txt-at-writer` troca por espaço **em silêncio** — a geometria sairia corrompida sem erro.
- **1e-6 e não 1e-5**: 1,1 m dá 4–5 px no zoom 18, escadinha visível em curva.
- Duplicata **consecutiva** é descartada; ponto repetido no fim **fica** (anel de praça).
- `decodePolyline` **lança** em entrada malformada: meia via desenhada é pior que nenhuma.

### `extract-geocode-pbf.js`

`geomFromNodeIds()` já percorria a coordenada de cada nó para o centroide e a bbox, e
descartava tudo. Agora coleta os pontos quando pedido (opt-in — a função roda para toda way
do PBF) e emite `OSM_LOGRADOURO_GEOM_{UF}.TXT` (`osm_id@polyline@oneway`: `0` ausente · `1` frente · `2` reverso · `3` duplo explícito).

- Flag `--way-geom` / `--datasets=…,geom`; pedir `geom` liga `logradouro` junto.
- Arquivo irmão, não coluna a mais: o join lê o logradouro inteiro e não usa geometria.
- Só way com 2+ pontos distintos entra — toda linha desenha. O resto vira
  `logradouroGeomVazio`.
- Fatia junto com o logradouro (`--shard-lines`), com MANIFEST próprio, sem configuração.
- Resumo: `Geom: N vias, M pontos (sem traçado: K)`. README-colunas documenta o contrato.
- **Nenhuma releitura do PBF**: a pass 2 já tinha resolvido os nós.

### `dne-geo-join.js`

A feature montada para o cluster guardava `{lat, lng, bbox, n, kind}` e descartava o
`osm_id` lido em `p[0]`. Agora carrega o id, e a coluna **26 `osm_way_ids`** traz as ways do
cluster vencedor, unidas por `+`, em ordem numérica, sem repetição.

- Vazia quando `geo_status != ok`.
- Ponto de `addr:street` não entra: é nó de numeração, não tem traçado, e um id que não
  resolve do outro lado é pior que coluna vazia.
- Determinístico — mesmo insumo, mesmo byte (há teste).

## Testes

`test/geo-polyline.test.js` (8, novo): ida e volta, delta, alfabeto seguro contra o
sanitizador, anel fechado, degenerado, malformado, erro de arredondamento.

`test/extract-geocode.test.js` (+3): geometria contra o fixture PBF real (ids batem com o
logradouro, centroide bate com a linha principal, nó não vira linha, anel de praça fecha),
opt-in de verdade sem a flag, e o fatiamento com MANIFEST próprio.

`test/dne-geo-join.test.js` (+2, e o contrato foi de 25 → 26 colunas): ids do cluster
vencedor batendo com `osm_ways`, ordem, unicidade, coluna vazia sem match, determinismo byte
a byte, e ponto de `addr` fora.

Suítes `test:extract` (19), `test:join` (28) e `geo-polyline` (8) verdes. Os 6 arquivos que
falham em `npm test` já falhavam antes desta branch (fixtures de bz2/xml que dependem de
`npm run fixtures` e do irmão `../seek-bzip`).

## Spec

[`docs/geo/geometria-via-destaque.md`](../geo/geometria-via-destaque.md) — as duas pontas,
incluindo o roteiro do ddsoft (migrations, carregador, `geometria_logradouro`, camada no
mapa), a volumetria a medir e os limites conhecidos (via inteira sem seccionamento,
cobertura de 75 % em SP, `sem_extract` fora do Sudeste).

## O que fica pendente

- Rodar `--way-geom` em SP e trocar as estimativas de volumetria pelos números medidos.
- **Re-rodar o join** nas UFs já carregadas: a coluna 26 não existe nos TXT antigos.
- Todo o lado ddsoft (§5 da spec).
