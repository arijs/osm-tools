# O que o script PBF grava (`index-pbf.js`)

Inventário de **`.osm.pbf`** com a mesma meta de geocoding que o pipeline XML.  
Plano: [plans/pbf-inventory.md](./plans/pbf-inventory.md).

## CLI

```bash
node index-pbf.js G:\sudeste-260725.osm.pbf
node index-pbf.js arquivo.osm.pbf [stats.json] [results.json]
```

| Arquivo | Conteúdo |
|---------|----------|
| `…-pbf-stats.json` | cursor de resume, header, geocodeSignals, coordLayout, tagKeyCounts, runs |
| `…-pbf-results.json` | resumo da execução + mesmos sinais/layout |

## Campos principais

### `header` (do OSMHeader)

- `bbox` em graus (`left/right/top/bottom`) — deve cobrir o extract (ex. Sudeste)
- `source`, `writingprogram`, `required_features`

### `cursor`

- `fileOffset` — byte do próximo blob (resume)
- `blobIndex` — quantos blobs já foram processados
- `eof` — chegou ao fim do arquivo

### `geocodeSignals`

Igual ao XML — ver [o-que-o-script-grava.md](./o-que-o-script-grava.md) §4.1.  
Nodes vêm de **DenseNodes** (padrão em PBF moderno).

### `coordLayout`

Igual ao XML — saltos em **graus**, samples, blocos.  
Aqui cada “bloco” de layout corresponde a um **blob OSMData** (`bzBlock` = `blobIndex` por compatibilidade com `coord-layout.js`).

### `tagKeyCounts` / `tagKeyCountsTop`

Mapa e top-40 de chaves de tag OSM vistas (útil para achar `addr:*` raros).

## Progresso

```text
12% 40MB/s  … / blob 1204 / GEO n=… addr=… / lay~seq seq=0.7 meanJump=0.02°
```

Soft-stop: Ctrl+C para no fim do blob atual.

## Diferenças vs `index0.js` (XML)

| | XML/bz2 | PBF |
|--|---------|-----|
| Unidade | bloco bzip | blob PrimitiveBlock |
| Parser | XML stream | protobuf DenseNodes |
| Changesets no início do planet | comum | extract regional costuma já ter nodes |
| Árvore XML aninhada | sim | contagens flat + tagKeyCounts |
