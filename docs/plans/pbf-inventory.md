# Plano: inventário PBF para geocoding (`index-pbf.js`)

> Plano aprovado e versionado no repositório. Implementação: `index-pbf.js`, `pbf-reader.js`, `fileformat.proto.js`, `geocode-signals.js`.

## Meta

Mesma de `index0.js` (XML/bz2): explorar dados para **geocoding** e **geocoding reverso**, sem implementar o geocoder ainda.

Arquivo prioritário: `G:\sudeste-260725.osm.pbf` (~813 MB, extract Sudeste BR).

## Entrega desta fase

- Ler OSM PBF em streaming (blobs)
- `geocodeSignals` + `coordLayout` (reuso)
- Progresso por PrimitiveBlock + soft-stop/resume
- Stats/results JSON (sem embutir todas as coords)

## Arquitetura

```
.pbf → pbf-reader (BlobHeader/Blob/zlib) → PrimitiveBlock
     → DenseNodes/Ways/Relations → geocodeSignals + coordLayout
     → *-pbf-stats.json / *-pbf-results.json
```

Unidade de resume: `fileOffset` do próximo BlobHeader + índices de blob.

## Distância / layout

Saltos em **graus** (`hypot(Δlat,Δlon)`), ver `docs/plans/coord-layout.md`.

## CLI

```bash
node index-pbf.js G:\sudeste-260725.osm.pbf
```
