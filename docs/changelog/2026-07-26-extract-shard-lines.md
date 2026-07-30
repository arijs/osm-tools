# Changelog: extract fatiado (`--shard-lines`)

## Prompt

Extrator PBF→TXT com suporte a arquivos fatiados (ex. a cada 10k/100k linhas → `OSM_LOGRADOURO_XX/10000-linhas/000001.txt`) para import retomável.

## O que foi implementado (osm-tools)

- `txt-at-writer.js`: `shardLines`, `shardOnly`, `MANIFEST.json`, `wipeOsmOutputs`
- `extract-geocode-pbf.js`: `--shard-lines=N`, `--shard-datasets=…`
- Testes: `test/txt-at-writer-shard.test.js`
- Docs: `docs/geo/extract-e-artefatos.md`

## Uso

```bash
node extract-geocode-pbf.js G:\sudeste-260725.osm.pbf \
  --out=G:\osm-geo-se-streets-sharded \
  --datasets=logradouro \
  --shard-lines=100000
```
