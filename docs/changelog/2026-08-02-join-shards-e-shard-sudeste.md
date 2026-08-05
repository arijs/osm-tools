# 2026-08-02 — join lê shards + script para fatiar o Sudeste

## Prompt original

> Eu prefiro usar os arquivos em shards. Então precisamos de dois scripts: (1) um pra dividir o
> sudeste em shards de 20000 linhas; e (2) alterar o dne-geo-join pra ser capaz de processar os
> arquivos em shards sem necessidade de concatenar eles. Aproveite e veja pra mim o Passo 0,
> confirme que todas as regiões estão completas.

## O que foi implementado

1. **`txt-at-writer.js`**: `resolveDatasetPaths` / `datasetExists` — resolve flat **ou** pasta de
   shards (`MANIFEST.json` ou scan de `*.txt`). Prefere pasta de shards se existir.
2. **`dne-geo-join.js`**: `loadOsmStreets` e `loadAddrPoints` leem todos os arquivos do dataset em
   sequência; relatório inclui `osm.mode` e `osm.files`.
3. **`scripts/shard-osm-txt.js`**: parte `OSM_LOGRADOURO_*` / `OSM_BAIRRO` / `OSM_ADDR_POINT_*`
   flat em shards (`--shard-lines=20000`, `--remove-flat`, `--force`, `--uf=`).
4. **Docs**: `proximo-passo-brasil.md` (Passo 0 com números reais; flatten removido; loop aponta
   direto a `G:\osm-geo-br-*`), `operacao-comandos.md`, `dne-geo-join.md`.

## Passo 0 (conferido em G:\)

| Pasta | eof | Formato |
|-------|:---:|---------|
| osm-geo-br-admin | true | flat estado/município |
| osm-geo-br-norte | true | shards, MANIFEST complete |
| osm-geo-br-nordeste | true | shards, MANIFEST complete |
| osm-geo-br-centro-oeste | true | shards, MANIFEST complete |
| osm-geo-br-sul | true | shards, MANIFEST complete |
| osm-geo-br-sudeste | true | flat (SP/RJ/MG/ES) — fatiar com o script |

## Comparação antes/depois

| Antes | Depois |
|-------|--------|
| Join só abria `OSM_LOGRADOURO_{UF}.TXT` | Lê flat ou `OSM_LOGRADOURO_{UF}/{N}-linhas/*.txt` |
| Docs pediam concatenar shards | Desnecessário |
| Sudeste só flat | `node scripts/shard-osm-txt.js --dir=… --shard-lines=20000` |

## Como foi testado

```bash
node --test test/txt-at-writer-shard.test.js test/shard-osm-txt.test.js \
  test/dne-geo-join.test.js test/name-keys.test.js test/geo-cluster.test.js
# 25 pass
```

Inspeção de `G:\osm-geo-br-*` (checkpoint + MANIFEST por UF oficial).

## Testes criados/alterados

- `test/txt-at-writer-shard.test.js` — `resolveDatasetPaths`
- `test/shard-osm-txt.test.js` — novo
- `test/dne-geo-join.test.js` — join com OSM em 2 shards
