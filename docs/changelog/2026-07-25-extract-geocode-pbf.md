# Changelog: extract geocode PBF → TXT `@` (fase 2a)

## Prompt original

Rodei o script pbf (Sudeste) e os JSONs foram gerados (~10 MB); qual a próxima etapa do geocoder? Há banco com estados/municípios/bairros/logradouros esperando lat/lon (`estrutura-dados-endereco.md`). Preferência: **CSV delimitados no modelo DNE** (não SQLite); logradouro **segmentado por UF**.

## O que foi implementado

1. **Plano** `docs/plans/osm-para-locais-geo.md` — pipeline inventário → extract TXT `@` → match PHP `locais` → busca.
2. **`extract-geocode-pbf.js`** — extrai features admin/place/highway para:
   - `OSM_ESTADO.TXT`, `OSM_MUNICIPIO.TXT`, `OSM_BAIRRO.TXT`
   - `OSM_LOGRADOURO_{UF}.TXT` (SP/RJ/MG/ES/… + residual XX)
   - opcional `OSM_ADDR_POINT_{UF}.TXT`
   - `README-colunas.md` + checkpoint `extract-checkpoint.json`
3. Helpers: `name-norm.js`, `uf-br.js`, `txt-at-writer.js`.
4. **Inventário mais leve:** `coord-layout` snapshot com `maxBlocks` (default **0** no `index-pbf` = sem gravar blocks; `--layout-full-blocks` / `--layout-max-blocks=N`).
5. Fixture `scripts/write-geocode-pbf.js` + testes.
6. Atualização de `docs/objetivo-e-contexto.md` (fase 2a).

## Comparação antes/depois

| Antes | Depois |
|-------|--------|
| Só inventário (contagens/samples/blocks ~10 MB) | Extract de features nomeadas para TXT `@` |
| Sem artefato para match com `locais` | Contrato de colunas DNE-like + split por UF em logradouro |
| `coordLayout.blocks` sempre no JSON | Omitidos por default no inventário |

## Como foi testado

- `npm test` (suite completa)
- Extract no fixture `geocode-mini.osm.pbf` → valida IBGE município, logradouro em `OSM_LOGRADOURO_SP.TXT`, estado/bairro/addr

## Testes criados/alterados

- `test/extract-geocode.test.js` (novo)
- `test/name-norm.test.js` (novo)
- `test/uf-br.test.js` (novo)
- `test/coord-layout-snapshot.test.js` (novo)
- `scripts/write-geocode-pbf.js` (novo)

## Follow-up 2026-07-26

- **Causa do 99% XX:** `logradouroNoGeom` ~1,23M — LRU de 500k nós esvazia antes das ways (ordem OSM: todos os nodes, depois ways).
- **Correção:** two-pass default para logradouro (pass1 agenda refs; pass2 harvest coords → UF por ponto).
- Re-extrair Sudeste: `node extract-geocode-pbf.js G:\sudeste-260725.osm.pbf --out=G:\osm-geo-se`
- Match `locais`: CLI PHP no ddsoft `osm:locais:enrich-geo` (só estado/município).
