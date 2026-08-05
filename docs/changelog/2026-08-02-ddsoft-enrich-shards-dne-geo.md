# 2026-08-02 — ddsoft `osm:dne:enrich-geo` shards + DNE_GEO

## Prompt original

> parece que o importador do ddsoft 'osm:dne:enrich-geo' não consegue lidar ainda com os arquivos
> divididos em shards - precisamos adicionar o suporte

## O que foi implementado (repo ddsoft-online)

1. **`OsmTxtSource`**: `tryResolve`, `listSuffixes`; shards mesmo sem MANIFEST; prefere pasta
   fatiada sobre flat.
2. **`DneOsmGeoEnricher`**:
   - preferência **`DNE_GEO_LOGRADOURO_{UF}`** (load por `log_nu`, `geo_status=ok`);
   - preferência **`DNE_GEO_BAIRRO_{UF}`** (load por `bai_nu`);
   - fallback OSM com **shards** em `OSM_LOGRADOURO_*` e `OSM_BAIRRO`;
   - `--legacy-match` força OSM.
3. **`OsmDneEnrichGeoCommand`**: `--uf` opcional (todas as UFs da pasta); `--legacy-match`.

## Comparação antes/depois

| Antes | Depois |
|-------|--------|
| Bairro só `OSM_BAIRRO.TXT` flat | Shards + prejoined `DNE_GEO_BAIRRO_*` |
| Logradouro só match OSM por nome | Prejoined `DNE_GEO_*` por chave; OSM legado com shards |
| `--uf` obrigatório no logradouro | Omitido = todas as UFs detectadas |

## Como foi testado

```bash
cd D:\dev\ddsoft\ddsoft-online
php vendor/bin/phpunit tests/Unit/Osm/
# OK (20 tests)
```

Smoke: `resolve(G:/dne-geo-br, DNE_GEO_LOGRADOURO_SP)` → flat; norte `OSM_BAIRRO` → sharded.

## Testes criados/alterados

- `tests/Unit/Osm/OsmTxtSourceTest.php`
- `tests/Unit/Osm/DneOsmGeoEnricherTest.php` (prejoined, shards bairro/logradouro)
