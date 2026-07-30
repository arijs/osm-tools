# 2026-07-30 — docs vivos + envelope + exclusão multi-município + load prejoined

## Prompt original

> vamos fazer os itens 1 a 4 das sugestões
> (1 sincronizar docs; 2 consumir DNE_GEO_* no ddsoft; 3 auditar clusters multi-município;
> 4 cobrir buracos de footprint sem dilatar)

## O que foi implementado

### 1. Docs vivos alinhados ao join feito

- `docs/geo/README.md`, `estado-atual.md`, `bairro-logradouro.md`, `operacao-comandos.md`,
  `objetivo-e-contexto.md`, `dne-geo-join.md` — join não é mais “próximo”; próximo passo é
  load PHP + busca.
- Checklist e mapa mental atualizados.

### 2. ddsoft: load por `log_nu` / `bai_nu`

No monorepo **ddsoft-online**:

- `DneOsmGeoEnricher::loadPrejoinedLogradouro` / `loadPrejoinedBairro`
- `enrichLogradouro` prefere `DNE_GEO_LOGRADOURO_{UF}.TXT` se existir
- `enrichBairro` com `--uf=SP` prefere `DNE_GEO_BAIRRO_{UF}.TXT`
- CLI: `--legacy-match` força o caminho OSM por nome; `--uf` também no bairro

### 3. Fase 5d — exclusão multi-município

Cluster usado por 2+ `loc_nu` com `geo_status=ok` → um dono (maioria de linhas; empate =
âncora mais próxima). Perdedores → `ambiguo` / `conflito_municipio`, sem coordenada.

### 4. Fase 5c — envelope (buraco na pegada)

Para `fora_do_footprint`: entre candidatos de nome, quantos a ≤ `--envelope-tol-km` (default 1)
da mancha (centro−raio das âncoras). Se exatamente 1 e extensão ok → aceita. **Não** dilata o
footprint (evita halo metropolitano).

Flags: `--envelope-tol-km=N`, `--sem-envelope`, `--sem-exclusao-cluster`.

## Comparação antes/depois

| | Antes | Depois |
|--|-------|--------|
| Docs | join “não existe” / “próximo” | join feito; load PHP é o próximo |
| PHP | só match por nome em OSM_* | prefere DNE_GEO_* por chave |
| Cluster multi-mun | 4 % de clusters `ok` em 2+ cidades | dono único; resto `conflito_municipio` |
| Buraco de pegada | `ambiguo` mesmo com km_mancha=0 e 1 cand. perto | recuperável via envelope |

## Como foi testado

- `npm run test:join` — 19 testes, todos ok (incl. fixture de polos + multi-mun)
- `php vendor/bin/phpunit tests/Unit/Osm/DneOsmGeoEnricherTest.php` (ddsoft)

## Testes criados/alterados

- `test/dne-geo-join.test.js` — envelope dedicado, multi-mun, parseCli
- `tests/Unit/Osm/DneOsmGeoEnricherTest.php` (ddsoft) — prejoined logradouro e bairro
