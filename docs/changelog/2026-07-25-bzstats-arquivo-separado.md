# 2026-07-25 — bzStats em arquivo separado (off por padrão)

## Prompt

> o script salva alguns jsons após o processamento. Faça o script salvar os dados de "bzStats" em arquivos separados, e desativado por padrão.

## Implementação

- `saveBzStatsSeparate` (default **false**): grava aggregate em `{stats}-bzip.json`
- Com flag ativa, o stats principal fica com `bzip: []` + `bzipPath`
- Resume carrega bzip do arquivo apontado por `bzipPath` se o array inline estiver vazio
- `saveBzStatsPerMember`: também grava `…-bzip/0000.json`, `0001.json`, …
- CLI: `--save-bz-stats`, `--save-bz-stats-per-member`
- Env: `OSM_SAVE_BZ_STATS=1`, `OSM_SAVE_BZ_STATS_PER_MEMBER=1`, `OSM_BZ_STATS=path`

## Testes

- default sem arquivo separado
- separate + resume reload
- per-member files
