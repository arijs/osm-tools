# 2026-07-30 — parar de versionar os resultados de teste

## Prompt original

> Está certo, vamos parar de versionar esses arquivos de resultados de teste

Sobre os `test/fixtures/*-results.json` que sujavam a árvore a cada rodada da suite.

## O que foi feito

`git rm --cached` nos 10 arquivos gerados + regra no `.gitignore`:

```
test/fixtures/*-results.json
test/fixtures/*-results-bzip.json
test/fixtures/*-pbf-results.json
```

Continuam existindo no disco — só deixaram de ser versionados.

## Por que eram ruído

`index0.js` deriva o caminho de saída a partir do input quando `resultsPath` não
é passado (`foo.osm.bz2` → `foo.osm-results.json`). Como a suite roda o indexador
contra as fixtures, ela reescreve esses arquivos toda vez. O diff era sempre o
mesmo: `statsPath` (caminho temporário que carrega o PID do processo) e
`elapsedMs`.

Antes de remover, confirmei que **nada os lê**: os testes que precisam comparar
saída escrevem em diretório temporário (`tempStatsPath`, `path.join(tmp,'r.json')`).
Não são golden files, são subproduto.

## Como foi testado

`node --test test/*.test.js` → **87 testes, 85 pass, 0 fail, 2 skip**, e — o que
importa aqui — `git status` **continua limpo depois da rodada**, mostrando só a
alteração intencional. Era exatamente o sintoma que a mudança tinha que eliminar.

## Arquivos

- `.gitignore`
- `test/fixtures/*-results.json` e `*-results-bzip.json` (untracked, mantidos no disco)
