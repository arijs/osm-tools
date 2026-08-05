# Changelog: resumo final com bairros

## Prompt original

Extract norte terminou com `bairro: 1994` no JSON, mas a linha de resumo só
mostrava Municípios e Logradouros.

## O que foi implementado

`formatExtractSummary(stats)` — stderr final:

`Municípios: … | Bairros: … | Logradouros: … (sem geom: …) | waves=…`

(Estados/Addr só se count > 0.)

## Como foi testado

`npm run test:extract`

## Testes

- `test/extract-geocode.test.js` — `formatExtractSummary`
