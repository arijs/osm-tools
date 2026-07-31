# 2026-07-30 — cache `CEP_EXTERNO.TXT` (padrão DNE)

## Prompt

> formato melhor para resultados AwesomeAPI, padrão DNE; próximas consultas não
> reconsultam CEPs já consultados

## O que foi feito

- Spec: `docs/geo/cep-externo.md` — 13 colunas `@`, chave = CEP 8 dígitos
- Lib: `cep-externo.js` (parse/format/load/merge)
- `scripts/sample-awesomeapi-cep.mjs` grava/lê o cache; skip de CEPs presentes
- `scripts/cep-externo-from-jsonl.mjs` migra a amostra JSONL antiga
- Testes: `test/cep-externo.test.js` (5)
- Amostra convertida em `G:\dne-geo-local\CEP_EXTERNO.TXT` (1000 CEPs); re-run
  pulou 1000 e só consultou novos

## Como usar

```bash
node scripts/sample-awesomeapi-cep.mjs --cache=G:\dne-geo-local\CEP_EXTERNO.TXT --n=1000
# --force  refaz todos; --retry-errors só status=error
```
