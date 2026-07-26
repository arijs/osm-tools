# 2026-07-25 — Fixtures grandes, testes index0, index.js documentado

## Prompt original

> faça o gerador de fixtures criar arquivos fixture maiores para teste (com 20MB e 200MB, que obviamente devem ser ignorados pelo git), e crie testes para eles. Documente como testar somente uma das fixtures em vez de todas num comando só. Quero criar testes para o arquivo index0.js também. O arquivo index.js parece corrompido, possui um "@TODO". Veja se é simples de arrumar ou se o script está fundamentalmente incompleto/quebrado.

## O que foi implementado

### Fixtures
- `scripts/generate-fixtures.py`: `--large`, `--only NAME`, streaming para large
- `large20` (~20 MB XML) e `large200` (~200 MB XML), só `.bz2` (sem raw)
- `.gitignore` ignora `large20*` / `large200*`
- Testes opcionais (skip se ausentes) em `bz2-fixtures.test.js` e `index0.test.js`

### index0.js
- Refatorado para exportar `runProcess(options)` + CLI (`require.main`)
- Opções: `quiet`, `resume`, `saveStats`, `checkpointEveryFiles`, `syncSchedule`
- `test/index0.test.js` cobre tiny/small/medium e large opcional

### index.js
- **Conclusão: fundamentalmente incompleto**, não um typo isolado
- Substituído por stub que documenta os problemas e aponta para `index0.js`
- Problemas: `@TODO` de sintaxe, `readBlock` com assinatura errada, `unbzip2` sem require, `bzInitFile` sem return, pipelines pela metade

### Como rodar um subset de testes
- `scripts/run-named-tests.js` + `npm run test:fixture -- <pattern>`
- Documentado no README

## Comparação antes / depois

| Item | Antes | Depois |
|------|--------|--------|
| Fixtures | tiny/small/medium | + large20 / large200 (gitignore) |
| index0 | script com path hardcoded no load | `runProcess` + CLI |
| Testes index0 | — | `test/index0.test.js` |
| index.js | syntax error + draft quebrado | stub explícito |
| Filtro de testes | — | `npm run test:fixture -- tiny` |

## Como foi testado

```bash
npm run fixtures
python scripts/generate-fixtures.py --only large20
node --test test/index0.test.js test/xml-parser.test.js test/bz2-fixtures.test.js
# 29 pass, 6 skip (large200 + raw large20), 0 fail

npm run test:fixture -- tiny   # via run-named-tests.js
node index.js                  # exit 1 + mensagem
```

## Testes criados / alterados

| Arquivo | Status |
|---------|--------|
| `scripts/generate-fixtures.py` | alterado |
| `scripts/run-named-tests.js` | criado |
| `test/index0.test.js` | criado |
| `test/bz2-fixtures.test.js` | alterado |
| `index0.js` | refatorado |
| `index.js` | stub |
| `.gitignore` | large fixtures |
| `package.json` | scripts |
| `README.md` | documentado |
