# 2026-07-25 — Resume planet stats, soft-stop, Buffer, legenda

## Prompt original

> (1) Deprecation Buffer() (2) legenda/header da linha de progresso (3) soft-stop Ctrl+C em ≤30s no melhor ponto (4) erro `Cannot read properties of undefined (reading 'blocks')` em `H:\osm\planet-latest.osm.bz2`

## Causa do erro principal (resume)

O JSON legado `H:\osm\planet-latest.osm-stats.json` (2018) tem `current.bzFile` / `bzBlock` mas **não** tem:

- `current.bzCurrentFile`
- `current.bzNextFileOffset`

No resume fazíamos `bzFile = stats.current.bzCurrentFile` → `undefined`, e o próximo `bzFinishBlock` quebrava em `bzFile.blocks.push`.

## Correções

1. **Resume robusto**
   - `ensureBzFile` / `polluteBzCurrentFile` se o campo faltar
   - `bzNextFileOffset` fallback para `fopt.fileOffset`
   - `bzFinishBlock` defensivo
   - Navegação XML no resume com fallback se a tag sumir da árvore

2. **Soft-stop real**
   - Bug: SIGINT só setava flag local no `main()` e **não** ligava ao `runProcess`
   - Agora: `onControl({ softStop, hardStop })`
   - 1º Ctrl+C → soft-stop (membro bzip / XML perto da raiz / timeout 30s)
   - 2º Ctrl+C → hard-stop (para no fim do bloco atual)
   - 3º Ctrl+C → `process.exit(130)`

3. **Legenda + header** antes do progresso

4. **`new Buffer()` → `Buffer.alloc`/`from`** em `@arijs/seek-bzip` (`../seek-bzip`)

## Testes

- `index0 resume works with legacy stats missing bzCurrentFile`
- `index0 soft-stop finishes quickly via onControl` (~600ms no medium)

## Como validar planet

```bash
node index0.js H:\osm\planet-latest.osm.bz2
# deve retomar sem TypeError; legenda no início
# Ctrl+C → soft-stop ≤30s; 2º Ctrl+C → hard-stop
```
