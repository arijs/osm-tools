# Changelog: cancelar extract com Ctrl+C de verdade

## Prompt original

Ao rodar extract do Brasil com `--region=norte`, o usuário apertou Ctrl+C várias
vezes no começo (datasets errados) e o processo **seguiu até terminar as waves**
do primeiro passe. Queria cancelar de verdade sem fechar o terminal.

## Causa

1. `forEachBlob` era **100% síncrono** — o event loop não rodava durante a wave
   (re-leitura do PBF inteiro). Handlers de `SIGINT` só executavam **depois**.
2. Soft-stop só virava `shouldStop` após **30 s** de trabalho extra, e ainda
   **iniciava waves novas** enquanto o prazo não vencia.
3. Ordem do loop: flush de wave **antes** de checar stop.

## O que foi implementado

1. **`pbf-reader.forEachBlobAsync`** — cede o event loop a cada blob (`setImmediate`).
2. Extract usa async em pass1, waves e pass2.
3. Soft-stop = para no **próximo blob**; **não inicia** wave/pass2 após cancel.
4. Wave cancelada **não emite** logradouros parciais.
5. 2º Ctrl+C: hard-stop + exit forçado em 2 s; 3º: `exit(130)` imediato.
6. Docs em `extract-e-artefatos.md`.

## Comparação antes/depois

| Antes | Depois |
|-------|--------|
| Ctrl+C engolido até o fim da wave (minutos) | Responde entre blobs |
| Soft-stop ainda começava waves | Soft-stop bloqueia novas waves |
| 2º/3º Ctrl+C pouco úteis no meio do sync | Hard-stop + exit forçado |

## Como foi testado

- `npm run test:extract` (inclui soft-stop via `onControl`)
- `npm test`

## Testes criados/alterados

- `test/extract-geocode.test.js` — soft-stop para com `stoppedEarly`
