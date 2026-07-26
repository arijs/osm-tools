# 2026-07-25 — Migrar XML parser, fixtures BZ2 e testes

## Prompt original

> estude este projeto, mude o pacote npm "node-xml-stream-parser" para usar "@arijs/stream-xml-parser", crie alguns arquivos BZ2 de alguns tamanhos diferentes e crie testes para eles, e crie um README documentando tudo

## O que foi implementado

1. **Dependência XML**
   - Removido `node-xml-stream-parser` (path local quebrado).
   - Adicionado `@arijs/stream-xml-parser@^0.2.24`.
   - Ajustado `@arijs/seek-bzip` para `file:../seek-bzip`.

2. **`parse-xml.js` reescrito**
   - Adapter `stream.Duplex` sobre `XMLParser` do `@arijs/stream-xml-parser`.
   - Mantém a superfície de eventos do parser antigo (`opentag`, `closetag`, `text`, `cdata`, `instruction`, `unparsed_remain`, `error`) para compatibilidade com `index.js` / `index0.js`.
   - Mapeia tags self-closing para open+close; parseia corpo de `<?xml …?>` em `name` + `attrs`.

3. **Fixtures BZ2** (`test/fixtures/`)
   - Gerador: `scripts/generate-fixtures.py`
   - `tiny`, `small`, `medium` (`.osm` + `.osm.bz2`) + `manifest.json`
   - Medium com `compresslevel=1` para múltiplos blocos bzip2.

4. **Testes** (Node built-in `node:test`)
   - `test/parse-xml.test.js` — parser XML
   - `test/bz2-fixtures.test.js` — decode, blocos, contagens, pipeline completo
   - Helpers: `test/helpers.js`
   - Script npm: `"test": "node --test test/**/*.test.js"`

5. **Documentação**
   - `README.md` completo (arquitetura, migração, API, fixtures, testes)
   - Este changelog

## Comparação antes / depois

| Item | Antes | Depois |
|------|--------|--------|
| Parser XML | `node-xml-stream-parser` (file: path inválido) | `@arijs/stream-xml-parser` no npm |
| `parse-xml.js` | EventEmitter do parser antigo | Adapter sobre `XMLParser` com mesma API de eventos |
| Testes | `"Error: no test specified"` | 19 testes automatizados |
| Fixtures | — | tiny / small / medium `.osm.bz2` |
| README | — | Documentação do projeto |
| `package.json` name | `osm-parser` | `osm-tools` |

## Como foi testado

### Automatizado

```bash
npm test
# 19 pass, 0 fail
```

Cobertura exercitada:

- Constantes e eventos ParseXML (instrução, open/close, self-close, text, cdata, chunk boundaries)
- Existência e tamanhos relativos das fixtures
- `SeekBzip.decode` vs arquivo `.osm` cru
- `SeekBzip.readBlock` bloco a bloco (medium com ≥2 blocos)
- Contagem de `node` / `way` / `tag` no XML
- Pipeline `bz2 → StreamingDecoder → ParseXML`

### Manual

- `python scripts/generate-fixtures.py` gerou fixtures com tamanhos esperados
- `require('@arijs/stream-xml-parser')` validado em Node 22 (CJS require de ESM)

## Testes criados / alterados

| Arquivo | Status |
|---------|--------|
| `test/parse-xml.test.js` | criado |
| `test/bz2-fixtures.test.js` | criado |
| `test/helpers.js` | criado |
| `test/fixtures/*` | criados (gerados) |
| `scripts/generate-fixtures.py` | criado |

## Arquivos principais alterados

- `package.json` / `package-lock.json`
- `parse-xml.js`
- `README.md`
- `docs/changelog/2026-07-25-stream-xml-parser-e-testes-bz2.md`
