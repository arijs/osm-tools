# 2026-07-25 — Remover parse-xml.js; index usa XMLParser puro

## Prompt original

> estou vendo que o projeto tem um arquivo "parse-xml.js", dizendo que o "index.js" e o "index0.js" usam ele. Quero eliminar este parse-xml.js, e fazer os arquivos index usarem o "@arijs/stream-xml-parser" puro.

## O que foi implementado

1. **Removido `parse-xml.js`** (adapter Duplex / API SAX legada).

2. **`index0.js` e `index.js`**
   - `const { XMLParser } = require('@arijs/stream-xml-parser')`
   - Callback `onXmlParserEvent` trata `startTag` / `tagName` / `tagAttribute` / `endTag` / `endStream`
   - Open/close de tags OSM e estatísticas derivados dos eventos nativos
   - Sink `stream.Writable` (`createXmlParserSink`) alimenta `parser.write` / `parser.end`
   - Pipeline enxuto: `decoder → lineSplitter → streamPos → xmlSink` (sem camadas object-mode de ParseXML)

3. **Testes**
   - `test/parse-xml.test.js` → `test/xml-parser.test.js` (API nativa)
   - `test/helpers.js` e `test/bz2-fixtures.test.js` atualizados para `XMLParser` puro

4. **README** reescrito sem menção ao adapter; documenta eventos nativos e o padrão dos `index`.

## Comparação antes / depois

| Item | Antes | Depois |
|------|--------|--------|
| Camada XML | `parse-xml.js` (Duplex) | `XMLParser` direto nos `index` |
| Eventos | `{ type: 'opentag', name, attrs }` | `ev.name === 'endTag'` + attrs acumulados |
| Pipeline index0 | + ParseXML + streamXMLPos + closeTag writable | xmlSink único com handlers inline |
| Arquivo `parse-xml.js` | existia | **removido** |

## Como foi testado

```bash
npm test
# 19 pass, 0 fail
```

- `node -e "require('./index0.js')"` carrega o módulo (falha só no path do dump OSM ausente — esperado).

## Testes criados / alterados

| Arquivo | Status |
|---------|--------|
| `parse-xml.js` | **removido** |
| `test/parse-xml.test.js` | **removido** |
| `test/xml-parser.test.js` | criado |
| `test/helpers.js` | alterado |
| `test/bz2-fixtures.test.js` | alterado |
| `index.js`, `index0.js` | alterados |
| `README.md` | alterado |
