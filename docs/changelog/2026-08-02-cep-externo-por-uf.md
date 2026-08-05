# 2026-08-02 — CEP externo por UF + sample em dne-geo-br

## Prompt original

> No documento proximo-passo-brasil.md que você gerou, inclua a etapa de import de cep externo
> (veja o doc cep-externo.md). Vamos fatiar o arquivo "G:\dne-geo-br\CEP_EXTERNO.TXT" em um
> arquivo por UF, com os dados já importados, e vamos modificar o script sample-awesomeapi-cep.mjs
> pra ler o cache desses arquivos divididos ao buscar os próximos CEPs. Confirme que esse script
> já está pronto pra ler os arquivos fontes da pasta dne-geo-br.

## O que foi implementado

1. **`cep-externo.js`**: `cachePathForUf`, `listCacheFiles`, `loadCacheMulti`,
   `mergeAndSaveByUf`, `splitCacheByUf`.
2. **`scripts/split-cep-externo-by-uf.mjs`**: parte monólito → `CEP_EXTERNO_{UF}.TXT`.
3. **`scripts/sample-awesomeapi-cep.mjs`**:
   - default `--dir=G:\dne-geo-br`;
   - UFs auto a partir de `DNE_GEO_LOGRADOURO_*.TXT`;
   - cache multi-UF (lê/grava `CEP_EXTERNO_{UF}.TXT`);
   - monólito ainda suportado com `--cache=arquivo`.
4. **`scripts/cep-externo-quality.mjs`**: `--dir` / `--cache-dir`, defaults `dne-geo-br`.
5. **Docs**: `proximo-passo-brasil.md` (Passo 3 CEP), `cep-externo.md`, `operacao-comandos.md`.
6. **Dados**: monólito de 95 050 CEPs fatiado em SP/RJ/MG/ES em `G:\dne-geo-br`.

## Comparação antes/depois

| Antes | Depois |
|-------|--------|
| Um `CEP_EXTERNO.TXT` nacional | `CEP_EXTERNO_SP.TXT` … por UF |
| Sample default `dne-geo-local` + SE | Default `dne-geo-br` + UFs do join |
| Cache sempre monólito | Multi-UF nativo; monólito opcional |

## Como foi testado

- `node --test test/cep-externo.test.js` (6 pass)
- `split-cep-externo-by-uf.mjs` em `G:\dne-geo-br` → SP 52442 · RJ 19833 · MG 15988 · ES 6787
- `loadCacheMulti` na pasta: 95050 CEPs; `DNE_GEO_LOGRADOURO_*` detectadas (27 UFs)

## Testes criados/alterados

- `test/cep-externo.test.js` — multi-UF split/load/save
