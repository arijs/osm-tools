# Changelog: extract Brasil — waves + filtro UF/região

## Prompt original

Extração do Brasil inteiro (`brazil-260724.osm.pbf`, ~1.9 GB) estourou:

```text
RangeError: Set maximum size exceeded
  at Set.add (processFeatureWay → neededNodeIds)
```

Ideia do usuário: fatiar por região (norte, nordeste, centro-oeste, sul) e no
sudeste por UF (ES, MG, RJ, SP).

## Causa

Two-pass de logradouro agenda **todos** os node ids de ways nomeadas em
`neededNodeIds` (`Set`). No PBF nacional o Set passa do teto do V8 (~16,7 M
elementos) antes da pass 2.

## O que foi implementado

1. **Waves (default)** — `flushStreetWave` no meio da pass 1 quando
   `neededNodeIds >= 8e6` ou `pendingStreets >= 150000`: re-lê o PBF, resolve
   coords, grava logradouros, zera o Set. Evita o `RangeError` mesmo sem filtro.
2. **`--uf=SP,RJ` e `--region=norte|nordeste|centro-oeste|sudeste|sul`** —
   grava só a fatia; early-skip quando tags/IBGE já apontam UF fora; filtro
   final por UF resolvida + bbox.
3. **`uf-br.js`** — bboxes de **todas** as UFs (não só SE), mapa `REGIOES`,
   `parseUfFilter`, `passesUfFilter`, `tagsDisallowedByFilter`.
4. **Admin_centre** em `neededAdminNodeIds` separado (não é limpo nas waves de
   rua).
5. Progresso com `pend=` / `need=`; checkpoint grava filtro e wave limits.
6. Docs de operação com receita PowerShell para o Brasil.

## Comparação antes/depois

| Antes | Depois |
|-------|--------|
| Brasil inteiro crasha no Set ~16M | Waves liberam Set; run completa (mais I/O) |
| Só bboxes SE para ponto→UF | Bboxes nacionais |
| Sem recorte por UF/região no extract | `--uf` / `--region` |
| admin_centre no mesmo Set das ruas | set separado |

## Como foi testado

- `npm run test:extract`
- `npm test` (suite)

## Testes criados/alterados

- `test/uf-br.test.js` — regiões, filtro, pontos AM/RS
- `test/extract-geocode.test.js` — `--uf=SP` vs `RJ`, wave forçado, `parseCli`

## Como rodar o Brasil (resumo)

```powershell
$env:NODE_OPTIONS="--max-old-space-size=8192"
node extract-geocode-pbf.js G:\brazil-260724.osm.pbf --out=G:\osm-geo-br-sp `
  --datasets=estado,municipio,bairro,logradouro --uf=SP --shard-lines=20000
```

Ver `docs/geo/operacao-comandos.md` § Brasil inteiro.
