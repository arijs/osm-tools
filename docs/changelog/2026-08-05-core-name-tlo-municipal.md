# 2026-08-05 — `coreName` stripa TLO composto (`Estrada Municipal`)

## Prompt original

> Rodei o join; em `sem_nome_osm_exemplos` aparece
> "Estrada Municipal Professora Therezinha de Lima Belloto". Apesar do `tlo` ser
> "Estrada Municipal", a palavra "municipal" ainda ficou em `nucleo` e `nucleo_bare`.
> Não devia ter sido removida?

## O que foi implementado

Em `name-keys.js`:

- Lista `TIPO_MOD`: `municipal`, `estadual`, `federal`, `vicinal`
- `coreName` remove esses tokens **somente depois** de ter visto um `TIPOS` real
  (`estrada municipal X` → `X`; `municipal X` sozinho não mexe)

Efeito colateral desejado: com `municipal` fora do caminho, `stripTitulos` passa a
enxergar `professora` e `coreBare` vira `therezinha de lima belloto`.

## Antes / depois

| Campo | Antes | Depois |
|-------|-------|--------|
| `nucleo` | `municipal professora therezinha de lima belloto` | `professora therezinha de lima belloto` |
| `nucleo_bare` | idem (professora bloqueada) | `therezinha de lima belloto` |

## Como foi testado

`npm run test:join` → 26 pass (incl. regressão Therezinha / Estrada Municipal).

## Testes

- `test/name-keys.test.js` — TLO composto + coreBare com Professora
