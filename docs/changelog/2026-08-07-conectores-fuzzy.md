# 2026-08-07 — cascata `conectores` + `fuzzy` (dist=1)

## O que entrou

Degraus novos no fim da cascata de nome (`dne-geo-join.js` / `name-keys.js`):

1. **`conectores`** — `stripMidLiga`: remove `de`/`da`/`do`/`das`/`dos`/`e` no meio do núcleo  
   (`Arlindo Moraes Costa` ↔ `Arlindo Moraes da Costa`).
2. **`fuzzy`** — Levenshtein **dist=1** no mid-bare, só se len≥10 (SymSpell K=1).  
   (`San Felipe Neri` ↔ `Sao Felipe Neri`). Dist=2 ficou de fora (Germani↔Germano).

Footprint / desempate / exclusão multi-município / CEP-5 continuam depois.  
`--sem-fuzzy` desliga só o fuzzy.

## Medição SP (`G:\osm-geo-br-sudeste` → `G:\dne-geo-conectores-fuzzy`)

Comparado ao join só com 5f (`G:\dne-geo-neuchatel-test`):

| | Antes | Depois |
|--|------:|-------:|
| `ok` | 260 338 (76,2 %) | **271 692 (79,5 %)** |
| `sem_nome_osm` | 53 293 | **40 408** |
| `conectores` | — | **2 724** |
| `fuzzy` | — | **8 723** |

Exemplos: `1001409` Arlindo Moraes Costa → `ok@conectores`.  
Neuchatel `606476` segue `ok@vizinho_cep5`.  
Alguns near-misses caem em `ambiguo` (2 candidatos no footprint) — correto não chutar.

## Testes

`npm run test:join` — `stripMidLiga` / `levenshtein` + fixture conectores/fuzzy + `--sem-fuzzy`.
