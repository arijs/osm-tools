# 2026-08-18 — re-rotular os extracts pelo polígono, sem re-extrair o PBF

Sequência de [2026-08-18-uf-por-poligono.md](./2026-08-18-uf-por-poligono.md): o código já
rotula certo, mas os artefatos em `G:\osm-geo-br-geom` foram gravados com o rótulo velho.

## Prompt

> E agora? Eu preciso recapturar os dados?

e, depois do levantamento do que estava no disco:

> faça

## A resposta curta: não

A UF é campo **derivado** da coordenada, e a coordenada está em cada linha gravada
(`OSM_LOGRADOURO_*` tem `lat`/`lng`; `OSM_LOGRADOURO_GEOM_*` tem a polyline, cujo primeiro
ponto é absoluto). Reclassificar o que está no disco custa **minutos**; re-extrair custa
~1h35 por fatia, ~12h o Brasil.

O que estava no disco (todas as oito fatias `done`):

| fatia | rodou | rótulo |
|---|---|---|
| norte, nordeste, centro-oeste, sul, es, rj, sp | 12–13/08 | menor-retângulo-vence |
| mg | 18/08 | paliativo (caixa permitida nomeia) |

## O que foi implementado

`scripts/relabel-uf.js` (`npm run uf:relabel`) — lê os datasets de cada fatia, decide a UF
pelo polígono, deduplica e regrava numa pasta **nova**, com `MANIFEST.json` por dataset (usa o
mesmo `txt-at-writer` do extract, então o contrato de shards é idêntico). Nada é apagado.

```powershell
node scripts/relabel-uf.js --base=G:\osm-geo-br-geom --out=G:\osm-geo-br-uf
node scripts/relabel-uf.js --base=G:\osm-geo-br-geom --dry-run       # só as contas
node scripts/relabel-uf.js --base=... --out=... --only=mg,sp
```

Três decisões que valem registro:

- **Deduplicação por `osm_type` + `osm_id`.** As fatias se sobrepõem muito: 1.204.944 das
  4.005.138 linhas de logradouro (30 %) são a mesma feature vista por duas fatias — a praça do
  Rio estava tanto na `rj` quanto na `mg`. Primeira ocorrência vence; a ordem das fatias e dos
  shards é fixa, então a saída é determinística.
- **O GEOM segue o irmão, não o próprio traçado.** O centróide da via e o primeiro nó dela
  caem em UFs diferentes numa via de divisa; se cada um decidisse por si, o
  `OSM_LOGRADOURO_GEOM_{UF}` deixaria de ser irmão do `OSM_LOGRADOURO_{UF}` — invariante que o
  README-colunas promete. O passo 1 guarda a UF de cada way e o passo 2 obedece. Na corrida
  real, **0 linhas de GEOM ficaram sem irmão**.
- **Aqui o polígono decide sozinho, e isso diverge do extract de propósito.** O extract
  respeita tag/IBGE antes da geometria; a linha gravada não guarda as tags, então não dá para
  reproduzir essa precedência. O rótulo antigo só sobrevive quando o ponto cai fora de todos os
  polígonos (mar, plataforma). O efeito é uma saída *mais* limpa que a de um re-extract: a
  feature com `addr:state` de UF vizinha vai para a UF onde ela geograficamente está.

## Resultado da corrida (Brasil inteiro, 18/08/2026)

`G:\osm-geo-br-geom` → `G:\osm-geo-br-uf`, **111 s**, 726 MB (contra 1,1 GB de entrada — a
diferença é a duplicata).

| | logradouro | geom |
|---|---:|---:|
| linhas lidas | 4.005.138 | 4.004.944 |
| duplicadas (mesma feature em 2 fatias) | 1.204.944 | 1.204.897 |
| **mudaram de UF** | **831.066** | — |
| gravadas | 2.800.194 | 2.800.047 |
| sem irmão | — | 0 |

As maiores correções — o defeito nunca foi só de MG:

| de → para | linhas | o que é |
|---|---:|---|
| MG → SP | 284.232 | retângulo de MG cobrindo o interior paulista |
| MG → RJ | 151.118 | idem, sobre o Rio |
| PB → PE | 70.237 | caixa da PB sobre o agreste pernambucano |
| SC → RS | 48.795 | |
| **GO → MG** | **36.331** | o defeito original: Triângulo, Alto Paranaíba, Noroeste |
| MA → PA | 28.360 | |
| PR → SP | 25.688 | |
| MS → SP | 24.530 | |
| PI → MA | 24.010 | |
| MG → BA | 20.231 | norte de MG |
| RN → PB, TO → PA, TO → MA, ES → MG, SC → PR | 9 a 14 mil cada | |

Nenhuma linha caiu em `XX`.

## Como foi testado

Automatizado (`npm test`): **163 testes, 161 passam, 2 skip** — eram 160/158/2; os skips
continuam sendo as fixtures `.osm` grandes ausentes.

`test/relabel-uf.test.js` (novo) monta duas fatias de mentira com o mesmo escritor do extract
e cobre, com coordenadas reais: Patrocínio saindo de MG, a praça do Rio rotulada MG voltando
para RJ, Catalão indo para GO, a via no mar mantendo o rótulo antigo, a duplicata entrando uma
vez só, o GEOM seguindo o irmão mesmo com o primeiro ponto do outro lado da divisa, o
`MANIFEST.json` batendo com o arquivo, e o `--dry-run` não escrevendo nada.

Campo: conferido o `OSM_LOGRADOURO_MG` da saída — **339.879 linhas, 0 fora do polígono de MG**
— e o `OSM_LOGRADOURO_GEOM_MG` — **339.854 linhas, 0 sem irmão**.

## O que fica em aberto

- **Apontar o join/enrich do DDSOFT para `G:\osm-geo-br-uf`.** Os `G:\osm-geo-br-geom\*`
  antigos continuam no lugar, intactos.
- **Re-extract ainda é o único jeito de recuperar o que o filtro velho nunca deixou entrar**:
  `passesUfFilter` era retângulo, e quatro UFs têm área fora do próprio retângulo — Fernando de
  Noronha (PE, 2,4° a leste), Trindade e Martim Vaz (ES, 10,3°), e faixas de ~8 km (AP) e ~4 km
  (PI). Com o código novo isso se resolve sozinho, porque quem filtra passou a ser o polígono.
  Fica para a próxima atualização do PBF; não vale 12h por ilha oceânica.
