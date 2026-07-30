# 2026-07-30 — `dne-geo-join.js`: junção OSM ↔ DNE com footprint municipal

## Prompt original

> Implemente

Referindo-se à especificação escrita em `docs/geo/dne-geo-join.md`.

## O que foi implementado

Três arquivos novos, seguindo a spec:

| Arquivo | Papel |
|---------|-------|
| [`name-keys.js`](../../name-keys.js) | `coreName` (núcleo sem tipo de logradouro), `phoneticKey` (chave fonética PT-BR), guardas de área (`isAreaTlo`/`isAreaKind`) |
| [`geo-cluster.js`](../../geo-cluster.js) | `clusterFeatures` (single-link em grade + union-find), `aggregate` (bbox união + centroide ponderado), `buildFootprint`/`inFootprint`/`footprintOverlap`, `distKm` |
| [`dne-geo-join.js`](../../dne-geo-join.js) | CLI: lê DNE (latin1) e OSM (utf8), roda as 6 fases, emite os 3 artefatos |

Fluxo: cluster por nome → âncoras (nome com 1 `loc_nu` e 1 cluster) → footprint municipal em grade
de 0,01° dilatada → cascata `exato → area → name_alt → addr → nucleo → fonetico` filtrada pelo
footprint → desempate (bairro, depois tamanho) → emissão. Duas voltas: a segunda usa o footprint
crescido e os centroides de bairro derivados da primeira.

## Resultado no Sudeste

| UF | Linhas DNE | `ok` | % | `ambiguo` | `sem_nome_osm` |
|----|-----------:|-----:|--:|----------:|---------------:|
| SP | 341 813 | 263 478 | **77,1 %** | 22 281 | 56 054 |
| ES | 31 993 | 23 972 | 74,9 % | 735 | 7 286 |
| RJ | 103 723 | 74 588 | 71,9 % | 8 001 | 21 134 |
| MG | 128 929 | 70 582 | 54,7 % | 13 812 | 44 535 |

**Capital paulista: 88,6 %**, acima da meta de 85 % da spec. SP inteiro roda em ~47 s.

Os cinco casos de regressão saem **idênticos** aos exemplos que a spec documentou — inclusive as
quatro avenidas que hoje ficam sem coordenada nenhuma no `osm:dne:enrich-geo`.

## Antes / depois

```diff
- Avenida Paulista (log_nu 608719): lat=null lng=null  (ambíguo em 19 municípios)
+ 608719@…@-23.5621600@-46.6554120@-23.5713665@-23.5545149@-46.6640565@-46.6442094@ok@exato@primary+secondary@65@4
```

## Defeito encontrado pelo próprio critério de aceite

O critério "nenhuma via > 15 km sem `kind` de rodovia" — escrito na spec antes do código — pegou um
defeito na **primeira execução**: `Rua Dois` em Guarulhos saiu com 29 ways cobrindo **25 km**.

Causa: clusterização single-link encadeia. Nome genérico espalhado pela cidade forma uma corrente de
células vizinhas que liga pontas distantes. O discriminador contra via longa legítima veio dos
próprios dados: `Rodovia Arão Sahm` (17 km) e `Avenida Presidente Kennedy` em Praia Grande (20 km,
orla real) aparecem com **`osm_clusters=1`**, enquanto `Rua Dois` tinha 19 candidatos.

Guarda adicionada (`--max-extent-km`, default 15): extensão > 15 km **e** mais de um candidato →
`geo_status=ambiguo`, sem coordenada. Custo: 42 linhas em SP. Depois dela, o detector só acusa as
duas vias legítimas.

## Outras correções durante a implementação

- **`ch` e `x` não colapsavam** na chave fonética: `chave` virava `khave` e `xavier` virava
  `chavier`. Corrigido convertendo `ch → x` antes do `c → k`. `Xavier ≡ Chavier` virou teste.
- **`writer.closeSync()` não espera o stream fechar** — o nome engana; ele só chama `stream.end()`.
  No caminho async o arquivo podia estar incompleto quando `run()` retornava. Trocado por
  `await writer.flush()`.
- **Rótulo `addr` nunca disparava**: os pontos de numeração entram no mesmo índice de nomes, então
  casavam como `exato`. Agora o cluster formado só por pontos `addr` é rotulado corretamente — são
  133 linhas em SP, confirmando que o dataset rende pouco como fonte de nome.

## Como foi testado

**Suite completa:** `node --test test/*.test.js` → **87 testes, 85 pass, 0 fail, 2 skip** (os 2 skip
são fixtures grandes, já puladas antes desta alteração).

**Unitários novos** (15) — `test/name-keys.test.js`, `test/geo-cluster.test.js`:
núcleo sem tipo (incluindo `Travessa Goiás` ≡ `Rua Goiás` e o caso degenerado "nome que é só o
tipo"); chave fonética com 10 pares reais medidos na base; preservação de `lh`/`nh`; nomes distintos
que **não** podem colapsar; cluster separando homônimos a 50 km e juntando 10 segmentos contíguos de
uma avenida; centroide ponderado por `way_node_count`; `n=0` sem divisão por zero; dilatação do
footprint; `footprintOverlap`; escala de `distKm`.

**Integração** (3) — `test/dne-geo-join.test.js`, com fixture DNE escrita em **latin1**: contrato de
25 colunas; desnormalização (distrito herdando IBGE do `LOC_NU_SUB`); cada degrau da cascata
marcando sua `geo_regra`; guarda kind-aware barrando parque homônimo de uma `Rua`; footprint
escolhendo o cluster de perto contra um **mais pesado** a 150 km; distrito sem âncora própria
herdando a pegada do pai; linha sem match com todas as colunas de geometria vazias; UF sem extract
saindo inteira como `sem_extract`; `parseCli`.

**Verificação na base real:** os 5 casos de regressão conferidos linha a linha na saída de SP;
detector de bbox e detector de "área casada com TLO de via" (0 ocorrências em 341 813 linhas).

Dois testes meus estavam errados e foram corrigidos, não o código:

1. `-23.5505` **não** está na mesma célula de `-23.55` — com coordenada negativa, `floor(-2355.05)`
   é `-2356`. Registrado como comentário no teste, porque erra fácil de novo.
2. A fixture dava âncora própria ao distrito, então não exercitava a herança. Reescrita para o
   distrito ter 2 clusters (logo, sem âncora), com o cluster distante mais pesado — assim o teste
   falha se o footprint herdado não funcionar.

## Arquivos

Novos: `name-keys.js`, `geo-cluster.js`, `dne-geo-join.js`, `test/name-keys.test.js`,
`test/geo-cluster.test.js`, `test/dne-geo-join.test.js`.
Alterados: `package.json` (scripts `dne:join` e `test:join`), `docs/geo/dne-geo-join.md`
(status, guarda de extensão, resultados, aceite).

## Próximo passo

Do lado do ddsoft: trocar o match do `DneOsmGeoEnricher` por load de
`DNE_GEO_LOGRADOURO_{UF}.TXT` por `log_nu` — ver `docs/geo/bairro-logradouro.md`.
A guarda kind-aware no PHP deixa de ser necessária nesse caminho, já que o join não emite área para
`TLO_TX` de via.
