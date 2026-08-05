# `dne-geo-join.js` — especificação

> **Status: implementado.** Sudeste 2026-07-30; re-join SP com título/CEP-5/TLO composto em
> 2026-08 (`G:\dne-geo-br-join-sudeste`). Código em
> [`dne-geo-join.js`](../../dne-geo-join.js), [`name-keys.js`](../../name-keys.js),
> [`geo-cluster.js`](../../geo-cluster.js). Resultado real em §Resultado e §Fase 4.
> O diagnóstico que justifica cada decisão está em
> [melhoria-extracao-coordenadas.md](./melhoria-extracao-coordenadas.md).

## Precisa existir?

Sim. O casamento OSM↔DNE **não é um problema de nome** — é de contexto espacial, e o contexto não
cabe num importador PHP linha a linha:

1. A way OSM não diz a que município pertence (`addr:city` em 0,01 % das linhas). Sem isso,
   `Avenida Paulista` é ambígua em 19 municípios de SP e fica sem coordenada.
2. Mesmo **dentro** de um município há homônimos. `Rua Augusta` tem 33 ways na capital, em 4 lugares
   diferentes. Agregar todas dá um bbox de **28 × 28 km** — inútil.
3. Resolver os dois exige índice espacial de 793 mil ways e duas passadas de convergência.

Isso é trabalho de ferramenta de dados. O produto dele é um TXT auditável e regenerável, e o PHP
volta a fazer o que sabe: carregar por chave.

## Entradas e saídas

```
D:\…\Delimitado\                       G:\osm-geo-br-nordeste\   (ex.)
  LOG_LOCALIDADE.TXT   (latin1)          OSM_LOGRADOURO_{UF}.TXT   (flat)
  LOG_BAIRRO.TXT       (latin1)            ou OSM_LOGRADOURO_{UF}/  (shards)
  LOG_LOGRADOURO_{UF}.TXT (latin1)       OSM_ADDR_POINT_{UF}…      (opcional)
         │                                        │
         └────────────────┬───────────────────────┘
                          ▼
                 dne-geo-join.js --uf=BA
                          │
      ┌───────────────────┼────────────────────┐
      ▼                   ▼                    ▼
DNE_GEO_LOGRADOURO_*.TXT  DNE_GEO_BAIRRO_*.TXT  DNE_GEO_RELATORIO_*.json
```

OSM: se existir a **pasta** de shards, ela tem prioridade sobre o `.TXT` flat. Shards são lidos
em ordem do `MANIFEST.json` (sem concatenar em disco).

```bash
set NODE_OPTIONS=--max-old-space-size=8192
node dne-geo-join.js ^
  --dne=D:\dev\ddsoft\ddsoft-online\_ignore\Delimitado ^
  --osm=G:\osm-geo-br-sudeste ^
  --out=G:\dne-geo-br --uf=SP
```

Opções: `--cluster-cell=0.02`, `--footprint-cell=0.01`, `--max-extent-km=15`,
`--footprint-dilate=1`, `--envelope-tol-km=1`, `--sem-envelope`,
`--vizinho-cep5-tol-km=1`, `--vizinho-cep5-min=3`, `--sem-vizinho-cep5`,
`--sem-exclusao-cluster`, `--quiet`.
SP inteiro leva ~47 s.

## Processo

### Fase 1 — clusterizar ways por nome

Agrupa ways de mesmo `name_norm` por proximidade: célula de **0,02°** (~2,2 km), componentes
conexas por vizinhança 8. Cada componente = uma via física distinta.

**Este passo não é otimização, é correção.** Medido na capital:

| Nome | Ways | Bbox sem clusterizar | Clusters | Bbox do cluster escolhido |
|------|-----:|---------------------:|---------:|--------------------------:|
| `rua augusta` | 33 | **28 × 28 km** | 4 | **2 × 3 km** (28 ways) |
| `rua estoril` | 5 | **32 × 41 km** | 4 | 1 × 1 km (2 ways) |
| `praca da republica` | 23 | 11 × 14 km | 2 | 1 × 1 km (17 ways) |
| `avenida paulista` | 65 | 2 × 2 km | 1 | 2 × 2 km (65 ways) |
| `rua maracuja natal` | 1 | 1 × 1 km | 1 | 1 × 1 km (1 way) |

### Fase 2 — âncoras

Nome que existe em **um só** `loc_nu` do DNE **e** forma **um só** cluster no OSM → aquele cluster
pertence àquele município, sem ambiguidade possível. Medido em SP: **126 096 nomes-âncora**.

### Fase 3 — footprint municipal

As âncoras de cada `loc_nu` viram uma grade de células ocupadas de **0,01°** (~1,1 km), dilatada em
1 célula para cobrir borda. Não é polígono nem bbox: é a pegada real, que lida com município
alongado ou não-convexo.

Cobertura medida em SP: **252 de 252** localidades codificadas, mediana de 320 pontos-âncora.
Distrito (`LOC_IN_TIPO_LOC != 'M'`) herda o footprint do `LOC_NU_SUB` — são 1 470 das 3 138
localidades do Sudeste; ignorar isso perde metade delas.

### Fase 4 — cascata de casamento

Para cada linha do DNE, candidatos = clusters cujo nome bate **e** que caem no footprint do seu
`loc_nu`. A regra de nome desce degrau a degrau, só no que sobrou:

Números abaixo: **SP estado**, re-join 2026-08 (`G:\dne-geo-br-join-sudeste`, OSM em shards,
341 813 linhas DNE, 256 249 `ok` = **75,0 %**). A coluna “% das linhas” é sobre o DNE inteiro;
“% do `ok`” é a fatia entre os casados. (A medição antiga só na capital — 80,5 % exato / 87,8 %
determinístico — está em [melhoria-extracao-coordenadas.md](./melhoria-extracao-coordenadas.md)
§8.5; não é comparável 1:1 com o estado.)

| `geo_regra` | Regra | Linhas SP | % linhas | % do `ok` |
|-------------|-------|----------:|---------:|----------:|
| `exato` | `name_norm` idêntico | 232 722 | 68,1 % | 90,8 % |
| `fonetico` | chave fonética PT-BR (`z→s`, `y→i`, `ph→f`, `h` mudo, dobradas colapsadas) | 7 962 | 2,3 % | 3,1 % |
| `nucleo` | núcleo sem tipo (DNE `Travessa Goiás` ↔ OSM `Rua Goiás`; TLO composto `Estrada Municipal X` → `X` via `TIPO_MOD`) | 6 530 | 1,9 % | 2,5 % |
| `name_alt` | bate em `name_alt_norm` | 4 321 | 1,3 % | 1,7 % |
| `area` | como exato, candidato `kind` ∈ {`square`,`park`}, só para `TLO_TX` de área | 2 459 | 0,7 % | 1,0 % |
| `titulo` | núcleo sem títulos/honrarias (`Doutor`, `Dr`, `Prof`, …) | 1 656 | 0,48 % | 0,65 % |
| `vizinho_cep5` | nome casou, fora da pegada; 1 candidato a ≤1 km de ≥3 vias ok no mesmo CEP-5 (ou bairro) — fase 5e | 317 | 0,09 % | 0,12 % |
| `titulo_fonetico` | `titulo` + chave fonética | 155 | 0,05 % | 0,06 % |
| `addr` | bate em `addr:street` de `OSM_ADDR_POINT` | 127 | 0,04 % | 0,05 % |
| | **Total `ok`** | **256 249** | **75,0 %** | **100 %** |

Ordem da cascata (nome, ainda dentro do footprint):  
`exato` → `area` → `name_alt` → `addr` → `nucleo` → `fonetico` → `titulo` → `titulo_fonetico`  
…e depois das voltas de footprint: **envelope** (mantém a `geo_regra` de nome) e **`vizinho_cep5`**
(grava `geo_regra=vizinho_cep5`).

Leitura dos degraus novos (mesmo re-join):

| Sinal | Linhas finais | Nota |
|-------|--------------:|------|
| `titulo` + `titulo_fonetico` | **1 811** (0,53 % do DNE) | Ex.: DNE `Rua Doutor Laurindo de Gênova` ↔ OSM `rua laurindo de genova` |
| `vizinho_cep5` | **317** finais | **763** recuperados na fase 5e; o resto caiu na exclusão multi-município (5d) |
| envelope | **6 719** | não tem `geo_regra` própria — entram como `exato`/`nucleo`/… |

Fuzzy por distância de edição fica **fora** (atrás de `--fuzzy`): rendia +5,2 % bruto produzindo
`flor de cereja` → `flor de cera` e `mércia` → `meca`.

**Títulos stripados** (lista em `name-keys.js` / `TITULOS`): doutor/dr, professor/prof, engenheiro/eng,
desembargador, senador, deputado, ministro, presidente, padre/frei/monsenhor, patentes (marechal…
tenente), comendador, vereador, prefeito, governador. **Fora:** santo/são (topônimo) e nobreza
(barão, visconde… — fazem parte do nome). Só tokens **no início** do núcleo; “Mario Doutor Silva”
não perde o meio.

**TLO composto** (`TIPO_MOD`): após um tipo real, stripa `municipal` / `estadual` / `federal` /
`vicinal` — DNE `TLO=Estrada Municipal` + `LOG_NO=Professora …` → núcleo `professora …` → bare
`therezinha …` (não deixa `municipal` bloquear o título).

Exemplo título: DNE `Olímpio Carr Ribeiro` + `TLO=Rua` ↔ OSM `Rua Doutor Olímpio Carr Ribeiro` →
`geo_regra=titulo`.

**Auditoria no relatório** (`DNE_GEO_RELATORIO_{UF}.json`):

- `titulo_exemplos` — até 30 matches `titulo` / `titulo_fonetico` com núcleos, tokens removidos e CEP
- `vizinho_cep5_exemplos` — até 30 recuperações CEP-5/bairro (vizinhas, distâncias, `nome_regra`)
- `sem_nome_osm_exemplos` — até 30 residual sem match (com `cep5` e núcleo bare) para amostrar o que falta
- contadores: `envelope_recuperados`, `vizinho_cep5_recuperados` (pré-exclusão 5d)

**Guarda kind-aware:** candidato de área só vale para `TLO_TX` ∈ {Praça, Largo, Parque, Jardim,
Vila, Área}. Sem ela, `Rua Dois` casa com `Praça Dois` — são 2 584 colisões possíveis em SP.

### Fase 5 — desempate entre clusters

Sobrando mais de um cluster dentro do footprint:

1. **Bairro** — cluster mais próximo do centroide do `bai_nu_ini`, calculado das vias do mesmo
   bairro já resolvidas. É o sinal mais forte e roda numa 2ª volta.
2. **Tamanho** — maior soma de `way_node_count`. Nos 5 exemplos acima acerta 5/5, mas é heurística:
   `Rua Estoril` venceu com 2 ways contra três clusters de 1.
3. Empate real → `geo_status=ambiguo`, **sem coordenada**.

**Guarda de extensão (`--max-extent-km`, default 15).** A clusterização é single-link: uma corrente
de células vizinhas liga pontas distantes. Nome genérico espalhado pela cidade encadeia tudo num
cluster só — medido em Guarulhos, `Rua Dois` saiu com 29 ways cobrindo **25 km**, com 19 clusters
concorrentes. Via longa legítima aparece como cluster **único** (`Rodovia Arão Sahm` 17 km,
`Avenida Presidente Kennedy` em Praia Grande 20 km, ambas `osm_clusters=1`). Daí a regra:

> extensão > 15 km **e** mais de um candidato no footprint → `geo_status=ambiguo`.

Custo medido: 42 linhas em SP. Foi o critério de aceite de bbox (§Aceite) que pegou isso na primeira
execução.

### Fase 6 — emitir

Geometria do cluster escolhido: bbox = união dos bboxes das ways; `lat`/`lng` = centroide ponderado
por `way_node_count`. Linha sem cluster sai **vazia**, com `geo_status` dizendo por quê.

Sem fallback de centroide de bairro ou município. Coordenada que não é da via envenena raio de
entrega e ordenação por distância — e não dá para distinguir depois.

## Contrato de saída

`DNE_GEO_LOGRADOURO_{UF}.TXT` — UTF-8, delimitador `@`, sem header, 25 colunas:

```
 1 log_nu           11 log_no_abrev      21 geo_status
 2 ufe_sg           12 loc_no            22 geo_regra
 3 loc_nu           13 bai_no            23 geo_kind
 4 bai_nu_ini       14 mun_nu (IBGE)     24 osm_ways
 5 bai_nu_fim       15 lat               25 osm_clusters
 6 log_no           16 lng
 7 log_complemento  17 lat_min
 8 cep              18 lat_max
 9 tlo_tx           19 lng_min
10 log_sta_tlo      20 lng_max
```

- **1–11**: `LOG_LOGRADOURO` do DNE intacto, só reencodado latin1 → UTF-8.
- **12–14**: desnormalização (nome da localidade, do bairro, IBGE) — evita o consumidor recarregar 3 arquivos.
- **21 `geo_status`**: `ok` · `ambiguo` · `sem_nome_osm` · `sem_extract` (UF sem PBF processado).
- **22 `geo_regra`**: qual degrau da cascata casou — permite reprocessar só o que veio de regra fraca.
- **23 `geo_kind`**: `kind`s do cluster, unidos por `+`. Auditoria: `park` numa Rua é sinal de erro.
- **25 `osm_clusters`**: quantos clusters disputaram. `> 1` marca onde o desempate decidiu.

## Exemplos de saída (dados reais)

Os cinco casos que abrem o [documento de melhoria](./melhoria-extracao-coordenadas.md) — quatro
deles hoje sem coordenada nenhuma:

```
580665@SP@9668@25422@@Augusta@- até 697 - lado ímpar@01305000@Rua@S@R Augusta@São Paulo@Consolação@3550308@-23.557535@-46.6586954@-23.5665717@-23.5500033@-46.6690324@-46.6462695@ok@exato@secondary@28@4
```
Rua Augusta: 4 clusters na capital, escolhido o de 28 ways na Consolação. **Bbox 2 × 3 km** — sem a
fase 1 seriam 28 × 28 km.

```
608719@SP@9668@25243@@Paulista@- até 610 - lado par@01310000@Avenida@S@Av Paul@São Paulo@Bela Vista@3550308@-23.56216@-46.655412@-23.5713665@-23.5545149@-46.6640565@-46.6442094@ok@exato@primary+secondary@65@1
```
Avenida Paulista: cluster único, 65 ways, 2,8 km de extensão real. `osm_clusters=1` = zero risco de desempate.

```
591259@SP@9668@26543@@Estoril@@04773090@Rua@S@R Estoril@São Paulo@Veleiros@3550308@-23.6826577@-46.7077375@-23.6838543@-23.6810728@-46.7095312@-46.7063691@ok@exato@residential@2@4
```
Rua Estoril: venceu com 2 ways contra três clusters de 1 — `osm_clusters=4` sinaliza que o desempate
por tamanho decidiu. Confere com o bairro Veleiros.

```
611789@SP@9668@26472@@da República@- lado ímpar@01045001@Praça@S@Pç da República@São Paulo@República@3550308@-23.5434736@-46.6430364@-23.5449416@-23.5420476@-46.6444382@-46.641106@ok@exato@pedestrian+secondary+square@17@2
```
Praça da República: `geo_kind` inclui `square` — só entrou porque `TLO_TX=Praça` liberou a guarda
kind-aware. O 2º cluster (6 ways, `park`) é outra praça homônima na zona leste.

```
603661@SP@9668@26895@@Maracujá Natal@@04863010@Rua@S@R Maracujá Natal@São Paulo@Vila Natal@3550308@-23.7647147@-46.7073156@-23.7678506@-23.7633246@-46.7116332@-46.7042099@ok@exato@residential@1@1
```
Caso de não-regressão: já funciona hoje no PHP e tem que continuar igual.

Linhas sem casamento — coordenadas **vazias**, nunca chutadas:

```
1023795@SP@9668@26627@@Roberto de Barros@@05360153@Viela@S@Vla Roberto de Barros@São Paulo@Vila Butantã@3550308@@@@@@@sem_nome_osm@@@0@0
585089@SP@9668@25254@@César Ravasco@@04623020@Travessa@S@Tv César Ravasco@São Paulo@Brooklin Paulista@3550308@@@@@@@sem_nome_osm@@@0@0
```

## Resultado

### SP re-join 2026-08 (`G:\dne-geo-br-join-sudeste`, shards)

| | Linhas | % |
|--|-------:|--:|
| DNE | 341 813 | 100 % |
| `ok` | **256 249** | **75,0 %** |
| `ambiguo` | 32 271 | 9,4 % |
| `sem_nome_osm` | 53 293 | 15,6 % |

OSM: 793 906 ways · 305 614 nomes · 431 827 clusters · 40 shards · 1 291 `addr` extras.

Por regra (final, pós-exclusão 5d): `exato` 232 722 · `fonetico` 7 962 · `nucleo` 6 530 ·
`name_alt` 4 321 · `area` 2 459 · `titulo` 1 656 · `vizinho_cep5` 317 · `titulo_fonetico` 155 ·
`addr` 127.

Recuperações espaciais: envelope **6 719** · vizinho_cep5 **763** (317 sobrevivem à 5d).
Exclusão multi-município: **13 270** clusters em disputa → **16 567** linhas revogadas
(`conflito_municipio`).

Comparado ao SP de 2026-07-30 (`G:\dne-geo-local`, 77,1 % `ok`): `sem_nome_osm` caiu
(56 054 → 53 293) com título/TLO composto; o `ok` global ficou um pouco menor porque a
**exclusão multi-município** e o extract/shards atuais movem mais linhas para `ambiguo`
(22 281 → 32 271, em grande parte `conflito_municipio`).

### Sudeste 2026-07-30 (`G:\dne-geo-local`) — outras UFs

| UF | Linhas DNE | `ok` | % | `ambiguo` | `sem_nome_osm` | ways OSM / linha DNE |
|----|-----------:|-----:|--:|----------:|---------------:|---------------------:|
| SP | 341 813 | 263 478 | **77,1 %** | 22 281 | 56 054 | 2,32 |
| ES | 31 993 | 23 972 | **74,9 %** | 735 | 7 286 | 2,40 |
| RJ | 103 723 | 74 588 | **71,9 %** | 8 001 | 21 134 | 1,97 |
| MG | 128 929 | 70 582 | **54,7 %** | 13 812 | 44 535 | 1,48 |

**Capital paulista (medição 2026-07-30): 88,6 %** (47 711 de 53 824) — acima da meta de 85 %.

Duas leituras que seguem válidas:

- **MG em 54,7 % é limite de dado, não do algoritmo.** `sem_nome_osm` responde por 34,5 % das
  linhas, e MG tem a menor densidade de ways por linha DNE do Sudeste (1,48 contra 2,32 de SP): o
  interior mineiro está menos mapeado no OSM.
- **`addr` rendeu ~127 linhas em SP.** Como fonte de nome o dataset de numeração é irrelevante.
  O que ele tem de valioso (`addr:postcode`, nomear way sem `name`) continua sem uso.

Extensão média das vias casadas em SP (medição 2026-07-30): **0,61 km** — coerente com quadra urbana.

## O resíduo `ambiguo`

O rótulo mistura causas diferentes. **SP 2026-08** (`G:\dne-geo-br-join-sudeste`):

| Motivo | Linhas | O que é |
|--------|-------:|---------|
| `conflito_municipio` | **16 567** (51 %) | Cluster reivindicado por 2+ `loc_nu` — exclusão 5d deixou só o dono |
| `fora_do_footprint` | 15 665 (49 %) | Nome casou na UF, nenhum cluster na pegada do município (pós-envelope/CEP-5) |
| `extensao_longa` | 37 | Guarda de 15 km (encadeamento do single-link) |
| `empate_de_tamanho` | 2 | Dois clusters com o mesmo peso |

> Na medição 2026-07-30 (antes da exclusão 5d dominar o relatório), `fora_do_footprint` era
> 99,8 % do `ambiguo`. Hoje metade do resíduo é **decisão deliberada** de não partilhar via
> entre municípios.

Distância do melhor candidato à mancha de âncoras (ainda útil no subconjunto
`fora_do_footprint`; contagens 2026-08 incluem o que a 5d depois revogou durante o pipeline):

| Distância | Linhas | Leitura |
|-----------|-------:|---------|
| até 1 km | **21 072** | Borda / buraco de pegada — envelope e CEP-5 atacam daqui |
| 1–5 km | 79 | idem |
| 5–20 km | 373 | duvidoso |
| > 20 km | 1 143 | homônimo de outro município — **rejeição correta** |

**Amostra de 20 linhas por motivo, com a leitura de cada padrão:
[amostras-ambiguo-sp.md](./amostras-ambiguo-sp.md).**

### Duas tentativas medidas e descartadas

**Dilatar o footprint** (`--footprint-dilate`):

| Dilatação | `ok` | Clusters usados por 2+ municípios |
|-----------|-----:|----------------------------------:|
| 1 (default) | 263 478 (77,1 %) | 8 691 (4,07 %) |
| 2 | 266 957 (78,1 %) | 12 143 (5,74 %) |
| 3 | 269 154 (78,7 %) | 14 575 (6,98 %) |

Ganho e risco andam **1:1** — de 1 para 2, +3 479 linhas resolvidas e +3 452 clusters passando a ser
reivindicados por mais de um município. Halo uniforme não distingue "periferia da própria cidade" de
"cidade vizinha", que em região metropolitana são a mesma direção. Fica em 1.

**Âncora local por bairro/CEP** — implementada, medida e **removida**: rendia 36 linhas em 341 813,
porque é redundante com o crescimento do footprint na 2ª volta. O histórico completo, incluindo o
defeito de raio que quase mascarou isso, está em
[amostras-ambiguo-sp.md](./amostras-ambiguo-sp.md#como-esta-amostra-derrubou-uma-feature).

### Fase 5c — envelope (buraco na pegada)

Depois da 2ª volta, linhas `fora_do_footprint` com **exatamente um** candidato de nome a
≤ `--envelope-tol-km` (default 1) da mancha (centro − raio das âncoras) são aceitas. Isso cobre
loteamento na borda e buracos de grade **sem** dilatar o footprint (halo metropolitano mistura
cidades vizinhas — medido e descartado).

Desligar: `--sem-envelope`.

### Fase 5d — exclusão multi-município

Cluster usado por 2+ `loc_nu` com `geo_status=ok` → um dono (maioria de linhas; empate = âncora
municipal mais próxima do centroide). Perdedores → `ambiguo` / `conflito_municipio`, sem coordenada.
Uma via física é de uma cidade só; falsos `ok` envenenam o produto mais que vazios.

Desligar: `--sem-exclusao-cluster`. Relatório: `clusters_multi_municipio`, `revogados_conflito_municipio`.

### Fase 5e — vizinhança CEP-5 (ou bairro)

Depois do envelope, linhas ainda `fora_do_footprint` com candidatos de **nome** são testadas
contra vias **já `ok`** no mesmo município:

1. Âncoras = linhas ok com o mesmo **CEP-5** (`digits(cep).slice(0,5)`); se houver menos que
   `--vizinho-cep5-min` (default **3**), cai para o mesmo **`bai_nu_ini`**.
2. Para cada candidato de nome, distância ao **vizinho mais próximo** (não raio de centroide —
   isso foi o defeito da âncora local removida).
3. Filtra candidatos a ≤ `--vizinho-cep5-tol-km` (default **1** km).
4. Aceita **somente** se sobrar **exatamente 1** e a extensão do cluster for ok.

`geo_regra=vizinho_cep5`. O relatório grava `vizinho_cep5_recuperados` e
`vizinho_cep5_exemplos` (CEP-5, fonte, nº de vizinhas, distâncias, top-3 vizinhas, `nome_regra`
que gerou os candidatos).

Desligar: `--sem-vizinho-cep5`.

Diferença da âncora local antiga: índice fixo (sem encadear recovery fraca), vizinho-mais-próximo,
mínimo de âncoras, e só no resíduo pós-envelope.

### O que as amostras revelaram

Ler 20 linhas de cada motivo ([amostras-ambiguo-sp.md](./amostras-ambiguo-sp.md)) mostrou coisas que
as contagens agregadas escondiam.

**1. `fora_do_footprint` tem dois padrões dominantes, os dois de cobertura de âncora:**

- **Distrito longe da sede.** `Caucaia do Alto` aparece 4 vezes nas 20 — é distrito de Cotia, a
  ~20 km da área urbana principal. As âncoras de Cotia concentram-se na sede e o distrito fica
  descoberto. Mesmo caso de `Moinho Velho` em Embu das Artes. Note que aqui o DNE **não** separa por
  `loc_nu`: o distrito está no nome do bairro, então a herança de footprint por `LOC_NU_SUB` (§Fase 3)
  não alcança.
- **Loteamento novo.** `Residencial Campo Florido`, `Chácaras Morumbi`, `Núcleo Residencial São Judas
  Tadeu` — bairros recentes, quase sempre com `candidatos=1`, na borda da malha mapeada.

Em quase todas, `km da mancha = 0`: o candidato está dentro do raio de espalhamento do município, só
não numa célula ocupada.

**2. A guarda de extensão está rejeitando via longa legítima.**

As 37 linhas de `extensao_longa` são de dois tipos, e na amostra de 20 a divisão é meio a meio:

| Tipo | Exemplo | Cand. | Veredito |
|------|---------|------:|----------|
| Nome genérico espalhado | `Rua Dois` (Guarulhos), 25 km | 31 | Rejeição **certa** — não há como saber qual das 31 |
| Via que **realmente** é longa | `Avenida Presidente Castelo Branco` (Praia Grande), 20 km | 2 | Rejeição **provavelmente errada** |

`Presidente Castelo Branco` é a orla de Praia Grande e tem mesmo ~20 km; `Rodovia Raposo Tavares`
(Ourinhos) e `Rodovia Padre Manoel da Nóbrega` idem. A regra atual (`> 15 km` **e** `> 1 candidato`)
não distingue "31 homônimas espalhadas" de "2 candidatos, um deles a via real".

Ajuste a considerar: exigir `> 2` candidatos, ou isentar `kind` de rodovia (`motorway`, `trunk`,
`primary`). **Não medido** — são 37 linhas em 341 813, então a prioridade é baixa; fica registrado
para não se redescobrir.

**3. `empate_de_tamanho` não é problema de desempate.** Das 3 linhas, `Rua Particular` (Birigui) é um
marcador genérico que o DNE usa quando não há nome — não existe resposta certa a encontrar.

## Serviço externo para o que falta? (pesquisado e testado em 2026-07-30)

Sobram em SP **56 054 `sem_nome_osm` + 18 218 `ambiguo` = 74 272 linhas**. Vale buscar coordenada
fora do OSM? Testado contra os **nossos** CEPs faltantes, não contra a documentação.

### O que **não** serve

| Serviço | Por quê |
|---------|---------|
| **Nominatim / Photon / Overpass** | São o **próprio OSM**. Nossa lacuna é rua que o OSM não nomeia — eles falham exatamente nas mesmas. Além disso a [política de uso do Nominatim](https://operations.osmfoundation.org/policies/nominatim/) **proíbe geocodificação em massa** na instância pública: 1 req/s, máximo 1 máquina, e scripts que rodam mais de um dia ou em intervalos regulares ficam limitados a 4 req/min. Volume nosso exigiria instância própria. |
| **[BrasilAPI](https://brasilapi.com.br/docs) `/cep/v2`** | Testado em 5 CEPs de SP, incluindo Avenida Paulista: **`location.coordinates` veio vazio em todos**. Devolve o endereço (que já temos, é a mesma base DNE) e nenhuma geometria. Inútil hoje. |
| **[banco-ceps](https://github.com/gpfconfea/banco-ceps)** (dataset offline, MIT) | Atraente por ser download único, mas as coordenadas dele vêm de Nominatim + AwesomeAPI + *scraping* — a própria documentação diz que o scraping é "variável e inconsistente". Herda os erros das duas fontes e adiciona os seus. |
| **Camadas comerciais** (Google, HERE, Mapbox, LocationIQ, OpenCage, Geoapify) | Free tiers de 2,5 k–250 k req/mês, mas quase todos **proíbem armazenar** o resultado fora do mapa deles. Como o produto aqui é uma base persistida, o licenciamento mata antes do preço. |

### O que serve, com ressalva: AwesomeAPI CEP

[`cep.awesomeapi.com.br`](https://docs.awesomeapi.com.br/api-cep) devolve `lat`/`lng` por CEP, de
base própria (Correios + IBGE), não OSM. Duas medições nossas:

**Aferição contra o que já validamos** — 40 logradouros com `geo_status=ok`, `regra=exato`,
cluster único e ≥3 ways:

| Métrica | Valor |
|---------|------:|
| Responderam com coordenada | **40/40** |
| Erro mediano vs. nossa coordenada | **0,16 km** |
| p75 / p90 | 0,23 km / 1,38 km |
| Dentro de 500 m | 34 (85 %) |
| Acima de 2 km | 3 |
| **Pior caso** | **319 km** |

Mediana de 160 m entre duas fontes independentes é corroboração forte. Mas a cauda é grave e o pior
caso é **erro deles, comprovado**: CEP `14165352` é `Rua Joaquim Ferreira, Sertãozinho`; nossa
coordenada cai em Sertãozinho (−21,11, −47,98), a deles cai na zona sul da capital
(−23,75, −46,73) — contradizendo o próprio campo `city` que a resposta traz.

**Teste no que falta** — 40 linhas sem coordenada da capital:

| | |
|--|--:|
| Responderam com coordenada | **40/40** |
| Caem dentro do município certo | **37 (93 %)** |
| Caem fora | 3 |

Os 3 erros são do mesmo tipo, e o padrão é revelador: `Praça Cidade de Itu` recebeu coordenada **na
cidade de Itu**, `Rua Ezequiel Wanderley` caiu a 100 km. Para CEP que eles não têm de verdade, a
resposta parece vir de uma geocodificação ingênua pelo nome.

### Conclusão: usar como fonte, nunca como verdade

O erro deles é **detectável com o que já construímos**. A pegada municipal (e o envelope da mancha)
rejeitam exatamente esse tipo de resposta. O desenho seria:

1. Consultar só as linhas `sem_nome_osm` / `ambiguo`, que já têm CEP.
2. **Validar contra o footprint do município** (e contra a vizinhança do bairro, mais apertado).
3. Aceitar com `geo_regra=cep_externo`, **bbox vazia** — é ponto, não traçado.
4. Rejeitado → continua vazio.

Rendimento esperado pela amostra: ~93 % das faltantes passam a validação municipal, ou seja da ordem
de **~5 600 das 6 008 faltantes da capital**. Precisão é de CEP, não de via.

**Dois bloqueios antes de implementar**, ambos fora do meu alcance:

- **Volume:** o plano gratuito é de [10 mil requisições/mês com chave](https://awesomeapi.com.br/).
  O Sudeste precisa de ~164 mil (SP 74 k + MG 55 k + RJ 27 k + ES 8 k) — **~16 meses** de free tier.
  Na prática, é plano pago ou nada.
- **Licenciamento:** a [página inicial](https://awesomeapi.com.br/) diz claramente "para qualquer uso". Não precisamos nos preocupar com restrições.

Não implementei nada disso — a decisão é comercial, não técnica.

### Existe um limite de solicitações à API?

O uso é gratuito para até 10 mil requisições mensais. Não há limite de requisições por minuto, mas para evitar abusos e garantir a qualidade do serviço, requisições incrementais ou erros 404 frequentes podem resultar em um bloqueio temporário de 1 hora. Recomendamos implementar lógicas adequadas de tratamento de erros e evitar solicitações desnecessárias para prevenir interrupções no acesso à API.

### `DNE_GEO_RELATORIO_{UF}.json`

Campos reais gerados pelo run (números de exemplo = SP 2026-07-30, antes de envelope/exclusão
re-rodados; reprocessar para atualizar):

```json
{
  "uf": "SP",
  "linhas_dne": 341813,
  "localidades": { "total": 3138, "com_footprint": 252, "herdados_de_subordinacao": 1470, "ancoras": 126096 },
  "geo_status": { "ok": 263478, "ambiguo": 22281, "sem_nome_osm": 56054 },
  "geo_regra": { "exato": 242178, "fonetico": 7864, "nucleo": 6481, "name_alt": 4353, "area": 2469, "addr": 133 },
  "ambiguo_por_motivo": { "fora_do_footprint": 22241, "extensao_longa": 37, "empate_de_tamanho": 3 },
  "envelope_recuperados": 0,
  "clusters_multi_municipio": 0,
  "revogados_conflito_municipio": 0
}
```

`envelope_*` e `clusters_multi_*` passam a ser preenchidos após reprocessar com as fases 5c/5d.

`DNE_GEO_BAIRRO_{UF}.TXT` sai de graça no mesmo passe: bbox por `bai_nu` pela união das vias
resolvidas. Resolve o lado bairro sem depender do `OSM_BAIRRO.TXT` (que tem 2 696 linhas em `XX`).

## Aceite — situação em 2026-07-30

| Critério | Situação |
|----------|----------|
| Os 5 casos de regressão saem idênticos aos exemplos acima | ✅ conferido linha a linha na saída real |
| ≥ 85 % de `geo_status=ok` na capital | ✅ **88,6 %** |
| Zero linha com coordenada de fallback | ✅ o código não tem caminho de fallback |
| Nenhuma via > 15 km sem `kind` de rodovia | ✅ sobraram só `osm_clusters=1` legítimas (Rodovia Arão Sahm 17 km, Av. Presidente Kennedy 20 km) |
| Nenhuma área casada com `TLO_TX` de via | ✅ 0 ocorrências em 341 813 linhas |

Testes automatizados (`npm run test:join`): 15 unitários em
[`test/name-keys.test.js`](../../test/name-keys.test.js) e
[`test/geo-cluster.test.js`](../../test/geo-cluster.test.js) — núcleo sem tipo, chave fonética com
os pares reais (`Luiz`/`Luis`, `Xavier`/`Chavier`), cluster separando homônimos a 50 km, centroide
ponderado, dilatação do footprint. Mais 3 de integração em
[`test/dne-geo-join.test.js`](../../test/dne-geo-join.test.js), com fixture DNE em latin1: cada
degrau da cascata marcando sua regra, guarda kind-aware barrando parque para `TLO_TX=Rua`, footprint
escolhendo o homônimo de perto contra um cluster mais pesado a 150 km, distrito herdando a pegada do
pai, e linha sem match saindo com todas as colunas de geometria vazias.

**Armadilha registrada:** com coordenada negativa, `-23.5505` cai na célula *seguinte* de `-23.55`
(`floor(-2355.05) = -2356`). O primeiro teste de footprint falhou por isso — era o teste errado, não
o código.

## Limites conhecidos

| Limite | Consequência |
|--------|--------------|
| ~12 % das linhas sem nome correspondente no OSM | Ficam vazias — é o teto do dado, não do algoritmo |
| Geometria por via inteira, não por seccionamento | Todos os CEPs de uma rua compartilham bbox; recorte por faixa exigiria `addr:housenumber` |
| Desempate por tamanho é heurística | `osm_clusters > 1` marca onde ela decidiu; auditar por amostragem |
| Via de divisa entre municípios | Pode cair no vizinho; footprint dilatado piora isso de propósito, para não perder a via |
| SP inteiro em memória (~400 MB est.) | `--max-old-space-size`; se estourar, shard por letra inicial do `name_norm` |
| Só o Sudeste tem extract | Demais UFs saem com `geo_status=sem_extract` |

## Depois

O `DneOsmGeoEnricher` **já carrega** por `log_nu` / `bai_nu` quando `DNE_GEO_*` está na `--dir`
(ddsoft `osm:dne:enrich-geo`). Próximo: dry-run → apply por UF no MySQL e coords na busca.
Ver [bairro-logradouro.md](./bairro-logradouro.md) e [operacao-comandos.md](./operacao-comandos.md).
