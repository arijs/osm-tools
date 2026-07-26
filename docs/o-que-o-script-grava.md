# O que o script grava e o que cada valor significa

Documento de referência para os arquivos e a saída de **`index0.js`** no estado atual (fase de inventário XML).  
Não descreve um índice de geocoding — só o **mapa estrutural** e o **estado de processamento**.

Para o *porquê*, ver [objetivo-e-contexto.md](./objetivo-e-contexto.md).  
Para exemplos concretos de JSON/XML, ver [amostra-estrutura-dados.md](./amostra-estrutura-dados.md).

---

## Visão geral dos artefatos

| Artefato | Quando | Conteúdo principal |
|----------|--------|-------------------|
| `…-stats.json` | sempre (se `saveStats`) | estado para **retomar** + inventário XML agregado |
| `…-results.json` | sempre ao terminar/parar | resumo da execução (contagens, tempo, paths) |
| `…-stats-bzip.json` | só com `--save-bz-stats` | lista de membros/blocos bzip2 |
| `…-results-bzip.json` | só com `--save-bz-stats` | mesma lista limpa para o snapshot de results |
| stdout (progresso) | durante o run | uma linha `\r` com % / velocidade / árvore XML resumida |
| stderr | durante o run | legenda, avisos de stack XML, mensagens de save |

**Importante:** as chaves `bzip` / `bzStats` **não** são gravadas dentro de `stats.json` nem `results.json`. Sem `--save-bz-stats`, o detalhe por bloco bzip **é descartado** no disco (pode existir só em memória no objeto retornado por `runProcess`).

Nomes padrão (a partir do path do `.bz2`):

```
arquivo.osm.bz2
  → arquivo.osm-stats.json
  → arquivo.osm-results.json
  → arquivo.osm-stats-bzip.json     (opcional)
  → arquivo.osm-results-bzip.json   (opcional)
```

---

## 1. Arquivo `*-stats.json` (resume + inventário)

Objetivo: poder **continuar** de onde parou e, ao mesmo tempo, acumular o inventário XML.

### 1.1 Chaves de primeiro nível

| Chave | Tipo | Significado |
|-------|------|-------------|
| `xml` | objeto | inventário XML + pilha aberta no momento do save |
| `fopt` | objeto | opções/posição do último `SeekBzip.readBlock` (de onde retomar o bzip) |
| `runs` | array | histórico de trechos de execução (cada run / resume) |
| `current` | objeto | snapshot de progresso e estado do membro bzip atual |
| `bzipPath` | string? | só se `--save-bz-stats`: path do sidecar de blocos |

Não existe `bzip` embutido no formato atual.

### 1.2 `xml` — inventário e estado do parser de tags

| Campo | Significado |
|-------|-------------|
| `xml.tags` | Pilha de nomes de elementos **abertos** no momento do save (ex.: `["osm"]` ou `["osm","node"]`). Usado no resume para reconstruir o caminho na árvore de stats. |
| `xml.index` | Índices irmãos correspondentes a cada nível da pilha (posição entre irmãos). |
| `xml.current` | Contador de “próximo irmão” no nível atual (quantos elementos já fecharam sob o pai atual). |
| `xml.root` | **Árvore de inventário** (ver §1.3). É o coração do “entendimento da estrutura”. |
| `xml.first` | Amostra dos primeiros ~10 eventos open/close (debug). |
| `xml.last` | Amostra dos últimos ~10 eventos open/close (debug / ver o que estava passando no soft-stop). |
| `xml.remain` | Resto de texto no buffer do `XMLParser` ao dar `end()` (soft-stop/fim). No resume é reenviado ao parser. Pode ser fragmento curto (ex.: `"50."`) se o corte foi no meio de um valor. |

### 1.3 `xml.root` — árvore de tags / atributos / `tag_k_map`

Cada nó da árvore (incluindo a raiz sintética e cada elemento contado) tem aproximadamente:

| Campo | Significado |
|-------|-------------|
| `count` | Quantas vezes este elemento (neste caminho de pais) foi **aberto**. |
| `tags` | Mapa `nomeDoElemento → nó filho` (ex.: sob `osm`, chaves `node`, `way`, `changeset`). |
| `attrs` | Mapa `nomeDoAtributo → estatística do atributo` (ver abaixo). |
| `tag_k_map` | Só relevante quando o elemento é `<tag k="…" v="…"/>`: agrupa por valor de `k` (chaves OSM semânticas), em vez de misturar todos os `<tag>` num saco só. |
| `firstOpen` / `lastOpen` | Snapshot de posição (bytes/chunks/bzip) na primeira e última abertura. |
| `firstClose` / `lastClose` | Idem no fechamento. |
| `firstIndex` / `lastIndex` | Índices de ordem entre irmãos. |
| `parent` | **Não** é serializado no JSON limpo (`cleanXMLTree`); existe só em memória. |

#### Estatística de atributo (`attrs[nome]`)

| Campo | Significado |
|-------|-------------|
| `count` | Quantas vezes o atributo apareceu neste elemento. |
| `first` / `last` | Índice (ordem) da primeira/última ocorrência. |
| `firstVal` / `lastVal` | **Amostra** do valor (string). Útil para ver formato de `lat`, `lon`, `id`, timestamps, etc. **Não** é a lista completa de valores. |

#### `tag_k_map` (chaves OSM)

Para cada `<tag k="addr:street" v="…"/>` (com atributo `k`), o script cria/atualiza:

```
…pai.tag_k_map["addr:street"] = { count, attrs, tags, … }
```

Assim o inventário distingue “quantas tags OSM de rua” de “quantas tags OSM de nome”, em vez de só dizer “houve N elementos `<tag>`”.

> **Limitação atual:** o valor `v` entra na estatística de atributo `v` (first/last), mas **não** há lista de todos os valores nem índice texto→coordenada.

### 1.4 `fopt` — cursor no stream bzip2

Espelho “limpo” do retorno de `SeekBzip.readBlock` (CRCs em hex string quando aplicável):

| Campo | Significado |
|-------|-------------|
| `fileCount` | Quantos membros bzip2 concatenados já terminaram. |
| `fileOffset` | Byte no `.bz2` onde começa o membro atual (ou o próximo, após fim de membro). |
| `byteOffset` / `byteOffsetStart` | Deslocamento dentro do membro (bloco). |
| `bitOffset` / `bitOffsetEnd` | Offset em bits (bzip2 não é alinhado só a bytes). |
| `blockCount` | Blocos já lidos no membro atual. |
| `bytesInput` / `bytesOutput` | Tamanhos do último bloco (comprimido / descomprimido). |
| `bytesInputPos` / `bytesOutputPos` | Acumulados no stream. |
| `blockCRC` / `streamPartialCRC` / `streamCRC` | Integridade bzip2; `streamCRC` preenchido no fim do membro. |
| `bzLevelBlock` / `bzLevelFile` | Nível de compressão detectado. |

No **resume**, o script usa isso para continuar `readBlock` sem recomeçar do byte 0.

### 1.5 `current` — progresso e membro em andamento

| Campo | Significado |
|-------|-------------|
| `percent` | `chunkPos[0] / tamanhoDoArquivo` (fração do `.bz2` lida). |
| `time` / `timeStr` | Tempo total acumulado (s / formatado). |
| `chunkCount[0,1,2]` | Nº de chunks: input bzip medido, saída descomprimida, linhas após `LineSplitter`. |
| `chunkPos[0,1,2]` | Bytes nos mesmos três estágios. |
| `bzFile` / `bzBlock` | Índices de membro / bloco na UI de progresso. |
| `bzNextFileOffset` | Offset do próximo (ou atual) membro no arquivo. |
| `bzCurrentFile` | Estrutura do membro em progresso (`blocks[]`, crcs, input/output). |

### 1.6 `runs[]` — cada trecho de execução

Cada elemento registra um período entre start e save (inclui resumes):

| Campo | Significado |
|-------|-------------|
| `start` | ISO timestamp do início daquele trecho. |
| `time` / `timeStr` | Duração do trecho. |
| `percent` | Progresso ao final do trecho. |
| `speed` / `speedStr` | Vazão de leitura do `.bz2` naquele trecho. |
| `chunkCount` / `chunkPos` | Deltas daquele trecho. |
| `endingCount` / `endingPos` | Totais ao final. |
| `bzFile` / `bzBlock` | Onde parou. |

---

## 2. Arquivo `*-results.json` (resumo da execução)

Gerado ao finalizar (fim do arquivo, soft-stop ou erro tratado após save de stats).  
É um **snapshot amigável** para inspeção humana/API — não é o formato primário de resume (o resume usa `*-stats.json`).

Campos típicos (ver também o retorno de `runProcess`):

| Campo | Significado |
|-------|-------------|
| `error` | Erro final, se houver. |
| `stoppedEarly` | `true` se parou por soft-stop/checkpoint sem EOF. |
| `stopReason` | Motivo textual (`bzip-member-end`, `xml-near-root`, `soft-stop-timeout`, …). |
| `inputPath` / `statsPath` / `resultsPath` | Paths usados. |
| `bzStatsPath` | Só com `--save-bz-stats`: path do results-bzip. |
| `fileSize` | Tamanho do `.bz2` em bytes. |
| `chunkCount` / `chunkPos` | Totais dos três estágios. |
| `bzIndexFile` / `bzIndexBlock` | Progresso bzip no fim. |
| `xmlStackMismatches` | Quantas vezes a pilha open/close precisou de recuperação (avisos). |
| `xml.tags` | Pilha aberta no fim (cópia). |
| `xml.openCounts` | Contagens soltas no primeiro nível de inventário (parcial; ver nota). |
| `xml.nodeCount` / `xml.wayCount` | Contagens sob `root.tags.osm.tags.node|way` se existirem. |
| `xml.root` | Mesma árvore de inventário que em stats (limpa). |
| `elapsedMs` | Duração daquele processo Node. |

**Nota sobre `openCounts`:** contagens em `openCounts.node` olham `root.tags.node` (filho direto da raiz sintética). No OSM real, `node` costuma estar sob `osm`, então o campo útil costuma ser `xml.nodeCount` / a árvore `xml.root.tags.osm.tags…`.

**Não contém** `bzStats` no disco. Em memória, `runProcess` ainda pode anexar `result.bzStats` para testes/API.

---

## 3. Sidecars `*-bzip.json` (opcional)

Só com `--save-bz-stats` (ou `OSM_SAVE_BZ_STATS=1`).

Estrutura: **array de membros** bzip2 concatenados; cada membro:

| Campo | Significado |
|-------|-------------|
| `blocks[]` | Blocos internos do membro |
| `blocks[].offsetStart` / `offsetEnd` / `offsetBit` | Posição do bloco no membro |
| `blocks[].input` / `output` | Bytes comprimidos / descomprimidos do bloco |
| `blocks[].crc` / `crcStream` | CRCs |
| `crc` | CRC do stream do membro (quando fechado) |
| `offsetStart` / `offsetEnd` / `offsetBitEnd` | Extensão do membro no arquivo |
| `input` / `output` | Totais do membro |
| `level` | Nível de compressão |

Com `--save-bz-stats-per-member`, além do array agregado, grava `…-stats-bzip/0000.json`, `0001.json`, …

Utilidade prática: **seek** futuro (“pular para o bloco X”), validação de integridade, entender fragmentação do dump — não é dado de endereço.

---

## 4. Linha de progresso (stdout)

Impressa com `\r` (atualiza na mesma linha). Antes, no stderr, há **legenda** e **header**.

| # | Campo | Significado |
|---|--------|-------------|
| 1 | `percent` | % do `.bz2` já lida |
| 2 | `speed` | Taxa de leitura comprimida nesta execução |
| 3 | `elapsed` | Tempo desta execução |
| 4 | `eta` | Estimativa de restante nesta velocidade |
| 5 | `counts` | `chunkCount` formatado (in, out, lines) |
| 6 | `sizes` | `chunkPos` formatado (in, out, lines) |
| 7 | `bzF.B` | `bz{membro}.{bloco}` |
| 8 | `xmlTree` | Resumo compacto da árvore (`osm 1(changeset 364k(…))`) |
| 9 | `geoSignals` | Sinais de geocoding (ver §4.1) |

O `xmlTree` é a forma mais rápida de ver **a estrutura XML**.  
O `geoSignals` responde rápido: **já apareceu material útil para geocoding?**

### 4.1 Campo `geoSignals` (progresso, stats e results)

Contadores incrementais (não listam cada endereço):

| Campo | Significado |
|-------|-------------|
| `node` / `way` / `relation` | Elementos abertos |
| `nodeWithLat` / `nodeWithLon` / `nodeWithLatLon` | Nodes com esses atributos |
| `addrAny` | Total de `<tag k>` com `k` começando em `addr:` |
| `addrStreet` / `addrHousenumber` / `addrCity` / `addrPostcode` / `addrCountry` | Subconjunto comum de `addr:*` |
| `addrByKey` | Mapa completo `addr:…` → contagem |
| `name` / `nameLocalized` | `k=name` e `k=name:xx` |
| `place` / `highway` / `building` | Tags de lugar / via / edifício |
| `hints.hasPointGeometry` | `nodeWithLatLon > 0` |
| `hints.hasAddressTags` | `addrAny > 0` |
| `hints.hasPlaceTags` / `hasNamedFeatures` / `hasRoadNetwork` | flags derivadas |
| `hints.likelyHasGeocodeMaterial` | pontos com coordenada **e** (addr OU place OU name) |

Na linha de progresso (compacto):

```text
geo n=0                    ← só structure / changesets
GEO n=1m2(ll=1m2) addr=12k st=8k hn=7k name=900k place=5k
```

- Prefixo **`GEO`** (maiúsculo) = `likelyHasGeocodeMaterial === true`  
- Prefixo **`geo`** = ainda sem material típico de geocoding  

Gravado em:

- `*-results.json` → `geocodeSignals` (e `xml.geocodeSignals`)
- `*-stats.json` → `geocodeSignals` / `xml.geocodeSignals` (restaurado no resume)

### 4.2 `coordLayout` — mapa lat/lon × posição no stream

Plano detalhado: [docs/plans/coord-layout.md](./plans/coord-layout.md).

Não grava todas as coordenadas. Mantém:

| Parte | Conteúdo |
|-------|----------|
| `sequence` | `meanJumpDeg`, `pctSmallJumps`, `maxJumpDeg`, `jumpCount` — saltos em **graus** (`hypot(Δlat,Δlon)`), não km |
| `blocks[]` | por bloco bzip com nodes: bbox, first/last, contagens, jumps do bloco, offsets |
| `samples[]` | reservoir (~512) de pontos com `kind`, `bzFile`/`bzBlock`, `chunkPos`, `nodeOrdinal` |
| `addrSamples[]` | reservoir de pontos com addr/name/place |
| `addressMappingEnabled` / `…At` | quando o primeiro sinal de endereço “ligou” o densify |

Progresso: `lay~seq seq=0.9 meanJump=0.002° blocks=12 samples=200 +addr`

| Interpretação | |
|---------------|--|
| `pctSmallJumps` alto + mean baixo | ordem do XML ≈ percurso local no mapa |
| mean alto, few small jumps | coordenadas “saltam” no arquivo (não sequencial) |

---

## 5. O que *não* está sendo gravado (e por quê importa para geocoding)

| Dado | Situação atual |
|------|----------------|
| Cada `lat`/`lon` de cada node | Só first/last amostra em `attrs.lat` / `attrs.lon` se `node` existir no inventário |
| Todos os valores de `addr:street` | Só `count` + first/last em `tag_k_map` se essa chave aparecer |
| Geometria de ways | Não extrai listas de `<nd ref>` |
| Índice espacial ou full-text | Não existe |
| Texto livre entre tags | Eventos de texto do XML não alimentam inventário (só estrutura de elementos/attrs) |

Enquanto o inventário não mostrar volume relevante de `node`/`way` com `lat`/`lon` e tags `addr:*` ou `name`, **ainda não há evidência no processamento de que este dump (ou esta parte dele) já entregou a matéria-prima do geocoder**.

---

## 6. Fluxo resumido (código ↔ disco)

```
.osm.bz2
  │
  ├─ SeekBzip.readBlock ──► fopt, current.bz*, opcional *-bzip.json
  │
  └─ bytes ──► decoder ──► lines ──► XMLParser
                                      │
                                      ├─ open/close ──► xml.root (tags, attrs, tag_k_map)
                                      ├─ pilha ──► xml.tags / index / current
                                      └─ end ──► xml.remain
                                                      │
                                                      ▼
                                            *-stats.json  (resume + inventário)
                                            *-results.json (resumo)
```

---

## Ver também

- [objetivo-e-contexto.md](./objetivo-e-contexto.md)
- [amostra-estrutura-dados.md](./amostra-estrutura-dados.md)
- Código: `index0.js` (`onXmlOpenTag`, `cleanXMLTree`, `saveProcessStats`, `buildResult`)
