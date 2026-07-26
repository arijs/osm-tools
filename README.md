# osm-tools

Ferramentas em Node.js para **explorar dumps OpenStreetMap** (`.osm.bz2`) em streaming — sem carregar o planeta inteiro na memória.

## Para que serve (resultado prático)

### Meta de longo prazo: geocoding

O projeto nasceu da ideia de usar um arquivo OSM grande (ex.: `planet-latest.osm.bz2`) para:

1. **Geocoding** — texto de endereço → latitude/longitude  
2. **Geocoding reverso** — lat/lon → endereço / nome do lugar  

Isso **ainda não está implementado**. Antes de indexar endereços, é preciso **entender o que o dump contém**, em que ordem, e com quais tags/atributos.

### O que o script faz *hoje* (fase 0 — inventário)

O processador principal, **`index0.js`**, percorre o `.osm.bz2` e **mapeia a estrutura do XML**:

- quais elementos aparecem (`node`, `way`, `relation`, `changeset`, …) e com que frequência;
- quais **atributos** cada um tem (`lat`, `lon`, `id`, …) com amostra first/last;
- quais chaves de tag OSM (`k` em `<tag k="addr:street" v="…"/>`) aparecem (`tag_k_map`);
- progresso no arquivo (%, velocidade) e estado para **retomar** após Ctrl+C.

**Resultado prático atual:** um inventário em JSON (`*-stats.json` / `*-results.json`) + linha de progresso — um “raio-X” do dump para decidir *como* extrair dados de endereço depois.

**Não é:** um geocoder, nem uma exportação de todos os pontos/endereços.

Documentação detalhada:

| Documento | Conteúdo |
|-----------|----------|
| [**docs/objetivo-e-contexto.md**](docs/objetivo-e-contexto.md) | Meta de geocoding, o que o script entrega agora, o que falta, roadmap |
| [**docs/geo/README.md**](docs/geo/README.md) | **Pipeline geo:** estado atual, extract, match, bairro/logradouro, comandos |
| [**docs/o-que-o-script-grava.md**](docs/o-que-o-script-grava.md) | Cada campo de stats/results/progresso e sidecars bzip |
| [**docs/amostra-estrutura-dados.md**](docs/amostra-estrutura-dados.md) | Exemplos XML + amostras reais/parciais do inventário |
| [**docs/plans/coord-layout.md**](docs/plans/coord-layout.md) | Plano: mapa lat/lon × posição no stream |
| [**docs/plans/pbf-inventory.md**](docs/plans/pbf-inventory.md) | Plano: inventário PBF (Sudeste / geocoding) |
| [**docs/plans/osm-para-locais-geo.md**](docs/plans/osm-para-locais-geo.md) | Plano histórico: extract TXT `@` → match `locais` |

### Exemplo de descoberta útil

Ao processar um trecho de `planet-latest.osm.bz2`, o inventário pode mostrar só `changeset` (metadados de edições) e ainda **zero** `node`/`way` com `lat`/`lon` e `addr:*`.  
Isso evita gastar semanas “geocodando” numa parte do arquivo que ainda não é mapa de endereços.

---

## Fluxo técnico

```
.osm.bz2
   │
   ▼
seek-bzip (blocos)  →  UTF-8  →  linhas  →  XMLParser
                                              │
                                              ▼
                                    inventário de tags/attrs
                                    + stats de progresso/resume
```

- Processador suportado: **`index0.js`** (`runProcess` + CLI)  
- **`index.js`**: rascunho incompleto — não usar ([detalhes](#sobre-indexjs))

---

## Início rápido

### Requisitos

- Node.js ≥ 18  
- Python 3 (só para gerar fixtures de teste)  
- Pacote local `@arijs/seek-bzip` em `../seek-bzip`

```bash
cd osm-tools
npm install
```

### Processar um dump XML (`.osm.bz2`)

```bash
node index0.js caminho/arquivo.osm.bz2

# exemplos:
node index0.js H:\osm\planet-latest.osm.bz2
node index0.js test\fixtures\tiny.osm.bz2
```

### Processar um dump PBF (recomendado para extract regional)

Script **independente** `index-pbf.js` — inventário + geoSignals + coordLayout em formato binário OSM PBF.

```bash
node index-pbf.js G:\sudeste-260725.osm.pbf

# fixture de teste:
node index-pbf.js test\fixtures\tiny.osm.pbf

# JSON mais leve (default): sem coordLayout.blocks
# --layout-full-blocks | --layout-max-blocks=2000
```

Saídas: `…-pbf-stats.json`, `…-pbf-results.json` (resume por offset de blob).

### Extract geocode → TXT `@` (fase 2a)

Gera arquivos no estilo DNE (delimitador `@`; logradouro **por UF**):

```bash
node extract-geocode-pbf.js G:\sudeste-260725.osm.pbf --out=G:\osm-geo-se
# --datasets=municipio,estado,logradouro --addr-points --resume
```

Saída típica: `OSM_MUNICIPIO.TXT`, `OSM_ESTADO.TXT`, `OSM_BAIRRO.TXT`, `OSM_LOGRADOURO_SP.TXT`, … + `README-colunas.md`.  
Plano e match com `locais` (ddsoft): [docs/plans/osm-para-locais-geo.md](docs/plans/osm-para-locais-geo.md).

Saídas padrão (ao lado do `.bz2`, trocando a extensão):

| Arquivo | Conteúdo |
|---------|----------|
| `…-stats.json` | inventário XML + estado para **retomar** |
| `…-results.json` | resumo da execução |

Dados detalhados de **blocos bzip** só se pedir:

```bash
node index0.js arquivo.osm.bz2 --save-bz-stats
# + …-stats-bzip.json e …-results-bzip.json
```

Sem essa flag, o detalhe bzip **não** é gravado (e **nunca** vai embutido como `bzip`/`bzStats` nos JSON principais).

### Parar e retomar

| Ctrl+C | Efeito |
|--------|--------|
| 1º | Soft-stop (até ~30s): tenta parar em fim de membro bzip ou XML perto da raiz; grava stats |
| 2º | Hard-stop após o bloco atual |
| 3º | Sai na hora (`exit 130`) |

Rodar de novo o mesmo comando **retoma** a partir do `*-stats.json` (se existir).

No início do run, o script imprime uma **legenda** da linha de progresso (percent, speed, árvore XML, …).

### API

```js
const { runProcess } = require('./index0');

const result = await runProcess({
  inputPath: 'test/fixtures/tiny.osm.bz2',
  statsPath: '/tmp/tiny-stats.json',
  quiet: true,
  resume: false,
  saveStats: true,
  saveBzStatsSeparate: false, // true = grava *-bzip.json
  checkpointEveryFiles: null,
  syncSchedule: true
});

// Contagens úteis se o dump já tiver mapa:
console.log(result.xml.nodeCount, result.xml.wayCount);
// Árvore completa de inventário:
console.log(JSON.stringify(result.xml.root, null, 2));
```

---

## Como interpretar o inventário (resumo)

Na linha de progresso, o trecho final (`xmlTree`) é um resumo tipo:

```text
/osm 1(changeset 364k(…))
```

ou, quando o dump já tiver geometria:

```text
/osm 1(node 5m(tag …),way 500k(…))
```

No JSON, o mesmo está em `xml.root.tags…` e, para chaves OSM (`k=`), em `tag_k_map`.

| Se você vir… | Significa… |
|--------------|------------|
| Muitos `changeset`, poucos/zero `node` | Ainda na seção de edições / dump sem mapa nesta fatia |
| `node` com attrs `lat`/`lon` | Há pontos com coordenada |
| `tag_k_map["addr:street"]` etc. | Há tags de endereço — candidato a geocoding |
| Só `name` + `place` | Lugares nomeados, não necessariamente endereço completo |

#### Sinais de geocoding automáticos (`geo` / `GEO`)

Além da árvore XML, a linha de progresso e o `*-results.json` trazem um resumo:

```text
… /osm 1(…) / geo n=0
… /osm 1(node 1m2…) / GEO n=1m2(ll=1m2) addr=12k st=8k name=900k
```

| Prefixo | Significado |
|---------|-------------|
| `geo` | Ainda sem combinação típica de geocoding |
| `GEO` | Já há **pontos com lat+lon** e tags de **endereço, place ou name** |

No JSON (`geocodeSignals.hints.likelyHasGeocodeMaterial` e contadores `addrStreet`, `nodeWithLatLon`, …).  
Detalhes: [docs/o-que-o-script-grava.md](docs/o-que-o-script-grava.md) §4.1.

#### Layout lat/lon no arquivo (`coordLayout`)

Além de contar, o script monta um **mapa compacto** (por bloco bzip + amostras + saltos em graus):

```text
… / GEO n=… / lay~seq seq=0.91 meanJump=0.002° blocks=40 samples=512 +addr
```

| Campo | Utilidade |
|-------|-----------|
| `seq` / `pctSmallJumps` | fração de saltos &lt; 0.01° entre nodes consecutivos no XML |
| `meanJump` | salto médio em **graus** (não km; plano lat/lon simples) |
| `blocks` | trechos do `.bz2` com bbox e contagens |
| `samples` | pontos amostrados com posição no stream |

Plano de desenho: [docs/plans/coord-layout.md](docs/plans/coord-layout.md).  
Campos: [docs/o-que-o-script-grava.md](docs/o-que-o-script-grava.md) §4.2.

---

## Testes e fixtures

```bash
npm run fixtures          # tiny / small / medium
npm test                  # suite (large* faz skip se faltar o arquivo)
npm run test:index0
npm run test:fixture -- tiny
```

| Fixture | Ordem de grandeza | No git? |
|---------|-------------------|---------|
| `tiny` / `small` / `medium` | KB–MB | sim |
| `large20` / `large200` | ~20 / ~200 MB XML | não (gitignore); `npm run fixtures:large` |

Os fixtures sintéticos têm `node`/`way` de mentira para testar o pipeline; **não** substituem o planet real.

---

## Sobre `index.js`

Rascunho antigo **incompleto** (API errada de `readBlock`, `unbzip2` sem require, etc.).  
O stub só avisa para usar `index0.js`.

---

## Dependências principais

| Pacote | Papel |
|--------|--------|
| `@arijs/seek-bzip` | Ler bzip2 por bloco / com seek |
| `@arijs/stream-xml-parser` | Parser XML por eventos |
| `pbf` | Protos OSM no repo (uso futuro / outros scripts) |

---

## Estrutura do repositório

```
osm-tools/
├── README.md                 ← este arquivo
├── index0.js                 ← inventário XML/bz2
├── index-pbf.js              ← inventário PBF (ex. Sudeste)
├── pbf-reader.js / fileformat.proto.js
├── geocode-signals.js        ← contadores GEO compartilhados
├── index.js                  ← stub incompleto
├── coord-layout.js           ← mapa lat/lon × stream (graus)
├── docs/
│   ├── objetivo-e-contexto.md
│   ├── o-que-o-script-grava.md
│   ├── amostra-estrutura-dados.md
│   ├── plans/coord-layout.md
│   └── changelog/
├── scripts/
│   ├── generate-fixtures.py
│   └── run-named-tests.js
└── test/
    ├── fixtures/
    ├── index0.test.js
    ├── xml-stack.test.js
    └── …
```

---

## Licença

MIT.
