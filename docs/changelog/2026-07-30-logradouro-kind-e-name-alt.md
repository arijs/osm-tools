# 2026-07-30 — `kind` (praça/parque) e `name_alt` no extract de logradouro

## Prompt original

> Dê uma sugestão pra melhorar nosso extrator e pegar praças/outros tipos de shapes além de highway, antes de nós rodarmos ele novamente pra pegar o dataset addr

E, na sequência: **"Sim!"** — implementar o patch proposto no §10.2 do documento
`docs/geo/melhoria-extracao-coordenadas.md` junto com os testes do §10.5.

## Por quê (medido, não suposto)

Taxa de casamento DNE↔OSM por `TLO_TX` na capital paulista, contra
`G:\osm-geo-se-streets`:

| `TLO_TX` | Linhas | Casadas | % |
|----------|-------:|--------:|--:|
| Alameda | 279 | 254 | 91,0 % |
| Avenida | 2 591 | 2 339 | 90,3 % |
| Rua | 41 684 | 35 969 | 86,3 % |
| **Praça** | **2 703** | 971 | **35,9 %** |
| **Via de Pedestre** | 527 | 197 | 37,4 % |
| **Parque** | 25 | 7 | 28,0 % |
| **Vila** | 151 | 32 | 21,2 % |

Rua/avenida/alameda em ~90 % — o extract fazia bem o que se propôs. O que
despencava era o que **não é linha**. Praça no OSM não é `highway`: é
`place=square`, `leisure=park` ou área fechada. A capital inteira tem só 458
ways `highway=pedestrian` no arquivo — prova de que o dado nunca foi pedido.

Em SP inteiro são 9 631 praças no DNE.

## O que foi implementado

### 1. `logradouroKind(tags)` — alarga o predicado, não o pipeline

`extract-geocode-pbf.js`. O portão do dataset era `tags.highway && name`;
agora é `logradouroKind(tags) && name`, com:

| Tag | `kind` |
|-----|--------|
| `highway=*` | o próprio valor (`residential`, `primary`, `pedestrian`…) |
| `place=square` | `square` |
| `leisure=park` · `leisure=garden` · `landuse=village_green` | `park` |

`highway` ganha quando a way tem os dois (praça com via de pedestre desenhada).

Sem passe novo, sem dataset novo: `geomFromNodeIds` já calcula bbox e centroide
dos nós — para way fechada isso é o polígono da praça — e o two-pass já agenda
`nodeIds` sem olhar tag.

### 2. Praça como nó

`processFeatureNode` passou a emitir `place=square` com nome no mesmo arquivo,
com `osm_type=node` e bbox degenerada. Coordenada real sem extensão — não
confundir com o fallback de centroide, que foi cortado do plano (§8.1).

### 3. `name_alt` / `name_alt_norm`

`altNames(tags, primary)` coleta `alt_name`, `short_name`, `old_name`,
`loc_name`, `name:pt-BR`, `official_name`, separados por `;` (convenção do
próprio OSM), sem repetir o nome principal. **A cadeia do `name` principal em
`displayName` não mudou** — as variantes entram em coluna nova, não como
fallback.

É o equivalente OSM do `LOG_VAR_LOG` do DNE (que, medido, rende 3 nomes em SP
inteiro — o lado OSM tende a render mais).

## Antes / depois

Contrato de `OSM_LOGRADOURO_{UF}` (delimitador `@`):

```diff
-osm_id@name@name_norm@highway@uf@city@city_norm@suburb@suburb_norm@postcode@
-lat@lng@lat_min@lat_max@lng_min@lng_max@way_node_count
+osm_id@name@name_norm@kind@uf@city@city_norm@suburb@suburb_norm@postcode@
+lat@lng@lat_min@lat_max@lng_min@lng_max@way_node_count@name_alt@name_alt_norm@osm_type
```

Colunas 1–17 **não mudaram de posição** — só a 4 mudou de significado
(`highway` → `kind`, superconjunto do valor anterior). As três novas são
append. Consumidor que lê por índice continua funcionando.

```diff
 function processFeatureWay(ctx, way) {
-	if (wantDataset(ctx.datasets, 'logradouro') && tags.highway && name) {
+	if (wantDataset(ctx.datasets, 'logradouro') && logradouroKind(tags) && name) {
```

## Guarda obrigatória no consumidor

`leisure=park` traz `Parque Villa-Lobos`, `Parque do Ibirapuera` e milhares de
parques que **não são logradouro**. Regra no join: candidato com `kind` de área
(`square`, `park`) só vale para linha DNE com `TLO_TX` de área — Praça, Largo,
Parque, Jardim, Vila, Área. Para `Rua`/`Avenida`, só `kind` de via.

Sem isso, `Rua Villa-Lobos` casa com o parque e estraga o que já funcionava a
86–91 %. Documentado no `README-colunas.md` gerado e em
`docs/geo/extract-e-artefatos.md`.

## Como foi testado

**Automatizado** — `node --test test/*.test.js`: **69 testes, 67 pass, 0 fail,
2 skip** (os 2 skip são fixtures grandes, já puladas antes desta alteração).

Testes novos em `test/extract-geocode.test.js`:

| Teste | Cobre |
|-------|-------|
| `logradouroKind aceita área além de highway` | os 5 casos positivos, `highway` ganhando de `place`, e 3 negativos (`landuse=residential`, `building`, `null`) |
| `altNames coleta variantes sem repetir o nome principal` | multi-valor `;`, dedup entre tags, nome principal excluído, `null` |
| `runExtractGeocode emite TXT por nível e logradouro por UF` (ampliado) | praça way (`kind=square`, bbox com altura real), parque (`kind=park`), praça nó (`osm_type=node`, bbox degenerada, `way_node_count=1`), `Largo Teste` como `pedestrian`, `name_alt` da Rua Augusta, e `Jardim Teste` **ausente** do arquivo |

Fixture `scripts/write-geocode-pbf.js` ganhou 5 nós e 4 ways para cobrir isso.

**Manual** — as taxas por `TLO_TX` da tabela acima saíram de sondagem sobre a
base real (`G:\osm-geo-se-streets` + `Delimitado`), e são a linha de base para
comparar depois do re-extract.

## Testes criados/alterados

- `test/extract-geocode.test.js` — 2 testes novos + asserções novas no de integração
- `scripts/write-geocode-pbf.js` — fixture ampliada

## Arquivos alterados

- `extract-geocode-pbf.js` — `logradouroKind`, `altNames`, portão do dataset, `writeLogradouroRow` (+`osmType`), emissão de nó `place=square`, `README-colunas.md` gerado, exports
- `scripts/write-geocode-pbf.js`
- `test/extract-geocode.test.js`
- `docs/geo/extract-e-artefatos.md`
- `docs/geo/melhoria-extracao-coordenadas.md` (§10)

## Próximo passo

Re-extract em **pasta nova** (`G:\osm-geo-se-streets` é a linha de base dos
números deste changelog e sem `--resume` o extract apaga a pasta):

```bash
set NODE_OPTIONS=--max-old-space-size=8192
node extract-geocode-pbf.js G:\sudeste-260725.osm.pbf ^
  --out=G:\osm-geo-se-streets2 ^
  --datasets=logradouro,bairro,addr
```

Critério de aceite: **Praça sai de 35,9 %** e Rua/Avenida **não caem**. Se Rua
cair, o `kind` vazou para o match — é a guarda kind-aware que falhou.
`writerCounts.OSM_ADDR_POINT_SP > 0` confirma que o `addr` rodou (hoje
`addr: 0` no checkpoint).

## Resultado do re-extract (mesmo dia)

Rodado em `G:\osm-geo-se-streets2`, completo (`eof: true`): logradouro
**1 265 470** (era 1 240 490), bairro 30 906, addr **205 660**.

| `TLO_TX` | Linhas | Antes | Depois |
|----------|-------:|------:|-------:|
| **Praça** | 2 703 | 35,9 % | **76,1 %** |
| **Parque** | 25 | 28,0 % | **68,0 %** |
| **Largo** | 71 | 66,2 % | **77,5 %** |
| **Ponte** | 24 | 37,5 % | **54,2 %** |
| Rua | 41 684 | 86,3 % | 87,4 % |
| Avenida | 2 591 | 90,3 % | 91,5 % |

**Aceite cumprido:** Praça mais que dobrou e nenhum tipo caiu — Rua e Avenida
subiram, por conta do `name_alt`. Total da capital: 81,1 % → 84,3 %.

Com a cascata determinística completa (núcleo sem tipo + chave fonética PT-BR,
§8.5 do documento de melhoria) a capital chega a **87,8 %**, ante 85,1 % com o
extract antigo.

Três resultados abaixo do previsto, registrados para não se reestimar depois:

- `addr:street` rendeu **+0,1 pp** como fonte de nome (33 linhas). Os 7 147
  nomes que ele traz na capital já eram quase todos conhecidos. O valor real do
  dataset é `addr:postcode` como guarda geográfica e nomear way sem `name` por
  proximidade — nada disso é casamento por nome, e nada disso foi testado.
- A guarda kind-aware custou **2 linhas** (1 111 → 1 109). O risco era menor do
  que a estimativa, mas continua seguro de graça.
- Praça como **nó** rendeu 2 features na capital. O ganho de praça veio quase
  todo de `leisure=park` (4 533 nomes contra 73 de `place=square`): no Brasil a
  praça costuma estar mapeada como parque.
