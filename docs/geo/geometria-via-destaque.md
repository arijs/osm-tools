# Geometria da via — destacar o logradouro no mapa por id exato

> **Especificação das duas pontas.** osm-tools emite; ddsoft carrega, serve e desenha.
> Pedido do Rafael (2026-08-06): *"pegar as vias pelo ID exato no OSM e fazer o destaque no
> mapa, sem depender do nome no runtime"*.
> Estado: **osm-tools feito** (esta branch). **ddsoft não começado** — §5 é o roteiro.

## 1. O problema

O mapa do ddsoft já lista logradouros do DNE dentro de uma área demarcada e destaca o
**ponto** do resultado escolhido (halo pulsante + popup). O ponto é o centroide do cluster
de ways — serve para dizer *onde*, não *qual*. Uma via é um traço, e é o traço que responde
"é esta rua aqui".

O caminho óbvio — filtrar o basemap pelo nome — é o mesmo beco que este pipeline já
percorreu no PHP e abandonou: `Rua São João` existe em dezenas de municípios da região
metropolitana, e o DNE escreve `R` + `SÃO JOÃO` enquanto o OSM escreve `Rua São João`.
Casar nome em runtime é repetir, no navegador e sem contexto municipal, o trabalho que o
`dne-geo-join.js` faz offline com footprint, cascata e desempate.

## 2. Por que o id do OSM não resolve sozinho

A primeira ideia é guardar o `osm_id` da way e apontar para ela no basemap. **Não dá**: os
tiles do Protomaps não expõem id. A layer `roads` do schema publicado carrega `kind`,
`kind_detail`, `ref`, `shield_text`, `network`, `oneway`, `service`, `is_link`, `is_tunnel`,
`is_bridge` (mais os campos de nome, que os rótulos usam) — e nenhum identificador de
origem. Sem id no tile não há `filter` nem `feature-state` que selecione a feature.

Isso **muda o desenho para melhor**. O id deixa de ser um ponteiro para o basemap e passa a
ser a chave da **nossa** geometria, que servimos e desenhamos numa camada própria:

| | filtro por nome no basemap | geometria própria por id |
|---|---|---|
| Precisão | toda via homônima nos tiles carregados | exatamente o cluster que o join escolheu |
| Nome em runtime | obrigatório (e abreviado no DNE) | nenhum |
| Fora do recorte do PMTiles | não desenha | desenha |
| Zoom | some abaixo de ~12,5 (largura 0 no estilo) | largura é nossa |
| Rua sem `name` no OSM | invisível | aparece, se entrou no cluster |

## 3. O que já existia, e as duas perdas de dado

O pipeline **já sabia** tudo o que é preciso. O que faltava era não jogar fora:

**No `dne-geo-join.js`**, a linha do extract é lida com `p[0]` = `osm_id`, mas o objeto de
feature montado para o cluster guardava só `{lat, lng, bbox, n, kind}`. O id morria ali. Na
saída, `osm_ways` (coluna 24) é a **contagem** de ways do cluster — nunca quais.

**No `extract-geocode-pbf.js`**, `geomFromNodeIds()` percorre a coordenada de cada nó da way
para calcular centroide e bbox, e descarta as coordenadas no fim do laço. A geometria passa
inteira pela memória e não sobra em artefato nenhum.

As duas correções são pequenas e ficam onde o dado já está. Nenhuma releitura extra do PBF:
a pass 2 já resolveu os nós.

## 4. Contratos novos (osm-tools — feito)

### 4.1 `OSM_LOGRADOURO_GEOM_{UF}.TXT` — o traçado

Opt-in: `--way-geom` (ou `--datasets=logradouro,geom`; pedir `geom` sem `logradouro` liga o
logradouro junto, senão a saída seria silenciosamente vazia).

```
osm_id@polyline
```

Irmão de `OSM_LOGRADOURO_{UF}`: mesma UF, mesmo filtro de fatia, mesmo fatiamento por
`--shard-lines`. **Todo `osm_id` daqui existe lá; o inverso não vale** — nó e way sem nó
resolvido ficam de fora.

Arquivo separado, e não uma coluna a mais na linha de logradouro, por dois motivos: o
`dne-geo-join.js` lê o arquivo de logradouro inteiro e não tem uso para geometria (pagaria
~3× o I/O à toa), e quem quer só o traçado não precisa varrer 20 colunas de nome e bbox.

### 4.2 Codificação da polyline

Pontos separados por `;`, cada um `lat,lng` em **unidades de 1e-6 grau** (inteiros). O
primeiro é absoluto; os seguintes são **deltas** do anterior:

```
-23552000,-46632000;-1000,-1000;2000,500
```

Três decisões, cada uma com uma razão que não é estética:

- **Não é o polyline codificado do Google.** O alfabeto dele vai de 63 a 126 na tabela
  ASCII, e 64 é `@` — o delimitador destes arquivos. O `txt-at-writer` troca `@` por espaço
  em silêncio: a geometria sairia corrompida **sem erro nenhum**, aparecendo só como traçado
  torto no mapa, meses depois. O alfabeto daqui é dígito, `-`, `,` e `;`.
- **Delta.** O absoluto custa ~19 caracteres e cada delta 2 a 4. Numa way de 10 nós é a
  diferença entre ~190 e ~60 bytes por linha.
- **1e-6 (≈ 0,11 m), não 1e-5 (≈ 1,1 m).** 1,1 m dá 4–5 pixels no zoom 18: escadinha visível
  em curva. Uma casa decimal a mais custa um dígito por delta.

Regras de emissão:

- Só entra way com **2+ pontos distintos** após o arredondamento — toda linha do arquivo
  desenha. Praça mapeada como nó, way sem nó resolvido e way degenerada são contadas em
  `logradouroGeomVazio` e ficam de fora.
- Ponto repetido **consecutivo** é descartado; repetido no fim **fica** — é assim que a way
  fechada (praça, parque) mantém o anel.
- **Não há simplificação** (Douglas-Peucker e afins): o que entra é o que o OSM tem, e a
  única perda é o arredondamento. Simplificar é decisão de quem desenha e pode ser feita
  depois, sem regerar o artefato.

O codec vive em `geo-polyline.js` (`encodePolyline` / `decodePolyline` / `countPolyline`) e
é testado à parte, inclusive contra o sanitizador do writer.

### 4.3 `DNE_GEO_LOGRADOURO_{UF}.TXT` — coluna 26 `osm_way_ids`

As ways do cluster vencedor, unidas por `+`, em ordem numérica, sem repetição. Vazia quando
`geo_status != ok`. Ponto de `addr:street` não entra: é nó de numeração, não tem traçado, e
um id que não resolve do outro lado é pior que coluna vazia.

```
…@ok@exato@secondary@28@4@12345678+12345679+…
```

Contrato completo em [dne-geo-join.md §Contrato de saída](./dne-geo-join.md).

**A coluna repete em todas as linhas DNE que compartilham o cluster**, e a cauda é longa
(`Avenida Paulista`: 207 ways, ~2,3 KB por linha, × 19 `loc_nu`). É redundância aceita em
troca de o carregador continuar lendo **um** arquivo por `log_nu`. Se doer, o caminho é um
artefato `DNE_GEO_CLUSTER_{UF}.TXT` (`cluster_id@osm_way_ids`) com só o id do cluster na
linha — não faça isso antes de medir.

### 4.4 Volumetria — a medir na primeira rodada

Conta de guardanapo, com SP (793 906 ways no extract) e uma way típica de ~10 nós:

| | estimativa |
|---|---|
| Bytes por linha do GEOM | ~60 + id ≈ 75 |
| `OSM_LOGRADOURO_GEOM_SP.TXT` (todas as ways) | ~50–60 MB |
| Só as ways de cluster vencedor (o que o ddsoft guarda) | ~15–30 MB |
| Coluna 26 no `DNE_GEO_LOGRADOURO_SP` | +15–20 MB |

O número real sai do primeiro extract com `--way-geom`: o resumo imprime
`Geom: N vias, M pontos (sem traçado: K)`. **Trocar estas estimativas pelos números medidos
quando eles existirem** — o resto deste documento não depende delas.

## 5. Roteiro do ddsoft (não começado)

### 5.1 Migrations

Duas tabelas de infra, fora do modelo drawDB, como as `dne_idx_*` (mesma justificativa:
índice regenerável, não catálogo de produto):

```
dne_geo_via              osm_id (PK, BIGINT)  uf CHAR(2)  pontos MEDIUMTEXT
                         n_pontos SMALLINT UNSIGNED       atualizado_em DATETIME

dne_idx_logradouro_via   log_nu INT  osm_id BIGINT   PK (log_nu, osm_id)  KEY (osm_id)
```

Por que tabela de ligação e não uma coluna `osm_way_ids` em `dne_idx_logradouro`: a cauda de
207 ways estoura qualquer `VARCHAR` razoável, e a ligação normalizada responde de graça
"quantos logradouros têm traçado" e "esta way serve a quantas faixas de CEP".

`pontos` como texto (a mesma polyline do artefato) e **não** como `LINESTRING`/`SPATIAL`: a
consulta é `WHERE osm_id IN (…)`, um lookup por chave. Tipo espacial só se paga quando a
pergunta for espacial ("que vias cruzam este retângulo") — e aí a decisão é outra, com
índice e SRID. Vale a mesma nota de `estrutura-dados-endereco.md` §11.2 sobre `DOUBLE`.

### 5.2 Carregador

Estender `osm:dne:enrich-geo` (que já prefere `DNE_GEO_*` e resolve flat/shard pelo
`resolveDatasetPaths`) ou comando irmão `osm:dne:load-via-geom --dir=… --uf=SP`:

1. Ler `DNE_GEO_LOGRADOURO_{UF}` → povoar `dne_idx_logradouro_via` a partir da coluna 26
   (só `geo_status=ok`), acumulando o conjunto de `osm_id` **usados**.
2. Ler `OSM_LOGRADOURO_GEOM_{UF}` em streaming → gravar em `dne_geo_via` **só** os ids
   usados. É o que separa ~50 MB de ~20 MB, e o que evita guardar o traçado de way que
   nenhuma linha DNE reivindica.
3. Idempotente por `osm_id` (upsert) e por par (`log_nu`, `osm_id`).

### 5.3 GraphQL

Campo custom, no molde de `buscar_dne_area` (as `dne_*` vivem fora do modelo e o motor
genérico não as expõe):

```graphql
geometria_logradouro(log_nu: Int!): GeometriaLogradouro
# { log_nu, ways: Int!, pontos: Int!, linhas: [[[Float!]!]!]! }
```

**`linhas` em ordem GeoJSON — `[lng, lat]`, não `[lat, lng]`.** O TXT guarda `lat,lng` (é a
ordem do DNE e do resto do pipeline) e o GeoJSON quer o inverso; é o erro clássico desta
fronteira, e o lugar de resolvê-lo é aqui, uma vez, com teste.

Uma linha por way, **não** concatenadas: uma via com trechos separados (canteiro central,
quadra interrompida) desenharia um traço fantasma ligando as pontas.

Vale expor também `tem_geometria: Boolean` em `buscar_dne_area`, para a lista do painel
poder marcar quais resultados vão destacar antes do clique.

### 5.4 Frontend

No `selecionar()` de um item com `fonte === 'dne_logradouro'`: buscar a geometria (cache por
`log_nu` num `Map`, a lista tem no máximo 100 itens), alimentar uma source GeoJSON
`via-destaque` e uma layer `line` na cor da fonte (`#2f9e44`), com largura interpolada por
zoom, **acima** das ruas do basemap e **abaixo** dos marcadores. O halo pulsante do ponto
continua: ele é que responde "onde", enquanto o traço responde "qual".

Limpar ao desmarcar, ao trocar de fonte e ao invalidar a busca — as mesmas três portas que
já limpam a seleção hoje (`limparResultadosUi`).

Sem geometria (`geo_status != ok`, UF sem extract, resultado fora do recorte): **só o halo**,
sem aviso ruidoso. Vazio honesto, como o resto do mapa.

## 6. Limites conhecidos

- **Via inteira, sem seccionamento.** O join casa o `log_nu` com um cluster de ways, e o
  cluster é a rua toda. Clicar em `Rua Augusta - até 697 - lado ímpar` destaca a Augusta
  inteira daquele cluster, não o trecho daquele CEP. Recortar por faixa de numeração
  dependeria de `OSM_ADDR_POINT_*`, que hoje está populado e sem consumidor
  ([bairro-logradouro.md §4](./bairro-logradouro.md)).
- **Cobertura parcial e assimétrica**: 75,0 % `ok` em SP, 71,9 % no RJ, 54,7 % em MG,
  `sem_extract` fora do Sudeste. Some os 9,4 % `ambiguo` e uma fatia grande dos resultados
  não terá traço.
- **Duas ways vizinhas com o mesmo nome em municípios diferentes** já são separadas pelo
  cluster + footprint; o que sobra de erro aqui é erro do join, não do desenho — e agora
  fica **visível**, que é um efeito colateral bom: um traço em bairro errado denuncia um
  match que o número sozinho escondia.
- **O traçado é do OSM, não do DNE.** Divergência de prolongamento, via renomeada, quadra
  nova: o mapa mostra o OSM. Não há reconciliação, e não deve haver.

## 7. Alternativa considerada: tiles nossos

Como o backend já serve `.pmtiles` com Range (ADR-0020), daria para gerar um tileset das
vias casadas com `log_nu` nas properties (tippecanoe) e o destaque viraria um `filter` sem
fetch nenhum — a mesma camada mostraria a cobertura geo de graça. Para destacar **uma** via
por vez o endpoint é muito menos trabalho, então fica como caminho de escala: se aparecer
"pinte todas as vias sem coordenada" ou "mostre a cobertura por bairro", é por ali.

## 8. Checklist

- [x] `geo-polyline.js` — codec + testes (incl. contra o sanitizador `@`)
- [x] `extract-geocode-pbf.js` — `--way-geom` → `OSM_LOGRADOURO_GEOM_{UF}`, shards, resumo, README-colunas
- [x] `dne-geo-join.js` — coluna 26 `osm_way_ids` (determinística, sem `addr`)
- [ ] Rodar `--way-geom` em SP e trocar as estimativas de §4.4 pelos números medidos
- [ ] Re-rodar o join nas UFs já carregadas (a coluna 26 não existe nos TXT antigos)
- [ ] ddsoft: migrations `dne_geo_via` + `dne_idx_logradouro_via`
- [ ] ddsoft: carregador (só ids usados)
- [ ] ddsoft: `geometria_logradouro` + `tem_geometria` em `buscar_dne_area`
- [ ] ddsoft: camada `via-destaque` no mapa

## 9. Ligações

- [dne-geo-join.md](./dne-geo-join.md) — spec do join; contrato de saída com a coluna 26
- [extract-e-artefatos.md](./extract-e-artefatos.md) — contrato dos TXT do extract
- [bairro-logradouro.md](./bairro-logradouro.md) — cobertura medida, `OSM_ADDR_POINT_*` sem consumidor
- ddsoft: `claude/mapa-busca-area-multifonte.md` — a busca por área e as quatro fontes
- ddsoft: `docs/locais-tenant-e-dne/estrutura-dados-endereco.md` §11 — por que `DOUBLE` e não espacial
