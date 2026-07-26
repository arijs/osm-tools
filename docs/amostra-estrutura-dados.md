# Amostra da estrutura de dados

Exemplos **ilustrativos** do que o inventário produz e de como o XML OSM se parece.  
Os números e valores abaixo misturam:

- trechos **reais** observados em `H:\osm\planet-latest.osm-stats.json` (processamento parcial), e  
- exemplos **didáticos** de `node`/`way` com endereço (ainda não vistos nesse inventário parcial, mas típicos de dumps OSM de mapa).

O inventário é **agregado**: não lista cada elemento do planeta.

---

## 1. Trecho XML típico (dados de mapa — o que se busca para geocoding)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6" generator="exemplo">
  <node id="1001" version="3" lat="-23.561414" lon="-46.655881">
    <tag k="addr:housenumber" v="1578"/>
    <tag k="addr:street" v="Avenida Paulista"/>
    <tag k="addr:city" v="São Paulo"/>
    <tag k="name" v="Conjunto Nacional"/>
  </node>
  <way id="2002" version="1">
    <nd ref="1001"/>
    <nd ref="1002"/>
    <tag k="highway" v="primary"/>
    <tag k="name" v="Avenida Paulista"/>
  </way>
</osm>
```

O que o script **faria** com isso no inventário (conceitualmente):

```
osm.count = 1
  attrs: version, generator (firstVal/lastVal)
  tags.node.count = 1
    attrs.lat.firstVal = "-23.561414"
    attrs.lon.firstVal = "-46.655881"
    attrs.id.firstVal = "1001"
    tag_k_map["addr:housenumber"].count = 1
    tag_k_map["addr:street"].count = 1
    tag_k_map["addr:city"].count = 1
    tag_k_map["name"].count = 1
  tags.way.count = 1
    tags.nd.count = 2
    tag_k_map["highway"].count = 1
    tag_k_map["name"].count = 1
```

**Ainda não grava** a associação completa “Paulista 1578 → (-23.56, -46.65)” em um registro exportável — só prova que essas chaves e atributos **existem** e com que frequência.

---

## 2. Trecho XML típico (changesets — o que o inventário parcial do planet já mostrou)

No processamento real de `planet-latest.osm.bz2` neste ambiente, a árvore sob `osm` continha principalmente `bound` e muitos `changeset`:

```xml
<osm version="0.6" generator="planet-dump-ng 1.1.6"
     timestamp="2018-11-05T02:00:01Z"
     copyright="OpenStreetMap and contributors"
     attribution="http://www.openstreetmap.org/copyright"
     license="http://opendatacommons.org/licenses/odbl/1-0/">
  <bound box="-90,-180,90,180" origin="http://www.openstreetmap.org/api/0.6"/>
  <changeset id="960186"
             created_at="2009-04-25T22:12:40Z"
             closed_at="2009-04-25T22:12:42Z"
             open="false"
             user="geobase_stevens"
             uid="92209"
             num_changes="0"
             comments_count="0">
    <tag k="comment" v="Merging nodes at intersections defined by junctions"/>
  </changeset>
  <!-- … centenas de milhares de changesets … -->
</osm>
```

Isso **não** é um endereço de usuário final. É metadado de **edições** no OSM (`changeset`).  
Coordenadas `min_lat` / `min_lon` / `max_lat` / `max_lon` (quando presentes) descrevem a **caixa** da edição, não um número de porta.

---

## 3. Amostra condensada de `xml.root` (dados reais parciais)

Forma aproximada do inventário após processar uma fatia do dump (valores reais arredondados/simplificados):

```json
{
  "tags": {
    "osm": {
      "count": 1,
      "attrs": {
        "version": { "count": 1, "firstVal": "0.6", "lastVal": null },
        "generator": { "count": 1, "firstVal": "planet-dump-ng 1.1.6", "lastVal": null },
        "timestamp": { "count": 1, "firstVal": "2018-11-05T02:00:01Z", "lastVal": null },
        "copyright": { "count": 1, "firstVal": "OpenStreetMap and contributors", "lastVal": null }
      },
      "tags": {
        "bound": {
          "count": 1,
          "attrs": {
            "box": { "count": 1, "firstVal": "-90,-180,90,180", "lastVal": null }
          }
        },
        "changeset": {
          "count": 364847,
          "attrs": {
            "id": { "count": 364847, "firstVal": "1", "lastVal": "364847" },
            "created_at": { "count": 364847, "firstVal": "2005-04-09T19:54:13Z", "lastVal": "2008-10-15T13:05:37Z" },
            "user": { "count": 358438, "firstVal": "Steve", "lastVal": "baali" },
            "min_lat": { "count": 338021, "firstVal": "51.5288506", "lastVal": "28.5584521" },
            "min_lon": { "count": 338021, "firstVal": "-0.1465242", "lastVal": "77.1879444" }
          },
          "tags": {},
          "tag_k_map": {}
        }
      }
    }
  }
}
```

### Como ler isso

| Observação | Interpretação |
|------------|----------------|
| Só `bound` + `changeset` sob `osm` | Nesta fatia **ainda não** há `node`/`way`/`relation` no inventário |
| `changeset.count` ~ 365k | Volume alto de metadados de edição |
| `min_lat` / `min_lon` com first/last | Há bounding boxes de changesets; **não** substitui endereço |
| `tag_k_map` vazio sob changeset no sample | Nested `<tag k=…>` pode aparecer em outros trechos; o sample de `last` no stats mostrou `k=comment` |

Quando o processamento avançar para a seção de nós do planet, espera-se algo como:

```json
"node": {
  "count": 5000000,
  "attrs": {
    "lat": { "count": 5000000, "firstVal": "…", "lastVal": "…" },
    "lon": { "count": 5000000, "firstVal": "…", "lastVal": "…" }
  },
  "tag_k_map": {
    "addr:street": { "count": 120000 },
    "addr:housenumber": { "count": 110000 },
    "name": { "count": 800000 },
    "place": { "count": 50000 }
  }
}
```

Esse seria um **sinal forte** de que o dump tem matéria-prima para geocoding — ainda sem extrair os pares endereço↔coordenada.

---

## 4. Esqueleto de `*-stats.json`

```json
{
  "xml": {
    "tags": ["osm"],
    "index": [0],
    "current": 364848,
    "root": { "tags": { "osm": { "count": 1, "tags": {}, "attrs": {} } } },
    "first": [ { "event": "open", "name": "osm", "attrs": { "…": "…" } } ],
    "last": [
      { "event": "open", "name": "changeset", "attrs": { "id": "…" } },
      { "event": "open", "name": "tag", "attrs": { "k": "comment", "v": "…" } },
      { "event": "close", "name": "tag" },
      { "event": "close", "name": "changeset" }
    ],
    "remain": { "buffer": "50.", "pos": 123456 }
  },
  "fopt": {
    "fileCount": 99,
    "fileOffset": 12528931,
    "byteOffset": 130507,
    "bitOffset": 3,
    "blockCount": 2,
    "blockCRC": "de595e96",
    "streamCRC": null,
    "bzLevelFile": 9
  },
  "runs": [
    {
      "start": "2026-07-25T18:00:00.000Z",
      "time": 66,
      "percent": 0.00016,
      "speed": 190000,
      "bzFile": 99,
      "bzBlock": 2
    }
  ],
  "current": {
    "percent": 0.00016,
    "time": 66,
    "chunkCount": [3093, 299, 364854],
    "chunkPos": [12668928, 90000000, 90000000],
    "bzFile": 99,
    "bzBlock": 2,
    "bzNextFileOffset": 12528931,
    "bzCurrentFile": { "blocks": [], "input": 0, "output": 0 }
  }
}
```

Campos omitidos ou enxutos em saves recentes: sem `bzip` embutido.

---

## 5. Esqueleto de `*-results.json`

```json
{
  "error": null,
  "stoppedEarly": true,
  "stopReason": "xml-near-root",
  "inputPath": "H:\\osm\\planet-latest.osm.bz2",
  "statsPath": "H:\\osm\\planet-latest.osm-stats.json",
  "resultsPath": "H:\\osm\\planet-latest.osm-results.json",
  "fileSize": 76609373305,
  "chunkCount": [3093, 299, 364854],
  "chunkPos": [12668928, 90000000, 90000000],
  "bzIndexFile": 99,
  "bzIndexBlock": 2,
  "xmlStackMismatches": 0,
  "xml": {
    "tags": ["osm"],
    "nodeCount": 0,
    "wayCount": 0,
    "root": { "tags": { "osm": { "count": 1, "tags": { "changeset": { "count": 364847 } } } } }
  },
  "elapsedMs": 66000
}
```

Interpretação prática: `nodeCount`/`wayCount` zero + só `changeset` na árvore ⇒ **ainda não estamos na parte do arquivo onde o mapa/endereços costumam estar** (ou o dump é só de changesets).

---

## 6. Fixture de teste (`tiny`) — inventário mínimo completo

O fixture gerado em `test/fixtures/tiny.osm` é pequeno e **já** contém `node`/`way` (dados sintéticos de teste, não o planet real):

```text
osm
  bounds
  node × 3
    tag k=name, tag k=source
  way × 1
    nd × 4
    tag k=highway
```

Útil para validar o pipeline; **não** representa a ordem nem o conteúdo do `planet-latest` real.

---

## 7. Checklist: “já tenho dados de geocoding neste dump?”

### Atalho automático (recomendado)

Olhe o final da linha de progresso ou `geocodeSignals` no `*-results.json`:

| Indicador | Interpretação |
|-----------|----------------|
| `geo n=0` | Ainda sem nodes (ex.: só changesets) |
| `geo n=1m2(ll=1m2)` sem `addr`/`name`/`place` | Há pontos, mas poucas tags de endereço/lugar |
| **`GEO`** `n=…(ll=…) addr=…` ou `name=…` / `place=…` | **Material provável de geocoding** (`hints.likelyHasGeocodeMaterial`) |

Exemplo no results:

```json
"geocodeSignals": {
  "node": 1000000,
  "nodeWithLatLon": 1000000,
  "addrStreet": 80000,
  "addrHousenumber": 75000,
  "name": 500000,
  "hints": {
    "hasPointGeometry": true,
    "hasAddressTags": true,
    "likelyHasGeocodeMaterial": true
  }
}
```

### Manual (árvore XML)

| Sinal | O que procurar |
|-------|----------------|
| Geometria de ponto | `…tags.node` com `attrs.lat` e `attrs.lon` e `count` alto |
| Nome | `tag_k_map["name"]` ou `["name:pt"]` com count alto |
| Endereço | `tag_k_map["addr:street"]`, `addr:housenumber`, `addr:city`, `addr:postcode` |
| Lugar | `tag_k_map["place"]` (city, town, village, …) |
| Ruas | `…tags.way` + `tag_k_map["highway"]` + `name` |
| Só edições | predominância de `changeset` (como no sample parcial do planet) |

Se a resposta for “ainda não”, continue o processamento (ou use um extract regional / dump de dados atuais sem a seção de changesets, se for o caso do arquivo).

---

## Ver também

- [objetivo-e-contexto.md](./objetivo-e-contexto.md) — meta de geocoding e fases  
- [o-que-o-script-grava.md](./o-que-o-script-grava.md) — referência de cada campo  
- OpenStreetMap Wiki: [Addresses](https://wiki.openstreetmap.org/wiki/Addresses), [Key:addr:\*](https://wiki.openstreetmap.org/wiki/Key:addr:*)  
