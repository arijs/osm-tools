# Extract PBF → TXT `@` (artefatos intermediários)

Script: **`extract-geocode-pbf.js`** (reutiliza `pbf-reader`, protos OSM, helpers `name-norm`, `uf-br`, `txt-at-writer`).

## Objetivo

Ler um `.osm.pbf` regional e gravar **só** features relevantes para casar com o cadastro brasileiro — **não** inventário, **não** 141 M nodes no disco.

Modelo de arquivo: **igual espírito DNE** — texto UTF-8, delimitador **`@`**, sem header, regenerável. Contrato: `README-colunas.md` na pasta de saída.

## Datasets

| Dataset CLI | Arquivo(s) | Filtro OSM típico |
|-------------|------------|-------------------|
| `estado` | `OSM_ESTADO.TXT` | `place=state` ou `boundary=administrative` + `admin_level=4` |
| `municipio` | `OSM_MUNICIPIO.TXT` | `place=city\|municipality\|town` ou admin `admin_level=8`; IBGE só **7 dígitos** |
| `bairro` | `OSM_BAIRRO.TXT` | `place=suburb\|neighbourhood\|quarter` (e afins) |
| `logradouro` | `OSM_LOGRADOURO_{UF}.TXT` | `name` (ou `name:pt`) **e** `highway=*` · `place=square` · `leisure=park\|garden` · `landuse=village_green` — way **ou** node |
| `addr` / `--addr-points` | `OSM_ADDR_POINT_{UF}.TXT` | node com `addr:street` (opcional; **não** popular número global em massa) |

Default de datasets: estado + município + bairro + logradouro (`addr` off).

### Logradouro por UF (obrigatório no desenho)

Espelho de `LOG_LOGRADOURO_{UF}.TXT` do DNE:

- Flat: `OSM_LOGRADOURO_SP.TXT`, `_RJ`, `_MG`, `_ES`, …
- Residual **`XX`** se UF não resolvida (tags → IBGE → bbox do way vs retângulos UF BR)
- Filtro de fatia: `--uf=SP,RJ` / `--region=norte|nordeste|centro-oeste|sudeste|sul`
- Waves (anti-OOM): `--wave-nodes=8000000` / `--wave-streets=150000` — ver changelog 2026-07-31
- **Fatiado** (`--shard-lines=N`):

```text
OSM_LOGRADOURO_SP/
  100000-linhas/
    000001.txt
    000002.txt
  MANIFEST.json
```

Atribuição de UF (ordem): tags (`ISO3166-2`, `addr:state`, …) → prefixo IBGE → ponto/bbox do way.

### CLI shard

```bash
node extract-geocode-pbf.js G:\sudeste.osm.pbf --out=G:\out \
  --datasets=logradouro --shard-lines=100000
# --shard-datasets=logradouro,bairro,addr   (default se shard-lines>0: logradouro,addr,bairro)
```

Import PHP: `osm:dne:enrich-geo --uf=SP --shard=1` (ver ddsoft).

### Município: o que **não** emitir

- `place=suburb|district` com IBGE de **8–9 dígitos** (código de distrito) — polui o match.
- Só entra IBGE no TXT se tiver **exatamente 7 dígitos** (`municipioIbgeOnly`).

## Colunas (resumo)

Detalhe completo no `README-colunas.md` gerado na saída.

**Estado:**  
`osm_type@osm_id@uf@name@name_norm@lat@lng@lat_min@lat_max@lng_min@lng_max@admin_level@place`

**Município:**  
`osm_type@osm_id@ibge@uf@name@name_norm@lat@lng@lat_min@lat_max@lng_min@lng_max@admin_level@place@source_tag`

**Bairro:**  
`osm_type@osm_id@name@name_norm@uf@city@city_norm@ibge_hint@lat@lng@…@place`

**Logradouro:**  
`osm_id@name@name_norm@kind@uf@city@city_norm@suburb@suburb_norm@postcode@lat@lng@…@way_node_count@name_alt@name_alt_norm@osm_type`

- `kind` (era `highway`): valor de `highway`, ou `square` / `park` para área.
- `name_alt` / `name_alt_norm`: `alt_name`, `short_name`, `old_name`, `loc_name`, `name:pt-BR`, `official_name`, separados por `;`.
- `osm_type`: `way` ou `node` (praça mapeada como ponto → bbox degenerada).
- **Match kind-aware:** candidato `square`/`park` só vale para `TLO_TX` de área (Praça, Largo, Parque, Jardim, Vila, Área). Ver `docs/geo/melhoria-extracao-coordenadas.md` §10.3.

Campos com `@` ou newline no nome são sanitizados (substituídos).

## Two-pass (geometria)

### Por que existe

No PBF típico a ordem é **todos os nodes → ways → relations**.  
Cache LRU de ~500 k nós: quando as ways chegam, os refs já saíram da memória → `logradouroNoGeom` ~99 % e UF=`XX`.

### O que o two-pass faz

| Pass | Ação |
|------|------|
| **1** | Emite estado/município/bairro “fáceis”; **agenda** ways de logradouro (refs) e relations município com `admin_centre`/`label` sem coords no cache |
| **2** | Relê o PBF só para colher lat/lon dos node ids necessários; emite logradouros e municípios pendentes |

Default: two-pass **ligado** se `logradouro` ou `municipio` estiver nos datasets.  
Custo: ~**2×** leitura do arquivo. Memória: set de node ids + lista de ways pendentes (logradouro SE pode ser grande → `NODE_OPTIONS=--max-old-space-size=8192`).

### Contagens úteis no checkpoint

- `municipioPending` / `logradouroPending` — agenda da pass 1  
- `logradouroNoGeom` — ways emitidos sem nenhum nó resolvido  
- `streetCoordsSize` / `neededNodeIds` — cobertura da pass 2  

## Resume e soft-stop — o que é e o que **não** é

### Soft-stop / cancelar (Ctrl+C)

O scan de blobs **cede o event loop** entre blobs (`forEachBlobAsync`), senão o
SIGINT só era processado **depois** de cada wave (re-leitura inteira do PBF).

| Ctrl+C | Efeito |
|--------|--------|
| 1º | Soft-stop: para no **fim do blob atual**, **não inicia** wave/pass2 novas; pendentes da onda atual **não** são gravados |
| 2º | Hard-stop + se ainda não sair, `process.exit(130)` em ~2 s |
| 3º | `process.exit(130)` imediato (terminal continua aberto) |

Grava `extract-checkpoint.json` com `cursor.fileOffset` / `blobIndex` se chegou a flush.

### `--resume`

- Continua a **pass 1** a partir do offset.  
- Abre TXT em **append** (`flags: 'a'`).  
- **Não** reconstrói a lista de ways pendentes da pass 2 a partir do disco.

### Limitações críticas (ler antes de confiar)

| Situação | Comportamento seguro |
|----------|----------------------|
| Soft-stop **antes da pass 2** com logradouro pendente | Pending **não** está serializado. Mensagem: rode de novo **sem** confiar só no resume; ideal **recomeçar do zero** na pasta |
| Soft-stop no meio da pass 2 | Idem: incompleto |
| `--resume` após run que já zerou TXT com datasets diferentes | Risco de **misturar** linhas antigas/novas |
| Rodar **sem** `--resume` | **Apaga todos** os `OSM_*.TXT` da pasta de saída (wipe total) |

### Recomendações operacionais

1. **Pastas separadas** por “produto” do extract:  
   - `G:\osm-geo-se` → estado + município (já usado no enrich de `locais`)  
   - `G:\osm-geo-se-streets` → bairro + logradouro  
2. Para logradouro em máquina de mesa: deixe a job **até o fim** (pass 1 + pass 2).  
3. Melhoria futura: persistir `pendingStreets` / `neededNodeIds` no checkpoint para resume real da pass 2; wipe **só** dos datasets ativos.

## CLI

```bash
node extract-geocode-pbf.js [arquivo.osm.pbf] --out=DIR
  --datasets=estado,municipio,bairro,logradouro
  --addr-points
  --resume | --no-resume
  --node-cache=500000
  --quiet
```

Env: `OSM_PBF_INPUT`, `OSM_GEO_OUT`.

Helpers: `name-norm.js`, `uf-br.js` (bbox SE, prefixo IBGE→UF), `txt-at-writer.js`.

## Testes

- Fixture: `scripts/write-geocode-pbf.js` → `test/fixtures/geocode-mini.osm.pbf`  
- `npm run test:extract` / `test/extract-geocode.test.js`  
- Cobre município IBGE, logradouro em `OSM_LOGRADOURO_SP.TXT`, two-pass geom
