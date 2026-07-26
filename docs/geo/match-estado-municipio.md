# Match OSM → `locais` (estado e município)

Código no monorepo **ddsoft-online** (não neste repo):

| Peça | Caminho |
|------|---------|
| CLI | `php bin/console osm:locais:enrich-geo` |
| Enricher | `src/Osm/LocaisOsmGeoEnricher.php` |
| Command | `src/Command/Osm/OsmLocaisEnrichGeoCommand.php` |
| Testes | `tests/Unit/Osm/LocaisOsmGeoEnricherTest.php` |

## Escopo

| `tipo_local` | Tabela | Match | Fonte TXT |
|--------------|--------|-------|-----------|
| 5 Estado | `locais` | `UPPER(sigla)` = UF | `OSM_ESTADO.TXT` |
| 8 Município | `locais` | **`codigo_ibge`** (7 dígitos) | `OSM_MUNICIPIO.TXT` |
| 10 Bairro | — | **não** | ver [bairro-logradouro.md](./bairro-logradouro.md) |
| 11 Logradouro | — | **não** | idem |

Datasets `bairro` / `logradouro` na CLI: **aviso e exit 0** (não grava em `locais`).

## Política de escrita

| Flag | Comportamento |
|------|----------------|
| (default) | Só atualiza se `lat IS NULL` |
| `--overwrite` | Atualiza mesmo com lat preenchida |
| `--dry-run` | Conta matches; não faz `UPDATE` |

Colunas gravadas: `lat`, `lng`, `lat_min`, `lat_max`, `lng_min`, `lng_max`.  
Ainda **não** há `geo_origem` no schema (recomendado em `estrutura-dados-endereco.md` §11.2).

## Município: regras de casamento

### 1. IBGE de município = 7 dígitos

- Aceita só códigos com **exatamente 7** dígitos.  
- 8–9 dígitos = distrito/subdistrito OSM → **ignorados** (`skipped_district_ibge`), nunca truncados para “inventar” município.

### 2. Preferência de feature (score)

Entre várias linhas do mesmo IBGE no TXT: prioriza quem tem lat/lng, `place=city|municipality|town`, `admin_level=8`, preferência leve a `node`.

### 3. Cruzamento nome + UF (geom)

Padrão OSM no Brasil:

- **Relation** admin: tem `IBGE:GEOCODIGO`, muitas vezes **sem** geometria resolvida no extract v1.  
- **Node** `place=city`: tem lat/lng, muitas vezes **sem** IBGE.

O enricher, após ler o arquivo:

1. Indexa nodes de município (`place` city/municipality/town) por `UF|nome_norm`.  
2. Se a feature IBGE não tem lat, copia geom desse índice (`geom_from_name`).

Com extract que resolve `admin_centre` (two-pass), muitas relations já vêm com ponto → `geom_from_name` pode ser 0 e mesmo assim a cobertura é alta.

### 4. Fallback nome em `locais`

Se ainda houver city node no OSM sem IBGE e município em `locais` sem geo: match único por `Encoding::normalizeName` + `id_estado`. Só aplica se **exatamente um** candidato.

## Contadores da mensagem CLI (interpretar)

Exemplo real pós-sucesso:

```text
estado candidates=9 updated=0 already=4 no_geom=5 not_in_db=0
municipio … updated=1038 already=635 matched_ibge=1038 no_geom=95 not_in_db=0
```

| Campo | Significado |
|-------|-------------|
| `updated` | `UPDATE` aplicado (ou contado no dry-run) |
| `already` | Linha em `locais` **já tinha** lat (onlyEmpty) |
| `no_geom` | Feature OSM sem lat após agregação/cruzamento |
| `not_in_db` | IBGE/UF do TXT sem linha em `locais` |
| `matched_ibge` / `matched_name` | Canal que gerou o update |
| `geom_from_name` | Quantos IBGE receberam ponto do node city |
| `skipped_district_ibge` | Linhas com IBGE longo descartadas |

**`updated=0` + `already>0`** nos estados SE **não** é falha: já estavam preenchidos.

## Primeiro run “ruim” (histórico)

| Sintoma | Causa | Correção |
|---------|-------|----------|
| `matched_ibge=23`, `no_geom~1709` | Relations sem geom; city sem IBGE; sem cruzamento | cruzar nome+UF + extract `admin_centre` |
| Pontos estranhos | IBGE 9 dígitos truncado | só 7 dígitos |
| Estado `updated=0` | SE já com lat | contador `already` |

## CLI

```bash
php bin/console osm:locais:enrich-geo --dir=G:\osm-geo-se --dry-run
php bin/console osm:locais:enrich-geo --dir=G:\osm-geo-se
php bin/console osm:locais:enrich-geo --dir=G:\osm-geo-se --dataset=municipio
php bin/console osm:locais:enrich-geo --dir=G:\osm-geo-se --dataset=estado
php bin/console osm:locais:enrich-geo --dir=G:\osm-geo-se --overwrite   # cuidado
```

Encoding dos TXT: **UTF-8** (não passar por latin1 do DNE).

## Idempotência

Reexecutar com onlyEmpty é seguro: não apaga geo existente; só preenche buracos.  
Não há checkpoint por linha (volume de município/estado é pequeno).
