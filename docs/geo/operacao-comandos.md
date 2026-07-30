# Receitas de operação (comandos)

Caminhos de exemplo no ambiente de desenvolvimento atual; ajuste discos/pastas.

## Inventário PBF (estrutura / sinais)

```bash
cd D:\dev\github\osm-tools
node index-pbf.js G:\sudeste-260725.osm.pbf
# JSON leve por default (sem coordLayout.blocks):
#   --layout-full-blocks
#   --layout-max-blocks=2000
```

## Extract geo → TXT

### Só estado + município (já usado no enrich de `locais`)

```bash
node extract-geocode-pbf.js G:\sudeste-260725.osm.pbf --out=G:\osm-geo-se --datasets=estado,municipio
```

### Logradouro + bairro + addr (pasta **separada**) — usado em 2026-07-30

```bash
set NODE_OPTIONS=--max-old-space-size=8192
node extract-geocode-pbf.js G:\sudeste-260725.osm.pbf --out=G:\osm-geo-se-streets2 --datasets=logradouro,bairro,addr
```

`G:\osm-geo-se-streets` (extract anterior, sem `kind`/`name_alt`/`addr`) é a **linha de base** das
medições — não apagar nem sobrescrever.

### Tudo de uma vez (cuidado com wipe da pasta)

```bash
node extract-geocode-pbf.js G:\sudeste-260725.osm.pbf --out=G:\osm-geo-full --datasets=estado,municipio,bairro,logradouro
```

### Resume (pass 1 apenas confiável)

```bash
node extract-geocode-pbf.js G:\sudeste-260725.osm.pbf --out=G:\osm-geo-se-streets --datasets=bairro,logradouro --resume
```

Se interrompeu **antes da pass 2** de logradouro: preferir **apagar a pasta e recomeçar** (ver [extract-e-artefatos.md](./extract-e-artefatos.md)).

## Enrich no ddsoft (`locais` — estado/município)

```bash
cd D:\dev\ddsoft\ddsoft-online

php bin/console osm:locais:enrich-geo --dir=G:\osm-geo-se --dry-run
php bin/console osm:locais:enrich-geo --dir=G:\osm-geo-se
php bin/console osm:locais:enrich-geo --dir=G:\osm-geo-se --dataset=municipio
php bin/console osm:locais:enrich-geo --dir=G:\osm-geo-se --overwrite   # sobrescreve lat existente
```

## Join OSM ↔ DNE (osm-tools)

```bash
cd D:\dev\github\osm-tools
set NODE_OPTIONS=--max-old-space-size=8192
node dne-geo-join.js --dne=D:\dev\ddsoft\ddsoft-online\_ignore\Delimitado --osm=G:\osm-geo-se-streets2 --out=G:\dne-geo-local --uf=SP
# demais UFs: --uf=RJ | MG | ES
```

Pasta canônica da saída boa (2026-07-30): **`G:\dne-geo-local`**. `G:\dne-geo` é run mais antigo
(mesmos TXT de logradouro/bairro; relatórios menores).

Flags úteis: `--envelope-tol-km=1` (default), `--sem-envelope`, `--sem-exclusao-cluster`, `--quiet`.

## Enrich no índice DNE (bairro/logradouro) — preferir `DNE_GEO_*`

A CLI **prefere** `DNE_GEO_LOGRADOURO_{UF}.TXT` / `DNE_GEO_BAIRRO_{UF}.TXT` na `--dir` (load por
chave). Match por nome em `OSM_*` só com `--legacy-match` — **não** usar contra `streets2`
(2 584 colisões kind medidas).

```bash
cd D:\dev\ddsoft\ddsoft-online

# caminho certo: pasta do join
# sem --uf: processa todas as UFs com DNE_GEO_* na pasta (SP,RJ,MG,ES, …)
php bin/console osm:dne:enrich-geo --dir=G:\dne-geo-local --dataset=logradouro --dry-run
php bin/console osm:dne:enrich-geo --dir=G:\dne-geo-local --dataset=logradouro
php bin/console osm:dne:enrich-geo --dir=G:\dne-geo-local --dataset=bairro

# uma UF só
php bin/console osm:dne:enrich-geo --dir=G:\dne-geo-local --dataset=logradouro --uf=SP --dry-run
```

Opções: `--uf=SP` (omitido = todas), `--overwrite` (default só `lat IS NULL`), `--legacy-match`,
`--max-rows` / `--max-seconds`, `--memory=4G`. Shards (`--shard*`) só no caminho legado OSM.

## Testes

```bash
# osm-tools
cd D:\dev\github\osm-tools
npm run test:extract
npm test

# ddsoft
cd D:\dev\ddsoft\ddsoft-online
php vendor/bin/phpunit tests/Unit/Osm/LocaisOsmGeoEnricherTest.php
```

## Conferência SQL (após enrich município)

```sql
SELECT e.sigla,
       COUNT(*) AS mun,
       SUM(m.lat IS NOT NULL) AS com_geo
FROM locais m
JOIN locais e ON e.id = m.id_estado
WHERE m.id_tipo = 8 AND e.sigla IN ('SP','RJ','MG','ES')
GROUP BY e.sigla;

SELECT id, sigla, lat, lng FROM locais WHERE id_tipo = 5 AND sigla IN ('SP','RJ','MG','ES');
```
