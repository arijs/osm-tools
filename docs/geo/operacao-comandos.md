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

## Enrich no índice DNE (bairro/logradouro)

> ⚠️ **Não apontar para `G:\osm-geo-se-streets2` ainda.** O extract novo emite praça/parque no mesmo
> arquivo e o `DneOsmGeoEnricher` não lê a coluna `kind`; o caminho `byKeyBare` (que remove `praca`
> do nome) produz **2 584 colisões medidas** em SP — `Rua Dois` recebendo a coordenada de
> `Praça Dois`. Ver [bairro-logradouro.md](./bairro-logradouro.md) §O que muda no ddsoft.

```bash
# seguro hoje: pasta antiga (sem área), só para bairro
php bin/console osm:dne:enrich-geo --dir=G:\osm-geo-se-streets --dataset=logradouro --uf=SP --dry-run

# depois da guarda kind-aware:
php bin/console osm:dne:enrich-geo --dir=G:\osm-geo-se-streets2 --dataset=bairro --dry-run
php bin/console osm:dne:enrich-geo --dir=G:\osm-geo-se-streets2 --dataset=logradouro --uf=SP --dry-run
php bin/console osm:dne:enrich-geo --dir=G:\osm-geo-se-streets2 --dataset=logradouro --uf=SP
```

Opções úteis: `--overwrite` (default é só `lat IS NULL`), `--shard=N` / `--shard-from` / `--shard-to`
(quando o extract roda com `--shard-lines`), `--max-rows` / `--max-seconds`, `--memory=4G`.

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
