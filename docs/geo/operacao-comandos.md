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

### Brasil inteiro (~2 GB) — fatiar por região / UF

O two-pass agenda **todos** os nós de logradouro num `Set`. No PBF nacional isso
estoura o limite do V8 (`RangeError: Set maximum size exceeded`). O extract agora:

1. **Waves** (default): faz pass2 e libera o `Set` a cada ~8 M nós / 150 k ruas.
2. **`--region` / `--uf`**: grava só a fatia pedida (e ignora cedo ways com
   `addr:state`/IBGE fora da fatia).

Receita recomendada (pastas **separadas** por fatia — cada run dá wipe na `--out`):

```powershell
$env:NODE_OPTIONS="--max-old-space-size=8192"
$PBF="G:\brazil-260724.osm.pbf"
$DS="estado,municipio,bairro,logradouro"

# Admin nacional (leve — sem logradouro, sem wave)
node extract-geocode-pbf.js $PBF --out=G:\osm-geo-br-admin --datasets=estado,municipio

# Regiões (exceto sudeste — este vai por UF)
node extract-geocode-pbf.js $PBF --out=G:\osm-geo-br-norte --datasets=bairro,logradouro --region=norte --shard-lines=20000
node extract-geocode-pbf.js $PBF --out=G:\osm-geo-br-nordeste --datasets=bairro,logradouro --region=nordeste --shard-lines=20000
node extract-geocode-pbf.js $PBF --out=G:\osm-geo-br-centro-oeste --datasets=bairro,logradouro --region=centro-oeste --shard-lines=20000
node extract-geocode-pbf.js $PBF --out=G:\osm-geo-br-sul --datasets=bairro,logradouro --region=sul --shard-lines=20000

# Sudeste por UF (SP é o volume maior)
node extract-geocode-pbf.js $PBF --out=G:\osm-geo-br-es --datasets=bairro,logradouro --uf=ES --shard-lines=20000
node extract-geocode-pbf.js $PBF --out=G:\osm-geo-br-mg --datasets=bairro,logradouro --uf=MG --shard-lines=20000
node extract-geocode-pbf.js $PBF --out=G:\osm-geo-br-rj --datasets=bairro,logradouro --uf=RJ --shard-lines=20000
node extract-geocode-pbf.js $PBF --out=G:\osm-geo-br-sp --datasets=bairro,logradouro --uf=SP --shard-lines=20000
```

Flags úteis: `--wave-nodes=6000000` (mais conservador), `--wave-streets=100000`.
Regiões: `norte`, `nordeste`, `centro-oeste` (aliases `co`, `centrooeste`),
`sudeste` (`se`), `sul`. Dá para combinar: `--uf=SP --region=sul`.

**Nota:** com `--uf`/`--region`, ways **sem** tag de estado ainda entram na agenda
(coords só na pass2); o filtro final descarta o que cair fora do bbox da fatia.
Waves evitam o crash mesmo sem filtro.

### Traçado completo (GEOM) — Brasil

Os extracts acima **não** geram `OSM_LOGRADOURO_GEOM_*` (só centróide/bbox). Para o
polyline, use o orquestrador retomável (doc curta):

→ [**extrair-geom-brasil.md**](./extrair-geom-brasil.md)

```powershell
$env:NODE_OPTIONS="--max-old-space-size=8192"
node scripts/extract-brasil-way-geom.js --list
node scripts/extract-brasil-way-geom.js              # G:\osm-geo-br-geom\
node scripts/extract-brasil-way-geom.js --only=sp    # uma fatia; Ctrl+C e retoma
```

### Cruzamentos + densificação (~111 m)

Com join **26 cols** (`osm_way_ids`) + GEOM da UF:

→ [**via-cruzamentos-densificar.md**](./via-cruzamentos-densificar.md)

```powershell
$env:NODE_OPTIONS="--max-old-space-size=8192"
node scripts/dne-via-cruzamentos.js `
  --dne-geo=G:\dne-geo-conectores-fuzzy `
  --geom=G:\osm-geo-br-geom\sp `
  --out=G:\dne-geo-via-sp `
  --uf=SP

# piloto RMSP:
node scripts/dne-via-cruzamentos.js `
  --dne-geo=G:\dne-geo-conectores-fuzzy `
  --geom=G:\osm-geo-br-geom\sp `
  --out=G:\dne-geo-via-rmsp `
  --uf=SP `
  --bbox=-47.20,-24.05,-46.30,-23.20
```

Saída: `DNE_GEO_VIA_PONTO_{UF}.TXT`, `DNE_GEO_VIA_LIGACAO_{UF}.TXT`, relatório JSON.

### Load via no ddsoft (GEOM + pontos + ligações)

Não usa `osm:dne:enrich-geo` (só centróide). Comando irmão:

```powershell
cd D:\dev\ddsoft\ddsoft-online
php bin/console doctrine:migrations:migrate
php bin/console osm:dne:load-via `
  --dir=G:\dne-geo-conectores-fuzzy `
  --geom-dir=G:\osm-geo-br-geom\sp `
  --via-dir=G:\dne-geo-via-rmsp `
  --uf=SP --dataset=all
# --dataset=geom|ponto|ligacao  --dry-run
```

Doc: [geometria-via-destaque.md §5](./geometria-via-destaque.md), [via-cruzamentos-densificar.md](./via-cruzamentos-densificar.md).

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

## Cache CEP externo (AwesomeAPI)

Cache canônico: **`CEP_EXTERNO_{UF}.TXT`** em `G:\dne-geo-br` (um arquivo por UF).

```bash
cd D:\dev\github\osm-tools

# fatiar monólito legado → por UF
node scripts/split-cep-externo-by-uf.mjs --in=G:\dne-geo-br\CEP_EXTERNO.TXT --out=G:\dne-geo-br

# consulta só CEPs ainda não cacheados (chave em .env.local)
# --dir = pasta com DNE_GEO_LOGRADOURO_* + CEP_EXTERNO_*
node scripts/sample-awesomeapi-cep.mjs --dir=G:\dne-geo-br --n=1000
node scripts/sample-awesomeapi-cep.mjs --dir=G:\dne-geo-br --ufs=SP,RJ --n=500
```

Spec: [cep-externo.md](./cep-externo.md). Reexecução **não** reconsulta CEP já presente.

Após cada lote: `G:\dne-geo-br\qualidade\`. Reprocessar:

```bash
node scripts/cep-externo-quality.mjs --dir=G:\dne-geo-br
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

### Brasil (pós-extract por região)

O join lê **flat ou shards** (`OSM_LOGRADOURO_{UF}/` + `MANIFEST.json`). Prefere shards se a
pasta existir. Sudeste flat → fatiar com:

```bash
node scripts/shard-osm-txt.js --dir=G:\osm-geo-br-sudeste --shard-lines=20000
```

Passo a passo (Passo 0 conferido + loop 27 UFs + load):  
[**proximo-passo-brasil.md**](./proximo-passo-brasil.md).

## Enrich no índice DNE (bairro/logradouro) — preferir `DNE_GEO_*`

A CLI **prefere** `DNE_GEO_LOGRADOURO_{UF}` / `DNE_GEO_BAIRRO_{UF}` na `--dir` (load por
`log_nu` / `bai_nu`). Aceita **flat** `.TXT` **ou** pasta fatiada (`KEY/20000-linhas/…` +
`MANIFEST.json`), igual ao extract. Match por nome em `OSM_*` só com `--legacy-match` (ou se
não houver `DNE_GEO_*`) — **não** usar legado contra extract com praças no mesmo arquivo.

```bash
cd D:\dev\ddsoft\ddsoft-online

# caminho certo: pasta do join (Brasil)
# sem --uf: processa todas as UFs com DNE_GEO_* na pasta
php bin/console osm:dne:enrich-geo --dir=G:\dne-geo-br --dataset=logradouro --dry-run
php bin/console osm:dne:enrich-geo --dir=G:\dne-geo-br --dataset=logradouro
php bin/console osm:dne:enrich-geo --dir=G:\dne-geo-br --dataset=bairro

# uma UF só
php bin/console osm:dne:enrich-geo --dir=G:\dne-geo-br --dataset=logradouro --uf=SP --dry-run

# legado OSM fatiado (sem DNE_GEO): shards nativos
php bin/console osm:dne:enrich-geo --dir=G:\osm-geo-br-norte --dataset=logradouro --uf=AM --legacy-match
php bin/console osm:dne:enrich-geo --dir=G:\osm-geo-br-norte --dataset=bairro --legacy-match
```

Opções: `--uf=SP` (omitido = todas detectadas), `--overwrite`, `--legacy-match`,
`--max-rows` / `--max-seconds`, `--memory=4G`, `--shard` / `--shard-from` / `--shard-to`.

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
