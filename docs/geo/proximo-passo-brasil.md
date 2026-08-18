# Próximo passo: extract Brasil → coordenadas DNE (ddsoft)

**Para quem:** você já rodou o extract do Brasil e tem pastas em `G:\`.  
**Objetivo:** obter coordenadas de logradouro/bairro e gravar nas tabelas `dne_idx_*` (e estado/município em `locais`).

Documento **operacional** (copiar e colar). Detalhe de desenho: [dne-geo-join.md](./dne-geo-join.md), [bairro-logradouro.md](./bairro-logradouro.md), [operacao-comandos.md](./operacao-comandos.md).

---

## Onde você está (mapa em 4 caixas)

```
┌─────────────────────┐     ┌──────────────────────┐
│ 1. EXTRACT (feito)  │  →  │ 2. JOIN (feito)       │
│ OSM_*.TXT / shards  │     │ DNE_GEO_*.TXT         │
└─────────────────────┘     └──────────┬───────────┘
                                       │
┌─────────────────────┐     ┌──────────▼───────────┐
│ 4. LOAD (ddsoft)    │  ←  │ 3. CEP externo (opc.) │
│ osm:dne / locais    │     │ AwesomeAPI + cache UF │
└─────────────────────┘     └──────────────────────┘
```

| Passo | Ferramenta | Entrada | Saída |
|------:|------------|---------|--------|
| **1** Extract | `extract-geocode-pbf.js` | `.osm.pbf` | `OSM_ESTADO`, `OSM_MUNICIPIO`, `OSM_LOGRADOURO_{UF}` (flat **ou** shards), `OSM_BAIRRO` |
| **1b** Shard SE | `scripts/shard-osm-txt.js` | flat do Sudeste | shards 20k (igual às outras regiões) |
| **2** Join | `dne-geo-join.js` | DNE oficial + OSM flat **ou** shards | `DNE_GEO_LOGRADOURO_{UF}.TXT` + bairro + relatório |
| **3** CEP externo | `sample-awesomeapi-cep.mjs` | linhas sem geo no join + cache | `CEP_EXTERNO_{UF}.TXT` |
| **4a** Load admin | ddsoft `osm:locais:enrich-geo` | `OSM_ESTADO` / `OSM_MUNICIPIO` | `locais.lat/lng` (estado e município) |
| **4b** Load ruas | ddsoft `osm:dne:enrich-geo` | pasta com `DNE_GEO_*` | `dne_idx_logradouro` / `dne_idx_bairro` |

**Não pule o join.** Não aponte `osm:dne:enrich-geo` direto para pastas `OSM_LOGRADOURO_*` (match por nome no PHP é legado e erra praça↔rua). O caminho certo é **sempre** `DNE_GEO_*` por `log_nu`.

---

## O que você já tem em `G:\` (referência 2026-08)

| Pasta | Conteúdo | Papel |
|-------|----------|--------|
| **`G:\osm-geo-br-uf`** | **as 27 UFs re-rotuladas por polígono** (logradouro, GEOM, addr, bairro) | **Join de qualquer UF — comece por aqui** |
| `G:\osm-geo-br-admin` | `OSM_ESTADO.TXT`, `OSM_MUNICIPIO.TXT` (`eof: true`) | Load em `locais` |
| `G:\osm-geo-br-norte` | shards logradouro/bairro (AC, AM, AP, PA, RO, RR, TO, …) | Join das UFs do Norte |
| `G:\osm-geo-br-nordeste` | shards (AL, BA, CE, MA, PB, PE, PI, RN, SE, …) | Join Nordeste |
| `G:\osm-geo-br-centro-oeste` | shards (DF, GO, MS, MT, …) | Join Centro-Oeste |
| `G:\osm-geo-br-sul` | shards (PR, RS, SC) | Join Sul |
| `G:\osm-geo-br-sudeste` | flat SP/RJ/MG/ES (+ addr); **fatiar** com `shard-osm-txt` | Join Sudeste |
| `G:\dne-geo-local` | `DNE_GEO_*` de **SP, RJ, MG, ES** (run 2026-07-30) | Já pronto para load SE se confiar nesses arquivos |

DNE oficial (latin1) no monorepo:

`D:\dev\ddsoft\ddsoft-online\_ignore\Delimitado\`  
(`LOG_LOCALIDADE.TXT`, `LOG_BAIRRO.TXT`, `LOG_LOGRADOURO_{UF}.TXT` — as 27 UFs).

---

## Passo 0 — Conferir o extract (feito 2026-08-02)

Todas as pastas BR em `G:\` estão **completas** (`extract-checkpoint.json` → `eof: true`). UFs oficiais com `MANIFEST.complete=true` (regiões sharded) ou flat (Sudeste):

| Pasta | eof | Formato | UFs oficiais (linhas logradouro) |
|-------|:---:|---------|----------------------------------|
| `osm-geo-br-admin` | ✅ | flat | estado 169 · município 9 546 |
| `osm-geo-br-norte` | ✅ | shards 20k | AC 10 200 · AP 6 286 · AM 27 697 · PA 22 901 · RO 20 202 · RR 8 567 · TO 51 144 |
| `osm-geo-br-nordeste` | ✅ | shards 20k | AL 31 148 · BA 185 639 · CE 71 189 · MA 54 755 · PB 109 504 · PE 40 637 · PI 61 229 · RN 63 438 · SE 38 064 |
| `osm-geo-br-centro-oeste` | ✅ | shards 20k | DF 34 376 · GO 151 766 · MT 54 343 · MS 74 950 |
| `osm-geo-br-sul` | ✅ | shards 20k | PR 203 609 · RS 140 222 · SC 214 110 |
| `osm-geo-br-sudeste` | ✅ | **flat** (fatiar no Passo 1) | ES ~13 MB · MG ~32 MB · RJ ~35 MB · SP ~138 MB |

**Nota (superada em 18/08/2026):** as pastas regionais têm UFs “vizinhas” a mais, e pior, com o
rótulo errado — o vazamento não era só de sobra, era de nome. Isso foi consertado de uma vez por
[`scripts/relabel-uf.js`](../../scripts/relabel-uf.js), que reclassifica tudo por polígono e
deduplica: **use `G:\osm-geo-br-uf` e ignore a tabela de região por UF.** As pastas regionais
ficam como histórico.

Se reprocessar e `eof` for false: recomece **só essa pasta** (sem `--resume` o extract apaga a `--out` — ver [extract-e-artefatos.md](./extract-e-artefatos.md)).

---

## Passo 1 — Fatiar o Sudeste em shards de 20 000 linhas

As outras regiões já saíram em shards. O Sudeste está em arquivos flat. Para unificar o formato:

```powershell
cd D:\dev\github\osm-tools

# logradouro + bairro + addr → pastas OSM_LOGRADOURO_SP/20000-linhas/…
node scripts/shard-osm-txt.js --dir=G:\osm-geo-br-sudeste --shard-lines=20000

# opcional: apagar os .TXT flat depois de confiar nos shards
# node scripts/shard-osm-txt.js --dir=G:\osm-geo-br-sudeste --shard-lines=20000 --remove-flat --force
```

Flags: `--uf=SP,RJ` (só algumas UFs), `--datasets=logradouro`, `--force` (refaz pasta de shards), `--out=DIR` (escreve noutro lugar).

Estrutura gerada (igual extract com `--shard-lines=20000`):

```text
OSM_LOGRADOURO_SP/
  20000-linhas/
    000001.txt
    000002.txt
    …
  MANIFEST.json
```

O join **lê shards ou flat** sem concatenar. Se **ambos** existirem, **prefere a pasta de shards**.

---

## Passo 2 — Join OSM ↔ DNE (`dne-geo-join.js`)

Gera, por UF:

- `DNE_GEO_LOGRADOURO_{UF}.TXT` — DNE + lat/lng/bbox + `geo_status`
- `DNE_GEO_BAIRRO_{UF}.TXT`
- `DNE_GEO_RELATORIO_{UF}.json`

Aceita `OSM_LOGRADOURO_{UF}.TXT` **ou** `OSM_LOGRADOURO_{UF}/` (shards). Idem `OSM_ADDR_POINT_{UF}`.

### Mapa UF → pasta OSM (shards)

| Região | UFs oficiais | Pasta `--osm=` |
|--------|--------------|----------------|
| Norte | AC AP AM PA RO RR TO | `G:\osm-geo-br-norte` |
| Nordeste | AL BA CE MA PB PE PI RN SE | `G:\osm-geo-br-nordeste` |
| Centro-Oeste | DF GO MT MS | `G:\osm-geo-br-centro-oeste` |
| Sudeste | ES MG RJ SP | `G:\osm-geo-br-sudeste` |
| Sul | PR RS SC | `G:\osm-geo-br-sul` |

### Comando (uma UF)

```powershell
cd D:\dev\github\osm-tools
$env:NODE_OPTIONS = "--max-old-space-size=8192"

$DNE = "D:\dev\ddsoft\ddsoft-online\_ignore\Delimitado"
$OUT = "G:\dne-geo-br"

New-Item -ItemType Directory -Force -Path $OUT | Out-Null

# Exemplo: Bahia (shards em G:\osm-geo-br-nordeste)
node dne-geo-join.js --dne=$DNE --osm=G:\osm-geo-br-nordeste --out=$OUT --uf=BA
```

### Loop: todas as UFs (recomendado)

```powershell
cd D:\dev\github\osm-tools
$env:NODE_OPTIONS = "--max-old-space-size=8192"
$DNE = "D:\dev\ddsoft\ddsoft-online\_ignore\Delimitado"
$OUT = "G:\dne-geo-br"
New-Item -ItemType Directory -Force -Path $OUT | Out-Null

$map = @{
  'G:\osm-geo-br-norte'        = @('AC','AP','AM','PA','RO','RR','TO')
  'G:\osm-geo-br-nordeste'     = @('AL','BA','CE','MA','PB','PE','PI','RN','SE')
  'G:\osm-geo-br-centro-oeste' = @('DF','GO','MT','MS')
  'G:\osm-geo-br-sudeste'      = @('ES','MG','RJ','SP')
  'G:\osm-geo-br-sul'          = @('PR','RS','SC')
}

foreach ($osm in $map.Keys) {
  foreach ($uf in $map[$osm]) {
    $flat  = Join-Path $osm "OSM_LOGRADOURO_$uf.TXT"
    $shard = Join-Path $osm "OSM_LOGRADOURO_$uf"
    if (-not (Test-Path $flat) -and -not (Test-Path $shard)) {
      Write-Warning "sem extract: $uf em $osm — pulando"
      continue
    }
    Write-Host "`n==== JOIN $uf ====" -ForegroundColor Cyan
    node dne-geo-join.js --dne=$DNE --osm=$osm --out=$OUT --uf=$uf
    if ($LASTEXITCODE -ne 0) { throw "join falhou: $uf" }
  }
}
```

**Tempo de referência:** SP ~1 min; UFs menores, segundos. Memória: `--max-old-space-size=8192`.

**Atalho Sudeste:** se `G:\dne-geo-local` já tem `DNE_GEO_*_SP|RJ|MG|ES` e você confia nesse run, **copie** para `G:\dne-geo-br` e só rode o join das demais UFs:

```powershell
Copy-Item G:\dne-geo-local\DNE_GEO_*_SP.* G:\dne-geo-br\ -Force
Copy-Item G:\dne-geo-local\DNE_GEO_*_RJ.* G:\dne-geo-br\ -Force
Copy-Item G:\dne-geo-local\DNE_GEO_*_MG.* G:\dne-geo-br\ -Force
Copy-Item G:\dne-geo-local\DNE_GEO_*_ES.* G:\dne-geo-br\ -Force
```

### Conferir o join

```powershell
Get-Content G:\dne-geo-br\DNE_GEO_RELATORIO_BA.json
# olhe: geo_status.ok, ambiguo, sem_nome_osm
# osm.mode = "shard" | "flat"
```

Cobertura típica no Sudeste (2026-07-30): SP ~77 % ok, capital ~89 %; MG mais baixo (~55 %) por menos ruas nomeadas no OSM — isso é **teto do dado**, não bug do join.

---

## Passo 3 — CEP externo (opcional, resíduo sem geo)

Spec completa: [cep-externo.md](./cep-externo.md).

Para linhas do join com `geo_status` ≠ `ok` (e sem lat), consulta a AwesomeAPI por **CEP**
e grava cache no formato DNE. **Não** reconsulta CEP já cacheado. Uso futuro no join:
`geo_regra=cep_externo` (validar footprint) — hoje o cache é o produto intermediário.

### 3a. Layout do cache (um arquivo por UF)

Canônico na pasta do join:

```text
G:\dne-geo-br\
  DNE_GEO_LOGRADOURO_SP.TXT   …  (fonte de candidatos sem geo)
  CEP_EXTERNO_SP.TXT          …  cache por UF (api_state)
  CEP_EXTERNO_RJ.TXT
  …
  CEP_EXTERNO.TXT             …  monólito legado (opcional; o script ainda lê se existir)
  qualidade\                  …  relatórios de bucket
```

Se ainda tiver só o monólito, fatie uma vez:

```powershell
cd D:\dev\github\osm-tools
node scripts/split-cep-externo-by-uf.mjs --in=G:\dne-geo-br\CEP_EXTERNO.TXT --out=G:\dne-geo-br
# opcional: --remove-source  (apaga o monólito após o split)
```

Referência (split 2026-08-02, ~95 050 CEPs do SE):

| Arquivo | CEPs |
|---------|-----:|
| `CEP_EXTERNO_SP.TXT` | 52 442 |
| `CEP_EXTERNO_RJ.TXT` | 19 833 |
| `CEP_EXTERNO_MG.TXT` | 15 988 |
| `CEP_EXTERNO_ES.TXT` | 6 787 |

### 3b. Consultar novos CEPs (AwesomeAPI)

Requisitos:

- `DNE_GEO_LOGRADOURO_{UF}.TXT` em `--dir` (saída do Passo 2)
- `AWESOMEAPI_API_KEY` em `.env.local` na raiz do osm-tools
- Plano / cota da API (free ~10 k/mês)

```powershell
cd D:\dev\github\osm-tools

# Default: --dir=G:\dne-geo-br
# UFs = todas com DNE_GEO_LOGRADOURO_*.TXT na pasta
# Cache = CEP_EXTERNO_{UF}.TXT na mesma pasta (lê e grava)
node scripts/sample-awesomeapi-cep.mjs --n=1000

# Só algumas UFs
node scripts/sample-awesomeapi-cep.mjs --dir=G:\dne-geo-br --ufs=SP,RJ --n=500

# Sem relatório de qualidade do bucket
node scripts/sample-awesomeapi-cep.mjs --n=1000 --no-quality
```

O script:

1. Lista `DNE_GEO_LOGRADOURO_*.TXT` em `--dir` (ou `--ufs=`).
2. Carrega **todos** os `CEP_EXTERNO_{UF}.TXT` (+ monólito se ainda existir).
3. Coleta linhas sem geo com CEP; **pula** CEPs já no cache.
4. Amostra por relevância (capitais / `sem_nome_osm`), consulta a API.
5. Grava de volta em `CEP_EXTERNO_{api_state}.TXT` (só UFs tocadas).
6. Opcional: qualidade em `G:\dne-geo-br\qualidade\`.

Flags úteis: `--force` (reconsulta tudo), `--retry-errors`, `--concurrency=4`,
`--delay-ms=40`, `--cache-dir=DIR`, `--cache=arquivo.TXT` (modo monólito legado).

### 3c. Qualidade (reprocessar buckets)

```powershell
node scripts/cep-externo-quality.mjs --dir=G:\dne-geo-br
node scripts/cep-externo-quality.mjs --dir=G:\dne-geo-br --last
```

---

## Passo 4 — Importar no ddsoft

### 4a. Estado e município → tabela `locais`

```powershell
cd D:\dev\ddsoft\ddsoft-online

php bin/console osm:locais:enrich-geo --dir=G:\osm-geo-br-admin --dry-run
php bin/console osm:locais:enrich-geo --dir=G:\osm-geo-br-admin
# opcional: só municípios / sobrescrever
# php bin/console osm:locais:enrich-geo --dir=G:\osm-geo-br-admin --dataset=municipio
# php bin/console osm:locais:enrich-geo --dir=G:\osm-geo-br-admin --overwrite
```

Isso **não** preenche bairro/logradouro em `locais` (não há cadastro em massa desses tipos). Geo de rua vai para o **índice DNE**.

### 4b. Logradouro e bairro → `dne_idx_*`

A CLI **prefere** `DNE_GEO_*` na `--dir` e grava por `log_nu` / `bai_nu` (só linhas com `geo_status=ok`).

```powershell
cd D:\dev\ddsoft\ddsoft-online

# Dry-run (todas as UFs com DNE_GEO_* em G:\dne-geo-br)
php bin/console osm:dne:enrich-geo --dir=G:\dne-geo-br --dataset=logradouro --dry-run
php bin/console osm:dne:enrich-geo --dir=G:\dne-geo-br --dataset=bairro --dry-run

# Apply
php bin/console osm:dne:enrich-geo --dir=G:\dne-geo-br --dataset=logradouro
php bin/console osm:dne:enrich-geo --dir=G:\dne-geo-br --dataset=bairro

# Uma UF por vez (se preferir controlar volume)
php bin/console osm:dne:enrich-geo --dir=G:\dne-geo-br --dataset=logradouro --uf=SP
```

Opções úteis: `--overwrite` (default só preenche `lat IS NULL`), `--memory=4G`, `--max-rows` / `--max-seconds`.

**Não use** `--legacy-match` contra extract com praças/parques no mesmo arquivo de logradouro.

### 4c. SQL de conferência (opcional)

```sql
-- Municípios com geo (por UF)
SELECT e.sigla, COUNT(*) AS mun, SUM(m.lat IS NOT NULL) AS com_geo
FROM locais m
JOIN locais e ON e.id = m.id_estado
WHERE m.id_tipo = 8
GROUP BY e.sigla
ORDER BY e.sigla;

-- Logradouros DNE com geo
SELECT ufe_sg,
       COUNT(*) AS total,
       SUM(lat IS NOT NULL) AS com_geo
FROM dne_idx_logradouro
GROUP BY ufe_sg
ORDER BY ufe_sg;

-- Bairros DNE com geo
SELECT ufe_sg,
       COUNT(*) AS total,
       SUM(lat IS NOT NULL) AS com_geo
FROM dne_idx_bairro
GROUP BY ufe_sg
ORDER BY ufe_sg;
```

---

## Checklist mínimo (ordem certa)

1. [x] Extract com `eof: true` em admin + 5 regiões (Passo 0, 2026-08-02)  
2. [x] `shard-osm-txt.js` no Sudeste (`--shard-lines=20000`)  
3. [x] `dne-geo-join.js` para cada UF → `G:\dne-geo-br\DNE_GEO_*`  
4. [x] Relatório join: `G:\dne-geo-br\RELATORIO-JOIN-BRASIL.md`  
5. [x] Cache CEP fatiado: `CEP_EXTERNO_{UF}.TXT` (SP/RJ/MG/ES)  
6. [ ] (Opcional) `sample-awesomeapi-cep.mjs --dir=G:\dne-geo-br --n=…` para novos CEPs  
7. [ ] `osm:locais:enrich-geo --dir=G:\osm-geo-br-admin`  
8. [ ] `osm:dne:enrich-geo --dir=G:\dne-geo-br --dataset=logradouro` (dry-run → apply)  
9. [ ] Idem `--dataset=bairro`  
10. [ ] SQL: `dne_idx_logradouro` com `lat` preenchido nas UFs desejadas  

---

## O que **não** fazer

| Evitar | Por quê |
|--------|---------|
| Rodar `osm:dne:enrich-geo` em pasta só com `OSM_LOGRADOURO_*` | Match por nome; colisão praça↔rua; sem município |
| Concatenar shards em flat “para o join” | Desnecessário — o join lê shards nativamente |
| Join com `--osm` na pasta errada da UF | Use a região oficial da UF (tabela Passo 2) |
| Esperar 100 % de coordenadas | ~12–45 % sem nome no OSM (`sem_nome_osm`) é normal |
| Preencher bairro/logradouro em massa em `locais` | Produto usa índice DNE + materialização lazy no accept |
| Apagar `G:\dne-geo-local` sem backup se for reutilizar SE | É o join bom do Sudeste (2026-07-30) |

---

## Depois (opcional / produto)

| Item | Status |
|------|--------|
| Expor lat/lng na busca de endereço | Parcial / futuro no ddsoft |
| `geo_origem` / `geo_status` no schema MySQL | Ainda sem migration |
| Aplicar `CEP_EXTERNO_*` no join (`geo_regra=cep_externo`) | Cache pronto; fase no join ainda futura |
| Geocoding por **número** de porta | Fora do escopo atual (via inteira) |

---

## Referências rápidas

| Doc | Quando abrir |
|-----|----------------|
| [operacao-comandos.md](./operacao-comandos.md) | Outras receitas CLI |
| [dne-geo-join.md](./dne-geo-join.md) | Como o join escolhe a coordenada |
| [cep-externo.md](./cep-externo.md) | Cache AwesomeAPI por CEP / por UF |
| [bairro-logradouro.md](./bairro-logradouro.md) | Por que load por `log_nu` |
| [extract-e-artefatos.md](./extract-e-artefatos.md) | Formato TXT, resume, wipe |
| [estado-atual.md](./estado-atual.md) | Números reais do Sudeste |
| `G:\dne-geo-br\RELATORIO-JOIN-BRASIL.md` | Totais do join nacional |
