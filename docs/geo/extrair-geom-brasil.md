# Extrair traçado das vias (GEOM) — Brasil inteiro

**Comece aqui** se você precisa de `OSM_LOGRADOURO_GEOM_{UF}` (polyline), não só centróide/bbox.

Script: [`scripts/extract-brasil-way-geom.js`](../../scripts/extract-brasil-way-geom.js)  
Spec do formato: [geometria-via-destaque.md](./geometria-via-destaque.md)  
Extract base: [extract-e-artefatos.md](./extract-e-artefatos.md) · [operacao-comandos.md](./operacao-comandos.md)

---

## Situação em `G:\` (conferido 2026-08-10)

Varredura em **todas** as pastas top-level `G:\osm*` e `G:\dne*`:

| Tipo | Pastas | `OSM_LOGRADOURO_GEOM*` |
|------|--------|------------------------|
| Extract OSM | `osm-geo-br-{admin,norte,nordeste,centro-oeste,sul,sudeste,sp,rj,mg,es}` | **nenhum** |
| Join / CEP | `dne-geo*` (local, br, neuchatel-test, conectores-fuzzy, …) | **nenhum** (só ponto/bbox no `DNE_GEO_*`) |

Os extracts atuais têm `datasets.geom: false` (ou omitido): há `OSM_LOGRADOURO_{UF}` com lat/lng/bbox, **sem** traçado. O código `--way-geom` existe; a corrida nacional **ainda não foi feita**.

---

## O que o script faz

1. Parte o PBF nacional nas **mesmas fatias** da receita Brasil (Norte, Nordeste, Centro-Oeste, Sul; Sudeste por UF ES/MG/RJ/SP).
2. Em cada fatia chama `extract-geocode-pbf.js` com `--datasets=logradouro,geom` (e `--shard-lines=20000`).
3. Grava em pastas **novas** sob `G:\osm-geo-br-geom\` — **não apaga** os `G:\osm-geo-br-*` atuais (só centróide).
4. Progresso em `G:\osm-geo-br-geom\EXTRACT_GEOM_PROGRESS.json`.
5. Fatia já `cursor.eof` + `datasets.geom` + artefato GEOM → **pula** na próxima execução.
6. Ctrl+C → soft-stop do extract (1º); o orquestrador **não inicia** o próximo job. Rode de novo → `--resume` na fatia incompleta.

---

## Uso

```powershell
cd D:\dev\github\osm-tools
$env:NODE_OPTIONS="--max-old-space-size=8192"

# Status (não extrai)
node scripts/extract-brasil-way-geom.js --list

# Brasil inteiro (horas; pode interromper e retomar)
node scripts/extract-brasil-way-geom.js

# Só algumas fatias
node scripts/extract-brasil-way-geom.js --only=sp,rj

# Ver comandos sem rodar
node scripts/extract-brasil-way-geom.js --dry-run --only=es

# Refazer uma fatia já done
node scripts/extract-brasil-way-geom.js --only=es --force

# Também emitir bairro (opcional; mais lento)
node scripts/extract-brasil-way-geom.js --with-bairro
```

### Opções

| Flag | Default | Nota |
|------|---------|------|
| `--pbf=` | `G:\brazil-260724.osm.pbf` | PBF nacional |
| `--out-base=` | `G:\osm-geo-br-geom` | Subpastas: `norte`, `nordeste`, `centro-oeste`, `sul`, `es`, `mg`, `rj`, `sp` |
| `--shard-lines=` | `20000` | Igual aos extracts atuais |
| `--datasets=` | `logradouro,geom` | `geom` liga logradouro automaticamente no extract |
| `--only=` | todas | Ids: `norte`, `nordeste`, `centro-oeste`, `sul`, `es`, `mg`, `rj`, `sp` |
| `--force` | off | Recomeça a fatia (extract **sem** `--resume` → wipe OSM_* **nessa** subpasta) |
| `--wave-nodes=` / `--wave-streets=` | defaults do extract | Se OOM, baixe (ex. `--wave-nodes=6000000`) |

---

## Interromper e retomar

| Momento | O que fazer |
|---------|-------------|
| Ctrl+C **uma vez** no meio do extract | Soft-stop no fim do blob; checkpoint gravado; rode o **mesmo** comando → retoma com `--resume` |
| Ctrl+C **duas/três** vezes | Hard-stop; se a mensagem for *pendentes não gravados*, **apague a subpasta** da fatia e recomece (não use resume) — ver [extract-e-artefatos.md](./extract-e-artefatos.md) |
| Entre fatias | Orquestrador para; `--list` mostra o que falta |

O script detecta `needs-wipe` (`stoppedEarly` + `logradouroPending > 0`) e **bloqueia** em vez de retomar errado.

---

## Saída esperada (por fatia)

```text
G:\osm-geo-br-geom\
  EXTRACT_GEOM_PROGRESS.json
  sp\
    extract-checkpoint.json
    OSM_LOGRADOURO_SP\          # shards (nome/bbox) — regenerado nesta pasta
    OSM_LOGRADOURO_GEOM_SP\     # traçado (shards)  ← o que faltava
    README-colunas.md
  norte\
    …
```

Resumo do extract inclui: `Geom: N vias, M pontos (sem traçado: K)`.

---

## Depois

1. Conferir `--list` → todos `done`.
2. Join / load ddsoft passam a poder resolver `osm_way_ids` → polyline em `OSM_LOGRADOURO_GEOM_*` (roteiro em [geometria-via-destaque.md](./geometria-via-destaque.md) §5).
3. Os `G:\osm-geo-br-*` antigos (sem GEOM) podem continuar servindo o join de coordenadas até você apontar o join/`enrich` para as pastas novas ou mesclar artefatos.
