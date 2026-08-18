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

## A UF da fatia manda no nome do arquivo

Cada way é rotulado por `ufBr.resolveUfFiltered`: tag/IBGE primeiro; faltando os dois, o
**polígono da UF** que contém o ponto (`uf-poly.js` + `uf-poly.json`, malha do IBGE
simplificada). Desde 18/08/2026 é o polígono quem decide — e é a mesma conta que
`passesUfFilter` usa para manter ou descartar a feature.

Antes disso decidia retângulo, e as caixas se sobrepõem de propósito:

| UF | área do retângulo | engolia |
|---|---|---|
| GO | 52°² | Triângulo, Alto Paranaíba e Noroeste de MG |
| BA | 91°² | norte de MG |
| MG | 99°² | pedaços de SP, RJ, ES, GO e BA |

Resultado prático da fatia `--only=mg` com o desempate "menor retângulo vence": as vias de
Patrocínio foram escritas em `OSM_LOGRADOURO_GEOM_GO`, e o consumidor que lê `..._MG` não
achava traçado nenhum. No DDSOFT isso deixou **29 505 dos 96 426** ways referenciados pelo
join de MG sem polyline (18/08/2026). O paliativo do mesmo dia — deixar a caixa **permitida**
nomear — consertou Patrocínio e, do outro lado, varreu a vizinhança para dentro do arquivo de
MG: reclassificando o `OSM_LOGRADOURO_MG` daquele run, só **339 065 de 1 007 314** linhas
estavam mesmo em MG.

O que vale para quem opera, agora:

- **A fatia só grava o que é dela.** Feature que o polígono põe fora das UFs permitidas é
  descartada do run — ela aparece na fatia da UF a que pertence (as oito fatias cobrem as 27
  UFs). A pasta ainda pode ganhar um dataset vizinho quando a própria feature traz **tag ou
  IBGE** de outra UF: aí o dado explícito manda, e o arquivo leva o nome certo.
- **Ponto no mar** (píer, plataforma, trecho ao largo) fica fora de todos os polígonos: aí, e
  só aí, o retângulo antigo ainda responde, para que nada que hoje tem rótulo vire `XX`.
- **Ponto exatamente sobre a divisa** pertence às duas UFs; vence a primeira em ordem
  alfabética. Regra arbitrária, mas fixa — o pipeline é retomável e comparado entre execuções.
- **Regerar a malha**: `node scripts/build-uf-poly.js` (baixa do IBGE, simplifica e reescreve
  `uf-poly.json`). O runtime não vai à rede.

---

## Consertar um extract antigo sem re-extrair

O rótulo de UF é campo derivado da coordenada, e a coordenada está em cada linha gravada.
`scripts/relabel-uf.js` relê os artefatos, decide a UF pelo polígono, deduplica a feature que
duas fatias viram e regrava numa pasta nova (mesmo `txt-at-writer`, mesmo `MANIFEST.json`).
Nada é apagado.

```powershell
node scripts/relabel-uf.js --base=G:\osm-geo-br-geom --dry-run          # só as contas
node scripts/relabel-uf.js --base=G:\osm-geo-br-geom --out=G:\osm-geo-br-uf
node scripts/relabel-uf.js --base=... --out=... --only=mg,sp
```

Brasil inteiro em **111 s** (18/08/2026): 4 005 138 linhas de logradouro lidas, 1 204 944
duplicadas, **831 066 mudaram de UF**, 2 800 194 gravadas — e o mesmo para o GEOM, que segue a
UF do irmão em `OSM_LOGRADOURO_{UF}` em vez de decidir pelo primeiro ponto do traçado.

Duas ressalvas:

- Aqui **o polígono decide sozinho** — a linha gravada não guarda as tags, então a precedência
  tag/IBGE do extract não é reproduzível. O rótulo antigo só sobrevive fora de todos os
  polígonos (mar).
- **Não recupera o que o filtro velho nunca deixou entrar.** `passesUfFilter` era retângulo, e
  quatro UFs têm área fora do próprio retângulo: Fernando de Noronha (PE), Trindade e Martim
  Vaz (ES) e faixas de ~8 km (AP) e ~4 km (PI). Só um re-extract com o código novo traz isso.

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
