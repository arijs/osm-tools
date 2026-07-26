# Plano: próxima etapa do geocoder — OSM Sudeste → `locais` lat/lng

> **Atualização 2026-07-26:** a documentação viva e operacional do pipeline (estado real do Sudeste, resume, bairro/logradouro, comandos) está em **[docs/geo/](../geo/README.md)**. Este arquivo permanece como plano histórico de desenho da fase 2a.

## Onde estamos

### Inventário PBF (feito)

Run completo de `G:\sudeste-260725.osm.pbf` (~813 MB) gerou stats/results ~**11 MB** cada (quase tudo é `coordLayout.blocks` × ~17 598 segmentos).

| Sinal | Ordem de grandeza (resultados reais) |
|-------|--------------------------------------|
| Nodes com lat/lon | **~141 milhões** |
| `addr:street` | ~472 k |
| `addr:housenumber` | ~398 k |
| `addr:city` | ~361 k |
| `addr:postcode` | ~305 k |
| `name` | ~1,8 M |
| `place` | ~83 k |
| Layout | `pctSmallJumps ≈ 0,93` → coords **bem sequenciais** no arquivo |
| Bbox header | lon ~−53…−28, lat ~−25…−14 (Sudeste BR) |

Conclusão: o extract **tem matéria-prima de geocoding**. A fase inventário cumpriu o papel.

### Destino no produto (ddsoft)

Doc: `D:\dev\ddsoft\ddsoft-online\docs\locais-tenant-e-dne\estrutura-dados-endereco.md`.

- Árvore canônica: **Município → Bairro → Logradouro → Número → Complemento**
- **Hoje em `locais`:** na prática só há cadastro materializado até **município** (e estado/país). **Bairro e logradouro ainda estão só em `dne_idx_*`** (lazy materialization no accept).
- Portanto o enrich geo **fase 2a** atualiza só `id_tipo` 5 e 8 em `locais`.
- Colunas `lat`/`lng`/`*_min`/`*_max` já existem e estavam vazias.
- Chaves de casamento: `codigo_ibge` (município), `sigla` (UF), `nome` (fallback).
- CLI: `php bin/console osm:locais:enrich-geo --dir=…`
- Doc §11.2 recomenda `geo_origem` / precisão **antes** de UPDATE agressivo (ainda não migrado).

### Preferência de artefato intermediário

**CSV/TXT delimitados no modelo DNE**, **não** SQLite:

| Princípio DNE | Como aplicamos no extract OSM |
|---------------|-------------------------------|
| Arquivos texto por “dataset” | Um arquivo por nível/tipo de feature |
| Logradouro **por UF** (`LOG_LOGRADOURO_{UF}.TXT`) | **`OSM_LOGRADOURO_{UF}.TXT`** (SP/RJ/MG/ES + residual XX) |
| Delimitador `@` | Mesmo `@` (campos vazios = string vazia; parser faz `null`) |
| UTF-8 (ou latin1→utf8 no import) | **UTF-8** no extract (OSM já é unicode) |
| Cabeçalho fixo documentado | Primeira linha = nomes de colunas **ou** README de colunas (como LOG_*) |
| Regenerável | Re-rodar extract sobrescreve o diretório de saída |
| Consumido por PHP no ddsoft | Reutilizar padrão `TxtReader` / `RowParser` (split `@`) do pacote DNE; match logradouro com `--uf=` |

Motivo: o ddsoft já opera com `_ignore/Delimitado/*.TXT` e importadores que leem linha a linha com checkpoint; SQLite exigiria runtime extra no host e foge do fluxo mental do time.

---

## Meta da próxima etapa (fase 2a)

**Povoar coordenadas em `locais` a partir do OSM Sudeste**, por nível, com casamento determinístico e rastreável:

| Nível `tipo_local` | O que gravar | Fonte OSM típica |
|--------------------|--------------|------------------|
| **5 Estado** | `lat`/`lng` centróide + bbox | `boundary=administrative` + `admin_level=4` / `place=state` + `ref`/nome UF |
| **8 Município** | centróide + bbox | `admin_level=8` / `place=municipality|city` + **`IBGE:GEOCODIGO` / `ref:IBGE`** → `locais.codigo_ibge` |
| **10 Bairro** | centróide + bbox (quando houver polígono/ponto) | `place=suburb|neighbourhood|quarter` + nome + município |
| **11 Logradouro** | centróide (ou ponto médio) + bbox do traçado | `highway=*` + `name` (+ opcional `addr:street` em nodes) |

**Não** nesta etapa: número/complemento em massa no global (ficam tenant/GPS); geocoder full-text; sobrescrever pontos de alta confiança sem `geo_origem`.

---

## Por que o JSON tem ~10 MB

Quase todo o tamanho é `coordLayout.blocks` (milhares de blocos com bbox/first/last/jumps). Útil para layout, **não** é o payload do geocoder.

| Ação | Efeito |
|------|--------|
| Manter samples (512) + sequence | leve, suficiente para “sequencial vs random” |
| Downsample blocks no save (ex. merge para ≤2 k) ou `--layout-blocks=0` | stats de MBs → centenas de KB |
| Extrair geocode **para TXT `@`**, não inflar stats | pipeline limpo |

Polish no `index-pbf` **junto** ou **logo após** a fase 2a.

---

## Arquitetura da fase 2a

```
                    ┌─────────────────────┐
  sudeste.osm.pbf ──┤ extract-geocode-pbf │──► _ignore/osm-geo/*.TXT  (@)
                    └─────────────────────┘     (um arquivo por dataset)
                              │
                              │ match keys (IBGE, nome_norm, UF)
                              ▼
                    ┌─────────────────────┐
     MySQL locais ──┤ osm:locais:enrich-geo│──► UPDATE lat/lng/*_min/*_max
     (+ dne_idx)    └─────────────────────┘     + geo_origem='osm' (se existir)
                              │
                              ▼
                    audit-match-*.CSV  (matched / ambiguous / unmatched)
```

Dois processos:

| Passo | Onde | Linguagem |
|-------|------|-----------|
| **A** Extract | **osm-tools** | Node (reuso `pbf-reader` + protos) |
| **B** Match+UPDATE | **ddsoft-online** | PHP (espelhar `dne:locais:enrich-cep` / `TxtReader`) |

---

## Passo A — Extração OSM → TXT `@` (osm-tools)

Novo script **`extract-geocode-pbf.js`** (independente do inventário; reutiliza `pbf-reader` + protos).

Saída sugerida (espelhando `_ignore/Delimitado/`):

```text
out/osm-geo/   (ou path configurável)
  OSM_ESTADO.TXT
  OSM_MUNICIPIO.TXT
  OSM_BAIRRO.TXT                 # nacional/recorte; opcional split por UF se crescer
  OSM_LOGRADOURO_SP.TXT          # um arquivo por UF (igual DNE)
  OSM_LOGRADOURO_RJ.TXT
  OSM_LOGRADOURO_MG.TXT
  OSM_LOGRADOURO_ES.TXT
  OSM_LOGRADOURO_XX.TXT          # residual: uf desconhecida / não resolvida
  OSM_ADDR_POINT_SP.TXT          # opcional / amostrado — também por UF se existir
  README-colunas.md              # contrato de colunas (igual espírito do DNE)
```

**Logradouro por UF (obrigatório):** igual `LOG_LOGRADOURO_{UF}.TXT` do DNE — um `source_key` por estado, import/match retomável por UF, e arquivos menores no disco.

### Formato de linha

- Delimitador: **`@`**
- Encoding: **UTF-8**
- Campo vazio: string vazia entre `@` (parser → `null`)
- Sem aspas (nomes OSM com `@` são raríssimos; se aparecer, substituir ou rejeitar a linha com log)
- **Sem** header embutido **ou** header na linha 1 documentado no README — **decidir uma convenção e fixar** (recomendação: **sem header**, contrato no README, igual muitos LOG_*; facilita append/resume)

### Esquemas de colunas (contrato)

**`OSM_ESTADO.TXT`** — prioridade alta, poucas linhas (4 UFs SE + extras se houver)

```
osm_type@osm_id@uf@name@name_norm@lat@lng@lat_min@lat_max@lng_min@lng_max@admin_level@place
```

- `uf`: derivado de `ISO3166-2=BR-SP` → `SP`, ou `ref`, ou mapa nome→sigla

**`OSM_MUNICIPIO.TXT`** — prioridade **máxima** (ouro = IBGE)

```
osm_type@osm_id@ibge@uf@name@name_norm@lat@lng@lat_min@lat_max@lng_min@lng_max@admin_level@place@source_tag
```

- `ibge`: `IBGE:GEOCODIGO` ou `ref:IBGE` (7 dígitos quando possível)
- `source_tag`: qual tag gerou o ibge (auditoria)

**`OSM_BAIRRO.TXT`**

```
osm_type@osm_id@name@name_norm@uf@city@city_norm@ibge_hint@lat@lng@lat_min@lat_max@lng_min@lng_max@place
```

- Contexto de município fraco no OSM (`addr:city`, `is_in`, tags do relation pai) — match no PHP usa nome+UF+município

**`OSM_LOGRADOURO_{UF}.TXT`** (segmentado por estado — espelho do DNE)

```
osm_id@name@name_norm@highway@uf@city@city_norm@suburb@suburb_norm@postcode@lat@lng@lat_min@lat_max@lng_min@lng_max@way_node_count
```

- Só ways com `highway=*` **e** `name` (ou `name:pt`)
- **Um arquivo por UF:** `OSM_LOGRADOURO_SP.TXT`, `_RJ`, `_MG`, `_ES` (Sudeste); residual `OSM_LOGRADOURO_XX.TXT` se a UF não puder ser resolvida
- Como atribuir UF na extração (ordem):
  1. tags do way (`addr:state`, `is_in:state`, …) se existirem
  2. heurística por **bbox do way** vs bbox das UFs do recorte (Sudeste: 4 retângulos)
  3. se ainda ambíguo → `XX` + contagem no log do extract
- Centróide v1: média dos nós do way **quando** resolvíveis na mesma passada; senão bbox dos nós densos vistos + centro da caixa
- Match no ddsoft: processar **um UF por vez** (`--uf=SP`), checkpoint por arquivo — mesmo contrato mental de `dne:idx:import --dataset=logradouro --uf=SP`

**`OSM_ADDR_POINT_{UF}.TXT`** (opcional, fase 2a.1 ou 2b)

```
osm_id@lat@lng@street@street_norm@housenumber@city@suburb@postcode@name
```

- Também segmentado por UF se for gerado
- **Não** popula `locais` tipo 12 em massa; serve validação e futuro tenant/GPS assistido

### O que extrair da primitiva OSM

| Dataset | Filtro |
|---------|--------|
| Estado | `admin_level=4` + boundary, ou `place=state` |
| Município | `admin_level=8` + boundary, ou `place` ∈ {city, municipality, town} com IBGE se possível |
| Bairro | `place` ∈ {suburb, neighbourhood, quarter} |
| Logradouro | `highway=*` + `name` |
| Addr point | node com `addr:street` + lat/lon |

Geometria v1 (pragmática):

| Tipo | lat/lng | bbox |
|------|---------|------|
| node | o próprio ponto | min=max = ponto |
| way | média dos nós (ou centro do bbox) | min/max dos nós |
| relation multipolygon | centro do bbox dos membros way | union dos bboxes dos ways |

**Risco ways:** centróide de street pode exigir 2ª passada (ids de nós → coords). Mitigações v1:

1. **Single-pass com cache LRU** de nodes recentes (layout sequencial ~93% ajuda).
2. Ou emitir só **bbox parcial** + `way_node_count` e aceitar centróide = centro da caixa.
3. Segunda passada só se cobertura de logradouro ficar ruim.

Tamanho esperado: **ordens de magnitude menor** que 141 M nodes (só features nomeadas / com addr). Municípios SE ~1,6 k linhas; estados 4; bairros dezenas/centenas de k; logradouros potencialmente centenas de k–baixo milhão de linhas TXT (ainda legível e streamável).

### CLI do extract (esboço)

```bash
node extract-geocode-pbf.js G:\sudeste-260725.osm.pbf \
  --out=G:\osm-geo-se \
  --datasets=estado,municipio,bairro,logradouro
# opcional: --addr-points --resume --max-seconds=N
# logradouro: sempre grava OSM_LOGRADOURO_{UF}.TXT (SP/RJ/MG/ES + XX)
```

- Soft-stop Ctrl+C + checkpoint (byte offset no PBF), no espírito do inventário e do `dne:idx:import`
- Handles de escrita: um stream/append por UF aberta (4+1 arquivos); flush no checkpoint
- Testes com fixture `tiny.osm.pbf` + admin mínimo

---

## Passo B — Match + UPDATE (ddsoft, PHP)

Novo comando no espírito de `dne:locais:enrich-cep`:

```bash
php bin/console osm:locais:enrich-geo \
  --dir=_ignore/osm-geo \
  --dataset=municipio \
  --dry-run
php bin/console osm:locais:enrich-geo --dataset=municipio --apply

# logradouro: um UF por vez (espelha dne:idx:import --uf=SP)
php bin/console osm:locais:enrich-geo --dataset=logradouro --uf=SP --dry-run
php bin/console osm:locais:enrich-geo --dataset=logradouro --uf=SP --apply
```

Reutilizar (ou copiar padrão de):

- `TxtReader` + split `@`
- Checkpoint opcional se arquivos forem grandes (logradouro)
- Encoding UTF-8 (sem latin1)

### Estratégia de match (ordem)

| Nível | Estratégia |
|-------|------------|
| Estado | `sigla` = `uf` do TXT; fallback `nome_norm` |
| Município | **`codigo_ibge` = `ibge`** (ouro); senão `nome_norm` + `id_estado` |
| Bairro | `nome_norm` + `id_municipio`; desempate por overlap de bbox se ambos tiverem caixa |
| Logradouro | `nome_norm` + `id_municipio` (+ bairro se `suburb` bater); normalizar tipo (“Rua X” vs “X”) |

### Política de escrita (doc §11.2)

- Só preencher se `lat`/`lng` **NULL** **ou** `geo_origem` ∈ {null, centroid, osm} com precisão pior
- Nunca sobrescrever `geo_origem=GPS` (quando a coluna existir)
- Preencher bbox em município/bairro/logradouro; ponto = centróide
- Log de match: `matched` / `ambiguous` / `unmatched` por nível → **também TXT `@` ou CSV de auditoria**

### Schema geo no ddsoft (recomendado antes do apply em massa)

Migration (MWB + drift):

- `geo_origem` (enum: GPS / osm / geocoder / centroid / manual / …)
- `geo_precisao_m`
- `geo_atualizado_em`

Sem isso, o UPDATE ainda é possível (colunas lat/lng já existem), mas a política de sobrescrita fica cega.

---

## Passo C — Consumo na busca (depois da cobertura aceitável)

- `BuscaEnderecoService` / DTO: devolver `lat`/`lng` + `geo_nivel` (município vs logradouro)
- Herança na árvore se folha sem ponto (já previsto no doc §11.2)

---

## Ordem de implementação recomendada

### 0. Docs (primeiro — regra do projeto)

- `docs/plans/osm-para-locais-geo.md` neste monorepo (cópia deste plano, já com CSV `@`)
- Atualizar `objetivo-e-contexto.md` com fase 2a e ponte para `locais`
- Nota: stats ~10 MB = `coordLayout.blocks`, não payload de geo
- Opcional no ddsoft: parágrafo em `estrutura-dados-endereco.md` §11 apontando o pipeline OSM → TXT → enrich

### 1. Reduzir ruído do inventário (quick win, osm-tools)

- `--layout-max-blocks=N` ou não gravar blocks por default no results
- Blocks completos só com `--layout-full-blocks`

### 2. Extract geocode PBF → TXT `@` (osm-tools)

- `extract-geocode-pbf.js` + README de colunas
- Normalização de nome (lower, sem acento) **no extract** (`name_norm`) para o match PHP ficar simples
- Prioridade de datasets: **município (IBGE) → estado → logradouro → bairro**
- Testes unitários + fixture PBF mínima

### 3. Schema `geo_*` no ddsoft (se ainda não houver)

- Migration + labels + drift

### 4. Match + UPDATE (ddsoft)

- CLI dry-run → apply
- Relatório: % municípios SP/RJ/MG/ES com geo

### 5. API / UI

- Coords no candidato de busca
- Mapa depois de taxa de match aceitável em município (e idealmente logradouro)

---

## Matching: o que vai funcionar primeiro

| Fácil / alto valor | Difícil |
|--------------------|---------|
| Município via **IBGE** no OSM | Bairro com nome ambíguo / grafia DNE ≠ OSM |
| Estado via UF | Logradouro sem bairro no OSM |
| Bounding box de relation admin | Ways que cruzam município |
| Fallback centróide município para mapa nunca vazio | Número de porta (ficar em tenant/GPS) |

Validar no extract quantos `admin_level=8` / `place=*` têm tag IBGE no Sudeste (contagem no próprio extract).

---

## Critérios de sucesso (fase 2a)

1. ≥ **90%** dos municípios de SP/RJ/MG/ES em `locais` com `lat`/`lng` (e preferencialmente bbox) via IBGE/OSM.
2. Estados SE com centróide/bbox.
3. Amostra de logradouros (ex. capital) com ponto/bbox e relatório de unmatched.
4. Dry-run auditável; zero sobrescrita de dados GPS.
5. Artefatos intermediários = **TXT `@` documentados**, regeneráveis, sem SQLite.
6. Docs cruzadas osm-tools ↔ ddsoft (`estrutura-dados-endereco.md` §11).

---

## Riscos

| Risco | Mitigação |
|-------|-----------|
| OSM sem IBGE em muitos municípios | Fallback nome+UF; lista manual dos faltantes |
| Nome logradouro diverge do DNE | `nome_norm` + tipo; score; relatório unmatched |
| Geometry de relation complexa | bbox first; centróide = centro da caixa na v1 |
| 10 MB stats confunde com “dados de geo” | Separar inventário vs extract; downsample blocks |
| Ways precisam de todos os nodes | LRU / bbox-only v1; 2ª pass só se necessário |
| `@` dentro de nome OSM | Sanitizar/rejeitar linha; logar contagem |
| Arquivo logradouro enorme | **Split obrigatório por UF** (`OSM_LOGRADOURO_{UF}.TXT`); stream line-by-line no PHP; match com `--uf=` |
| Way sem UF resolvível | Bucket `XX` + relatório; reprocessar com bbox UF melhorada |

---

## Não fazer agora

- Geocoder completo offline  
- Preencher todo número de porta no global  
- SPATIAL index (só se aparecer consulta por raio)  
- Reprocessar planet inteiro em XML  
- **SQLite / banco local no osm-tools** como artefato de entrega  
- Unificar CLIs inventário + extract numa só (podem compartilhar libs)

---

## Resposta direta: “qual a próxima etapa?”

1. **Congelar a fase inventário** (já prova que o PBF Sudeste serve).  
2. **Extrair** do PBF só features administrativas + ruas nomeadas → **arquivos TXT delimitados com `@`** (modelo DNE); logradouro **já partido por UF** (`OSM_LOGRADOURO_SP.TXT` etc.).  
3. **Casar** com `locais` (IBGE no município primeiro; logradouro com `--uf=`) via CLI PHP no ddsoft.  
4. **UPDATE** `lat`/`lng`/`*_min`/`*_max` com procedência OSM (`geo_origem` se já existir).  
5. **Expor** na busca de endereço.

O inventário + `coordLayout` respondem *onde no arquivo* e *se é sequencial*; o extract TXT + match respondem *qual linha do banco recebe qual ponto*.
