# Coordenadas de bairro e logradouro

> **Atualizado 2026-07-30.** Extract **e** `dne-geo-join.js` estão feitos. O PHP **não casa mais por
> nome**: carrega `DNE_GEO_LOGRADOURO_{UF}.TXT` / `DNE_GEO_BAIRRO_{UF}.TXT` por `log_nu` / `bai_nu`.
> A estratégia antiga (`ufe_sg` + `nome_norm`) **não escala** e não deve rodar contra `streets2`.
> Spec do join: [dne-geo-join.md](./dne-geo-join.md).

## Por que **não** é `UPDATE locais` (ainda)

| Camada | Papel | Bairro / logradouro hoje |
|--------|-------|---------------------------|
| `dne_idx_bairro` / `dne_idx_logradouro` | Índice de **busca** DNE, regenerável | **Sim** — dezenas/centenas de milhares de linhas |
| `locais` | Catálogo global materializado | Quase só até **município**; bairro/logradouro entram no **accept lazy** |
| `locais_tenant` | Endereço operacional do tenant | Número/complemento e materializações privadas |

`locais` **já tem** colunas `lat`/`lng`/bbox, mas **não há cadastro em massa** de tipos 10 e 11.
Gravar geo “em `locais`” para bairro/logradouro exigiria **criar** essas linhas (ou mudar o produto). O caminho alinhado ao modelo atual:

> **Pôr geo no índice DNE** → a busca já lê `dne_idx_*` → depois, no accept, copiar geo para `locais` / `locais_tenant` se quiser.

## Schema do índice

`dne_idx_bairro` (resumo): `bai_nu`, `ufe_sg`, `loc_nu`, `bai_no`, `nome_norm`, faixas CEP, …
`dne_idx_logradouro`: `log_nu`, `ufe_sg`, `loc_nu`, `bai_nu_ini`, `tlo_tx`, `log_no`, `nome_norm`, `cep`, `log_complemento`, …

`lat`/`lng`/bbox **já existem** nas duas tabelas desde a migration `Version20260726000100`.
Especificação de produto: **ddsoft-online** `docs/locais-tenant-e-dne/estrutura-dados-endereco.md` §11.4.
`geo_origem` / `geo_atualizado_em` continuam **não** migrados — e passaram a ser necessários (ver §Match).

## Extract OSM (osm-tools)

### Comando usado (pasta **separada**)

```bash
set NODE_OPTIONS=--max-old-space-size=8192
node extract-geocode-pbf.js G:\sudeste-260725.osm.pbf ^
  --out=G:\osm-geo-se-streets2 ^
  --datasets=logradouro,bairro,addr
```

**Por que pasta separada:** sem `--resume`, o extract **apaga todos** os `OSM_*.TXT` da pasta.
Não misturar com `G:\osm-geo-se` (municípios validados) nem com `G:\osm-geo-se-streets`, que é a
**linha de base** contra a qual os ganhos de 2026-07-30 foram medidos.

### O que saiu de fato (2026-07-30)

`eof: true`, sem soft-stop. Logradouro **1 265 470**, bairro **30 906**, addr **205 660**.

| Arquivo | Linhas | Área (`square` / `park`) | Com `name_alt` | `osm_type=node` |
|---------|-------:|-------------------------:|---------------:|----------------:|
| `OSM_LOGRADOURO_SP` | 793 906 | 17 315 (283 / 17 032) | 80 855 | 16 |
| `OSM_LOGRADOURO_RJ` | 204 235 | 3 509 (193 / 3 316) | 27 047 | 23 |
| `OSM_LOGRADOURO_MG` | 190 674 | 3 066 (76 / 2 990) | 21 404 | 15 |
| `OSM_LOGRADOURO_ES` | 76 653 | 1 090 (21 / 1 069) | 10 601 | 2 |

`OSM_BAIRRO`: 30 906 linhas, **28 206 com geom** — SP 19 178 · MG 3 905 · RJ 3 315 · ES 1 812 · **XX 2 696**.
`OSM_ADDR_POINT_*`: SP 163 212 · RJ 21 122 · ES 13 663 · MG 7 526.

Quatro leituras que mudam decisão:

- **Praça no Brasil é `leisure=park`, não `place=square`** — 17 032 contra 283 em SP. Qualquer filtro por tipo de área tem que aceitar os dois.
- **`name_alt` cobre ~10 % das linhas** (80 855 em SP) e vale **+1,1 pp** de casamento — de graça, é só ler a coluna.
- **Praça como nó é irrelevante** (16 linhas em SP). O caminho existe; não é ele que paga.
- **`OSM_BAIRRO` tem 2 696 linhas em `XX`** e 2 700 sem geom — tratar antes de usar.

Contrato de colunas em [extract-e-artefatos.md](./extract-e-artefatos.md); resume/two-pass idem.

### Resume do extract neste cenário

| | |
|--|--|
| Soft-stop na pass 1 | Checkpoint de offset existe; pending de logradouro **não** serializado |
| Soft-stop antes/durante pass 2 | **Não** resume de forma confiável — recomeçar do zero na pasta de streets |
| Recomendação | Rodar logradouro **até o fim** numa sessão (ou implementar pending em disco depois) |

## Match — o que mudou

A CLI `osm:dne:enrich-geo` **existe** (`src/Command/Osm/OsmDneEnrichGeoCommand.php` +
`src/Osm/DneOsmGeoEnricher.php`, 785 linhas). O problema não é falta de código — é a chave.

### Por que a estratégia original não escala

O enricher casa por `UF|nome_norm` e só aplica quando o nome é **único na UF inteira**
(`count($candidates) !== 1` → `ambiguous++`). O caminho melhor, `UF|loc_nu|nome`, depende de
`addr:city` na way OSM — preenchido em **0,01 %** das linhas (35 em 300 000).

Resultado medido: `Rua Augusta` tem 46 ways em SP e 12 `loc_nu` no DNE; `Avenida Paulista`, 207 ways
e 19 `loc_nu`. Ambas caem em `ambiguous` e ficam sem coordenada — enquanto `Rua Maracujá Natal`,
nome único no estado, casa. **Nome famoso = nome repetido = falha.**

Dentro de **um** município a mesma comparação dá 80,5 % de casamento por nome exato. O que falta
não é regra de nome: é **saber a que município a way pertence**.

### Decisão: o match sai do PHP

O cruzamento passa para **osm-tools** (`dne-geo-join.js`), que resolve o município por *footprint*
bootstrapado e emite `DNE_GEO_LOGRADOURO_{UF}.TXT` — DNE completo + geometria + `geo_status`.
Especificação com processo, contrato de colunas e exemplos: **[dne-geo-join.md](./dne-geo-join.md)**.

O PHP deixa de casar e passa a **carregar por `log_nu`**:

```
antes:  OSM_LOGRADOURO_{UF}.TXT  --(match por nome no PHP)-->  dne_idx_logradouro
depois: OSM_LOGRADOURO_{UF}.TXT  --(join no osm-tools)-->  DNE_GEO_LOGRADOURO_{UF}.TXT
                                                              |
                                                     (UPDATE por log_nu)
                                                              v
                                                     dne_idx_logradouro
```

Por que mover: a chave certa exige um índice espacial de 793 mil ways em memória e duas passadas de
convergência. Isso é trabalho de ferramenta de dados, não de importador — e o resultado é um TXT
auditável e regenerável, no mesmo espírito do resto do pipeline.

### Cobertura esperada (capital, medida)

| Degrau | Acumulado |
|--------|----------:|
| Nome exato | 80,5 % |
| + área `square`/`park` (guardado) | 82,5 % |
| + `name_alt` | 83,7 % |
| + `addr:street` | 83,7 % |
| + núcleo sem tipo | 86,2 % |
| + chave fonética PT-BR | **87,8 %** |

Sem candidato: 12,2 % — vielas, travessas numeradas, loteamento novo. Esses ficam **vazios**, com
`geo_status` explicando. Nada de centroide de bairro/município como preenchimento.

## O que muda no ddsoft

### 1. Perigo imediato se rodar o enricher atual contra `streets2`

O extract novo emite praças e parques no mesmo arquivo. O `DneOsmGeoEnricher` **não lê a coluna 3**
(`kind`) e tem um caminho `byKeyBare` que remove o prefixo de tipo dos dois lados
(`STREET_PREFIXES` inclui `praca`). Consequência medida em SP:

> **2 584 nomes** de área OSM cujo *bare* colide com logradouro **não-área** do DNE.
> `Rua Dois` × `Praça Dois` · `Rua Ipê Roxo` × `Praça Ipê Roxo` · `Rua Dolores Duran` × `Praça Dolores Duran`.

Sem guarda, uma rua recebe a coordenada de uma praça homônima. **Não apontar o enricher atual para
`G:\osm-geo-se-streets2` sem antes aplicar a guarda kind-aware.**

### 2. Caminho atual no `DneOsmGeoEnricher` (ddsoft)

| # | Status | O quê |
|---|--------|--------|
| 1 | **Feito** | `loadPrejoinedLogradouro` — lê `DNE_GEO_LOGRADOURO_{UF}.TXT` por `log_nu`, só `geo_status=ok` |
| 2 | **Feito** | `loadPrejoinedBairro` — lê `DNE_GEO_BAIRRO_{UF}.TXT` por `bai_nu` (com `--uf=`) |
| 3 | **Feito** | Preferência automática: se `DNE_GEO_*` existe na `--dir`, usa prejoined |
| 4 | Legado | Match por nome em `OSM_*` só com `--legacy-match` — **não** apontar a `streets2` |

O caminho por nome **não** recebe guarda kind-aware nem `name_alt`: o join no osm-tools já resolve
isso. Não investir em enriquecer o match PHP.

### 3. `--dataset=bairro` agora tem insumo

`OSM_BAIRRO.TXT` não existia em `G:\osm-geo-se-streets`; agora são 30 906 linhas (28 206 com geom).
`enrichBairro()` pode rodar de verdade — descontando as 2 696 linhas `XX`, que o match por
`UF|nome` descarta sozinho.

### 4. `OSM_ADDR_POINT_*` está populado e sem consumidor

205 660 pontos com `addr:street` + `addr:postcode`. Como fonte de **nome** rende +0,1 pp — quase
nada. O valor real é outro e ainda não foi testado:

- **`addr:postcode` como guarda geográfica** — validar/descartar candidato pelo prefixo do CEP.
- **Nomear way sem `name`** por proximidade do ponto de numeração.
- **Recorte por faixa de numeração** (o seccionamento do DNE), que hoje não existe.

### 5. `geo_origem` / `geo_status` deixaram de ser opcionais

Com 12,2 % de linhas sem candidato e três regras diferentes de casamento (exato, núcleo, fonético),
a tabela precisa dizer **como** cada ponto chegou lá. Sem isso não dá para reprocessar só o que veio
de regra fraca, nem para a busca decidir se mostra o ponto no mapa.

## Depois do match no índice

1. **Busca:** `BuscaEnderecoService` / DTO `LocalEnderecoCandidato` incluir `lat`/`lng` quando `origem=DNE` e índice tiver geo.
2. **Accept / materialização:** ao criar linha em `locais` ou `locais_tenant`, copiar geo do `dne_idx_*` (ou re-match).
3. **Não** preencher número de porta (tipo 12) em massa no global — tenant/GPS.

## O que **não** fazer nesta fase

- Apontar o enricher atual para `streets2` sem a guarda kind-aware (2 584 colisões medidas)
- Preencher coordenada de fallback (centroide de bairro/município) para “não ficar vazio”
- Tratar bairro/logradouro como `osm:locais:enrich-geo` sem materializar
- Truncar IBGE de distrito para “ajudar” município (já corrigido no extract de mun)
- Apagar `G:\osm-geo-se` (municípios) nem `G:\osm-geo-se-streets` (linha de base)
- Confiar em resume mid two-pass sem pending em disco

## Checklist

- [x] Migration `lat`/`lng` + bbox em `dne_idx_bairro` e `dne_idx_logradouro` (`Version20260726000100`)
- [x] Extract `logradouro,bairro,addr` → `G:\osm-geo-se-streets2`
- [x] `DneOsmGeoEnricher` + CLI `osm:dne:enrich-geo`
- [x] `dne-geo-join.js` (footprint + cascata + envelope + exclusão multi-mun)
- [x] Load prejoined por `log_nu` / `bai_nu` no PHP
- [ ] Dry-run SP → apply → métricas de cobertura no MySQL
- [ ] Migration `geo_origem` / `geo_status` / `geo_atualizado_em` (opcional, útil)
- [ ] Docs ddsoft §11 + changelog do apply em volume
- [ ] Expor lat/lng na busca quando o índice tiver geo

## Ligações

- [melhoria-extracao-coordenadas.md](./melhoria-extracao-coordenadas.md) — diagnóstico medido e plano do join
- [estado-atual.md](./estado-atual.md) — o que já funciona em município
- [match-estado-municipio.md](./match-estado-municipio.md) — padrão da CLI de enrich
- ddsoft: `docs/locais-tenant-e-dne/estrutura-dados-endereco.md` (camadas DNE vs `locais`)
- ddsoft: `docs/locais-tenant-e-dne/geo-osm-pipeline.md` (visão produto)
