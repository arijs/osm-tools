# 2026-07-30 — docs atualizados após o extract `streets2`

## Prompt original

> Vamos atualizar estes documentos (e outros que precisarem), refletindo os novos dados extraídos,
> e vendo como os scripts de importação no ddsoft precisam ser atualizados

Referindo-se a `docs/plans/osm-para-locais-geo.md` e `docs/geo/bairro-logradouro.md`.

## O que foi feito

Documentação, sem alteração de código. Seis arquivos atualizados com os números reais de
`G:\osm-geo-se-streets2` e com a análise do que muda no importador PHP.

| Arquivo | Mudança |
|---------|---------|
| `docs/geo/bairro-logradouro.md` | Reescrito: extract passa de "próxima fase" para **feito**, com volumes reais; nova seção **Match — o que mudou** (por que a chave `UF\|nome` não escala) e **O que muda no ddsoft** (5 mudanças no `DneOsmGeoEnricher`) |
| `docs/geo/estado-atual.md` | Nova seção da fase concluída (logradouro/bairro/addr); tabela "não está feito" refeita; 4 lições novas |
| `docs/geo/README.md` | Diagrama de camadas com o join no osm-tools; ponteiro para `melhoria-extracao-coordenadas.md` |
| `docs/geo/operacao-comandos.md` | Comando do extract novo; **aviso** de não apontar o enricher para `streets2` sem a guarda |
| `docs/geo/extract-e-artefatos.md` | Contrato de colunas com `kind`/`name_alt`/`osm_type` (já feito no changelog anterior) |
| `docs/plans/osm-para-locais-geo.md` | Nota do que o plano **errou**, medido — fica como registro histórico |

## Levantamento no ddsoft

Lidos `src/Command/Osm/OsmDneEnrichGeoCommand.php` e `src/Osm/DneOsmGeoEnricher.php` (785 linhas).

### Achado bloqueante

O enricher **não lê a coluna 3** (era `highway`, agora `kind`) e tem o caminho `byKeyBare`, que
remove o prefixo de tipo dos dois lados — `STREET_PREFIXES` inclui `praca`. Com praças e parques
agora no mesmo arquivo, isso colide:

> **2 584 nomes** de área OSM cujo *bare* bate com logradouro **não-área** do DNE em SP.
> `Rua Dois` × `Praça Dois` · `Rua Ipê Roxo` × `Praça Ipê Roxo` · `Rua Dolores Duran` × `Praça Dolores Duran`.

Rodar `osm:dne:enrich-geo --dir=G:\osm-geo-se-streets2` hoje grava coordenada de praça em rua.
Registrado como aviso em três documentos.

### Achado de desenho

O enricher casa por `UF|nome_norm` e só aplica quando o nome é único na UF inteira
(`count($candidates) !== 1` → `ambiguous++`). O caminho bom, `UF|loc_nu|nome`, depende de
`addr:city` na way — preenchido em **0,01 %** das linhas. Por isso `Avenida Paulista` (207 ways,
19 `loc_nu`) fica sem coordenada enquanto `Rua Maracujá Natal` casa.

Decisão documentada: **o match sai do PHP**. O `dne-geo-join.js` (osm-tools) resolve o município por
footprint e emite `DNE_GEO_LOGRADOURO_{UF}.TXT`; o PHP passa a carregar por `log_nu`. Motivo: a
chave certa exige índice espacial de 793 mil ways e duas passadas de convergência — ferramenta de
dados, não importador.

### Mudanças listadas para o `DneOsmGeoEnricher`

1. Ler coluna 3 (`kind`) e classificar `square`/`park` como área
2. Guarda kind-aware (área só casa com `TLO_TX` de área) — **bloqueante**
3. Ler `name_alt`/`name_alt_norm` (colunas 17/18) — +1,1 pp de graça
4. Preferir `osm_type=way` sobre `node` no desempate
5. Depois do join: trocar match por load de `DNE_GEO_LOGRADOURO_{UF}.TXT`

Mais: `--dataset=bairro` agora tem insumo (30 906 linhas, antes zero); `OSM_ADDR_POINT_*` está
populado (205 660) e sem consumidor; `geo_origem`/`geo_status` deixaram de ser opcionais.

## Como foi testado

Documentação — sem código alterado, sem suite para rodar. Verificações feitas:

- **Números conferidos na fonte**, não copiados: contagens por UF, `kind`, `name_alt` e `osm_type`
  lidas de `G:\osm-geo-se-streets2`; colisões `byKeyBare` medidas reimplementando o
  `STREET_PREFIXES` do PHP contra `LOG_LOGRADOURO_SP.TXT`.
- **Links internos validados**: todos os `](./*.md)` em `docs/geo/` resolvem para arquivo existente.
- `git status` conferido: nenhum arquivo de código ou fixture tocado nesta alteração.

## Arquivos alterados

- `docs/geo/bairro-logradouro.md`
- `docs/geo/estado-atual.md`
- `docs/geo/README.md`
- `docs/geo/operacao-comandos.md`
- `docs/plans/osm-para-locais-geo.md`

## Próximo passo

`dne-geo-join.js` — footprint municipal bootstrapado + cascata determinística (87,8 % medidos na
capital). Antes dele, a guarda kind-aware no PHP, que é o que destrava rodar contra `streets2`.
