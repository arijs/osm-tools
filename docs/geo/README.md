# Pipeline de geocoding OSM → produto (ddsoft)

Documentação operacional e de desenho do caminho **OpenStreetMap (PBF) → arquivos TXT `@` → coordenadas no banco**.

| Documento | Conteúdo |
|-----------|----------|
| [**proximo-passo-brasil.md**](./proximo-passo-brasil.md) | **Comece aqui** se o extract BR já rodou: flatten → join → load DNE/locais |
| [**extrair-geom-brasil.md**](./extrair-geom-brasil.md) | **Traçado das vias (GEOM)** — script retomável Brasil; o que falta em `G:\` |
| [**estado-atual.md**](./estado-atual.md) | O que já foi feito (Sudeste: estados, municípios, join DNE↔OSM) e números reais |
| [**extract-e-artefatos.md**](./extract-e-artefatos.md) | `extract-geocode-pbf.js`, formatos TXT, two-pass, **resume**, wipe |
| [**match-estado-municipio.md**](./match-estado-municipio.md) | CLI `osm:locais:enrich-geo`, IBGE, lições (distrito 9 dígitos, `admin_centre`) |
| [**bairro-logradouro.md**](./bairro-logradouro.md) | Extract + join feitos; **load PHP** de `DNE_GEO_*` por `log_nu` |
| [**melhoria-extracao-coordenadas.md**](./melhoria-extracao-coordenadas.md) | **Diagnóstico medido** do cruzamento OSM↔DNE: por que falhava, o que foi medido |
| [**dne-geo-join.md**](./dne-geo-join.md) | **Especificação** do `dne-geo-join.js`: processo, contrato, envelope, multi-mun |
| [**geometria-via-destaque.md**](./geometria-via-destaque.md) | **Traçado da via** por id exato: `--way-geom`, coluna `osm_way_ids`, e o roteiro do ddsoft |
| [**via-cruzamentos-densificar.md**](./via-cruzamentos-densificar.md) | **Cruzamentos + densificação** (~111 m) → `DNE_GEO_VIA_PONTO_*` / `VIA_LIGACAO_*` |
| [**cep-externo.md**](./cep-externo.md) | Cache `CEP_EXTERNO.TXT` (AwesomeAPI): formato `@`, dedupe por CEP |
| [**cep-externo-qualidade/**](./cep-externo-qualidade/README.md) | Relatórios de qualidade por bucket de consulta |
| [**operacao-comandos.md**](./operacao-comandos.md) | Receitas de CLI (copiar e colar) |

Plano original (histórico): [../plans/osm-para-locais-geo.md](../plans/osm-para-locais-geo.md).  
Produto / schema de endereço: monorepo **ddsoft-online** → `docs/locais-tenant-e-dne/` (em especial `geo-osm-pipeline.md` e `estrutura-dados-endereco.md` §11).

## Visão em uma frase

1. **Inventário** PBF prova que há matéria-prima (`index-pbf.js`).  
2. **Extract** grava só features úteis em TXT estilo DNE (`extract-geocode-pbf.js`).  
3. **Join** OSM↔DNE no osm-tools (`dne-geo-join.js`) → `DNE_GEO_LOGRADOURO_{UF}.TXT`.  
4. **Load** no ddsoft preenche `dne_idx_*` por `log_nu` / `bai_nu` (sem casar nome).  
5. **Busca** devolve coordenadas no candidato de endereço (ainda parcial).

## Mapa mental das camadas (ddsoft)

```
OSM PBF (osm-tools)
    → OSM_*.TXT  (@)                     ← feito: estado, município, logradouro, bairro, addr
        → locais (estado, município)     ← feito no Sudeste
        → DNE_GEO_LOGRADOURO_{UF}.TXT    ← feito: dne-geo-join.js (SE, 2026-07-30)
            → dne_idx_bairro / dne_idx_logradouro   ← load por log_nu (ddsoft; não casa mais)
                → materialização lazy em locais / locais_tenant no accept
```

**Mudança de desenho (2026-07-30):** o cruzamento OSM↔DNE **sai do PHP**. Casar por nome exige
resolver o município da way (o OSM não traz: `addr:city` em 0,01 % das linhas), e isso é índice
espacial de 793 mil ways — trabalho de ferramenta de dados. O PHP volta a ser importador.

**Regra de ouro:** bairro e logradouro **não** estão cadastrados em massa em `locais` hoje; o índice DNE é a camada certa para geo de busca até a materialização.

## Próximo passo de produto

**Brasil (extract regional já feito):** seguir o passo a passo em
[**proximo-passo-brasil.md**](./proximo-passo-brasil.md) — achatar shards →
`dne-geo-join.js` por UF → `osm:locais:enrich-geo` (admin) + `osm:dne:enrich-geo`
(pasta com `DNE_GEO_*`).

1. Rodar `osm:dne:enrich-geo --dir=G:\dne-geo-br --dataset=logradouro` (ou
   `G:\dne-geo-local` se só Sudeste) — a CLI prefere `DNE_GEO_*` automaticamente.  
2. Migration `geo_origem` / `geo_status` (ainda opcional no schema; útil para reprocessar regra fraca).  
3. Expor lat/lng na busca quando o índice tiver geo.
