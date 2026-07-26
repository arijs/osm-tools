# Pipeline de geocoding OSM → produto (ddsoft)

Documentação operacional e de desenho do caminho **OpenStreetMap (PBF) → arquivos TXT `@` → coordenadas no banco**.

| Documento | Conteúdo |
|-----------|----------|
| [**estado-atual.md**](./estado-atual.md) | O que já foi feito (Sudeste: estados + municípios) e números reais |
| [**extract-e-artefatos.md**](./extract-e-artefatos.md) | `extract-geocode-pbf.js`, formatos TXT, two-pass, **resume**, wipe |
| [**match-estado-municipio.md**](./match-estado-municipio.md) | CLI `osm:locais:enrich-geo`, IBGE, lições (distrito 9 dígitos, `admin_centre`) |
| [**bairro-logradouro.md**](./bairro-logradouro.md) | **Próxima fase:** onde gravar (`dne_idx_*`), match, riscos |
| [**operacao-comandos.md**](./operacao-comandos.md) | Receitas de CLI (copiar e colar) |

Plano original (histórico): [../plans/osm-para-locais-geo.md](../plans/osm-para-locais-geo.md).  
Produto / schema de endereço: monorepo **ddsoft-online** → `docs/locais-tenant-e-dne/` (em especial `geo-osm-pipeline.md` e `estrutura-dados-endereco.md` §11).

## Visão em uma frase

1. **Inventário** PBF prova que há matéria-prima (`index-pbf.js`).  
2. **Extract** grava só features úteis em TXT estilo DNE (`extract-geocode-pbf.js`).  
3. **Match** no ddsoft preenche `lat`/`lng` (hoje em `locais` até município; bairro/logradouro → `dne_idx_*`).  
4. **Busca** devolve coordenadas no candidato de endereço (ainda parcial).

## Mapa mental das camadas (ddsoft)

```
OSM PBF (osm-tools)
    → OSM_*.TXT  (@)
        → locais (estado, município)     ← feito no Sudeste
        → dne_idx_bairro / dne_idx_logradouro  ← próximo (geo no índice)
            → materialização lazy em locais / locais_tenant no accept
```

**Regra de ouro:** bairro e logradouro **não** estão cadastrados em massa em `locais` hoje; o índice DNE é a camada certa para geo de busca até a materialização.
