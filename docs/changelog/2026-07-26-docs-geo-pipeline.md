# Changelog: docs do pipeline geo

## Prompt original

Salvar informações críticas (estado Sudeste, extract/resume, match município, próxima fase bairro/logradouro, onde gravar) em docs no repositório, dividindo em subarquivos se fizer sentido.

## O que foi implementado

Nova pasta **`docs/geo/`** no osm-tools:

| Arquivo | Conteúdo |
|---------|----------|
| `README.md` | Índice do pipeline |
| `estado-atual.md` | Números reais SE; o que está feito / não feito |
| `extract-e-artefatos.md` | TXT `@`, two-pass, resume, wipe, pastas |
| `match-estado-municipio.md` | CLI ddsoft, IBGE 7 dígitos, contadores |
| `bairro-logradouro.md` | Próxima fase `dne_idx_*`, checklist |
| `operacao-comandos.md` | Receitas CLI |

Atualizados: `docs/objetivo-e-contexto.md`, `README.md`, ponteiro no `docs/plans/osm-para-locais-geo.md`.

No **ddsoft-online** (mesmo pedido de documentação cruzada):

- `docs/locais-tenant-e-dne/geo-osm-pipeline.md`
- §11.0 de `estrutura-dados-endereco.md` apontando para o doc novo

## Testes

N/A (somente documentação).
