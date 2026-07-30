# 2026-07-30 — especificação do `dne-geo-join.js`

## Prompt original

> Precisamos fazer um `dne-geo-join`? Ele já está documentado? Se não, documente o processo que ele
> vai fazer, dando uns exemplos de saída dele

## Resposta às duas perguntas

**Precisa:** sim. O casamento OSM↔DNE não é problema de nome, é de contexto espacial — a way não
diz o município (`addr:city` em 0,01 % das linhas) e há homônimo dentro do mesmo município.

**Estava documentado:** parcialmente, espalhado em §2–§5 de
`melhoria-extracao-coordenadas.md`, e **com uma contradição**: o §4 ainda listava as colunas de
fallback (`geo_origem=municipio`, `geo_metodo=municipio_centroide`, `geo_confianca`) que o §8.1
cortou depois. Quem lesse o §4 implementaria o fallback que foi rejeitado.

## O que foi feito

Novo `docs/geo/dne-geo-join.md` como especificação única: por que existe, entradas/saídas, as 6
fases do processo, contrato de 25 colunas, exemplos de linha, relatório JSON, aceite e limites.
O §4 do documento de melhoria virou ponteiro, resolvendo a contradição.

## Achado durante a redação

Ao gerar exemplos com dados reais (em vez de inventar coordenadas), a agregação por nome dentro do
município produziu **bbox de 28 × 28 km para a Rua Augusta** — são 33 ways homônimas em 4 lugares
diferentes da capital. Com a clusterização da fase 1 (célula 0,02°, componentes conexas por
vizinhança 8), cai para **2 × 3 km**, no cluster de 28 ways da Consolação.

| Nome | Ways | Sem clusterizar | Clusters | Cluster escolhido |
|------|-----:|----------------:|---------:|------------------:|
| `rua augusta` | 33 | 28 × 28 km | 4 | 2 × 3 km |
| `rua estoril` | 5 | 32 × 41 km | 4 | 1 × 1 km |
| `praca da republica` | 23 | 11 × 14 km | 2 | 1 × 1 km |
| `avenida paulista` | 65 | 2 × 2 km | 1 | 2 × 2 km |

Isso promoveu a clusterização de "passo 1 do algoritmo" para **requisito de correção**, e gerou um
critério de aceite novo: nenhuma via com extensão > 15 km sem `geo_kind` de rodovia — o detector de
falha de clusterização.

Duas colunas nasceram daí: `geo_kind` (auditoria — `park` numa Rua é erro) e `osm_clusters` (marca
onde o desempate decidiu; `Rua Estoril` venceu com 2 ways contra três clusters de 1).

## Como foi testado

Documentação, sem código. Verificações:

- **Exemplos são dados reais**, não ilustrativos: campos DNE lidos de `LOG_LOGRADOURO_SP.TXT` /
  `LOG_BAIRRO.TXT` / `LOG_LOCALIDADE.TXT`; geometria calculada de `G:\osm-geo-se-streets2` com a
  clusterização e o centroide ponderado que a spec descreve.
- **Contagem de colunas conferida**: as 7 linhas de exemplo têm exatamente 25 campos, igual ao
  contrato (`awk -F'@'`).
- **Links internos validados**: todos os `](./*.md)` de `docs/geo/` resolvem.

## Arquivos

- `docs/geo/dne-geo-join.md` (novo)
- `docs/geo/melhoria-extracao-coordenadas.md` — §4 vira ponteiro; fallback marcado como cortado
- `docs/geo/README.md`, `docs/geo/bairro-logradouro.md` — ponteiros

## Próximo passo

Implementar. A ordem que a spec sugere: fase 1 (cluster) e fase 3 (footprint) são as que têm risco
de algoritmo; o resto é leitura de arquivo e cascata de string.
