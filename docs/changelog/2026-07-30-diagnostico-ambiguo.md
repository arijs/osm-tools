# 2026-07-30 — diagnóstico do resíduo `ambiguo` do `dne-geo-join`

## Prompt original

> Já tem um plano para o ambiguo? Mostre uns exemplos

## Resposta curta

**Não havia plano** — a spec só documentava a ordem de desempate (bairro → tamanho → `ambiguo`).
Agora há, medido.

## O que foi feito

### 1. Instrumentação (código)

`dne-geo-join.js` passou a registrar no relatório JSON por que cada linha virou `ambiguo`:

- `ambiguo_por_motivo` — contagem por causa
- `ambiguo_distancia_ate_a_mancha` — distância do melhor candidato à mancha de âncoras do município
- `ambiguo_exemplos` — até 15 amostras por causa, com nome, localidade, bairro, CEP e nº de candidatos

Também entrou `--footprint-dilate=N` (default 1, comportamento inalterado) para poder **testar** a
hipótese óbvia em vez de assumi-la.

### 2. Descoberta: `ambiguo` não é ambiguidade

| Motivo | Linhas (SP) | |
|--------|------------:|--|
| `fora_do_footprint` | 22 241 | **99,8 %** |
| `extensao_longa` | 37 | |
| `empate_de_tamanho` | 3 | |

O rótulo engana: a esmagadora maioria é "o nome casou, mas nenhum cluster caiu na pegada do
município" — não "dois candidatos igualmente bons".

E dessas, **93 % têm o melhor candidato a menos de 1 km da mancha do próprio município**. São
buracos na pegada (loteamento novo, periferia sem âncora), não rua de outra cidade. Só 1 068 estão a
mais de 20 km — essas a rejeição está certa.

Exemplos: `Rua Camilo Chagas` (Campinas, Núcleo Residencial São Judas Tadeu, `candidatos=1`),
`Rua Paulo Leminski` (Campinas, Residencial Campo Florido), `Caminho de Servidão 3` (Campinas,
Chácaras Morumbi). Bairro de loteamento recente com candidato único é o padrão.

### 3. Hipótese testada e **descartada**: dilatar o footprint

| Dilatação | `ok` | Clusters usados por 2+ municípios |
|-----------|-----:|----------------------------------:|
| 1 | 263 478 (77,1 %) | 8 691 (4,07 %) |
| 2 | 266 957 (78,1 %) | 12 143 (5,74 %) |
| 3 | 269 154 (78,7 %) | 14 575 (6,98 %) |

**+3 479 linhas resolvidas custam +3 452 clusters disputados** entre municípios. Razão 1:1 — cada
resposta nova vem junto com uma suspeita. Halo uniforme não separa "periferia da própria cidade" de
"cidade vizinha"; em região metropolitana são a mesma direção. Default fica em 1.

### 4. Plano proposto: âncora local por bairro/CEP

Testar o candidato contra as vias **já resolvidas que compartilham `bai_nu` ou prefixo de CEP** —
vizinhança do tamanho de um bairro, não halo de cidade. Aplicabilidade medida nas 22 281 ambíguas:

| Sinal | Cobertura |
|-------|----------:|
| ≥1 vizinha resolvida no mesmo CEP-5 | **98,0 %** |
| ≥1 vizinha resolvida no mesmo bairro | 92,3 % |
| os dois | 92,1 % |

Média de ~94 vias resolvidas por CEP-5. É a mesma lógica da pesquisa manual do §9 de
`melhoria-extracao-coordenadas.md`.

**Não implementado** — o plano vem com gate: medir a mesma razão ganho/risco da tabela de dilatação
antes de ligar. Se cada linha nova continuar trazendo um cluster disputado, o resíduo fica vazio.

## Como foi testado

- `npm run test:join` → **18 testes, 18 pass, 0 fail**. A instrumentação não muda o caminho de
  decisão; `--footprint-dilate` default 1 mantém o comportamento anterior (SP continua em 263 478
  `ok`, idêntico à execução anterior).
- Medições rodadas na base real de SP: decomposição por motivo, faixas de distância, e a tabela de
  ganho×risco comparando três execuções completas (`G:\dne-geo`, `G:\dne-geo-d2`, `G:\dne-geo-d3`).
- Risco medido contando geometrias idênticas reivindicadas por `loc_nu` diferentes na saída.

## Arquivos

- `dne-geo-join.js` — diagnóstico no relatório + `--footprint-dilate`
- `docs/geo/dne-geo-join.md` — nova seção "Plano para o resíduo `ambiguo`"

## Sobra no disco

`G:\dne-geo-d2` e `G:\dne-geo-d3` são as saídas dos experimentos de dilatação (~65 MB cada).
Podem ser apagadas; ficaram para conferência.
