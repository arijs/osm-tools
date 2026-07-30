# 2026-07-30 — amostras do `ambiguo` e remoção da âncora local

## Prompt original

> Crie um documento novo com um exemplo de amostragem de 20 linhas para cada motivo de ambíguo em SP.

## O que foi entregue

`docs/geo/amostras-ambiguo-sp.md` — 20 linhas por motivo (as 3 do `empate_de_tamanho` são todas),
com a leitura dos padrões de cada grupo.

**Mas a amostra derrubou uma feature no caminho**, e os números que eu havia reportado antes estavam
errados. Isso vai primeiro.

## Correção: a âncora local não valia 4 063 linhas, valia 36

Ao montar a tabela do motivo `conflito_municipio`, o padrão não fechava:

```
Rua Camilo Chagas     | Campinas | perdeu para Conchal
Caminho de Servidão 3 | Campinas | perdeu para Ubatuba     ← 300 km
Rua Serra Azul de Minas | Cotia  | perdeu para Guarulhos
```

Disputa de divisa é entre vizinhos. Campinas e Ubatuba não são.

**Causa:** `centroRaio()` definia a vizinhança local como centroide + raio, com **raio = distância ao
ponto mais afastado**. Medido em SP, o raio máximo das vias resolvidas de um bairro tem mediana de
**1,3 km** mas **p90 de 344 km** — uma via mal casada no conjunto explode o raio e a vizinhança
passa a aceitar qualquer coisa. Na prática o raio mediano em uso era **146 km**, e a distância
mediana aceita, **55 km**.

Corrigido para vizinho-mais-próximo (imune a outlier por construção), a fase rendia:

| Tolerância | Ganho líquido em SP | Revertidos por conflito |
|-----------|--------------------:|------------------------:|
| 1 km (default) | **+36** | 26 |
| 2 km | +89 | 89 |
| 3 km | +195 | 224 |
| 5 km | +491 | 716 |

MG, a UF mais fraca, deu +34. Motivo: **redundância** — a 2ª volta já recria o footprint incluindo
os clusters resolvidos, com dilatação de ~1,1 km, cobrindo quase o mesmo espaço.

**A fase foi removida** (~90 linhas, a flag `--sem-ancora-local`, `--local-tol-km` e a resolução de
conflito). Um comentário no lugar registra a medição para ninguém reimplementar.

### Números corrigidos

| UF | Reportado antes (errado) | Correto |
|----|-------------------------:|--------:|
| SP | 78,3 % | **77,1 %** |
| RJ | 73,4 % | **71,9 %** |
| MG | 57,3 % | **54,7 %** |
| ES | 75,6 % | **74,9 %** |
| Capital | 88,8 % | **88,6 %** |

`docs/geo/dne-geo-join.md` foi corrigido: tabela de resultado, seção do resíduo e o texto que
afirmava que a âncora local "resolveu".

## O documento de amostras

Três motivos, com o que cada amostra revela:

- **`fora_do_footprint` (22 241, 99,8 %)** — `km da mancha = 0` em quase todas: o candidato está no
  raio do município, só não numa célula ocupada. Padrões visíveis na amostra: **distrito longe da
  sede** (`Caucaia do Alto` aparece 4× — é distrito de Cotia a ~20 km da área urbana) e
  **loteamento novo** (`Residencial Campo Florido`, `Chácaras Morumbi`).
- **`extensao_longa` (37)** — a amostra mostra que a guarda pega **dois tipos**: `Rua Dois` em
  Guarulhos com 31 candidatos (rejeição certa) e rodovias/avenidas de orla com 2 candidatos que
  **realmente** têm 17–21 km (`Raposo Tavares`, `Presidente Castelo Branco` em Praia Grande) —
  rejeição provavelmente errada. Registrado como ajuste a considerar, não medido.
- **`empate_de_tamanho` (3)** — as únicas de fato ambíguas; seguem todas no documento.

## Pista aberta registrada

**8 691 clusters (4,07 %) já são usados por linhas de mais de um município** no pipeline atual — e
essas linhas estão marcadas como `ok`, não como resíduo. Uma via física é de uma cidade só. Merece
um passe de verificação.

## Como foi testado

- **Suite completa:** `node --test test/*.test.js` → **87 testes, 85 pass, 0 fail, 2 skip** (os 2
  skip são fixtures grandes, já puladas antes).
- Teste de integração ajustado: a linha `Rua Periferia` da fixture, que antes verificava a
  recuperação pela âncora local, agora verifica que ela fica `ambiguo` **sem coordenada** e que o
  relatório contabiliza `fora_do_footprint = 1`. O teste espelho da flag removida saiu.
- `parseCli` passou a cobrir `--footprint-dilate`.
- As 4 UFs reprocessadas e conferidas contra os números da linha de base.
- Links internos de `docs/geo/` validados.

## Arquivos

- `docs/geo/amostras-ambiguo-sp.md` (novo)
- `docs/geo/dne-geo-join.md` — números corrigidos, seção do resíduo reescrita
- `dne-geo-join.js` — âncora local removida
- `test/dne-geo-join.test.js` — teste ajustado

## Sobra no disco

`G:\dne-geo-local` tem a saída boa. `G:\dne-geo`, `-d2`, `-d3`, `-t2`, `-t3`, `-t5`, `-mg1`, `-mg3`
são execuções de comparação e podem ser apagadas.
