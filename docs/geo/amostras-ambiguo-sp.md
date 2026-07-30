# Amostras do resíduo `ambiguo` — SP

Amostra de 20 linhas por motivo, extraída de `DNE_GEO_RELATORIO_SP.json`
(`ambiguo_exemplos`) da execução de 2026-07-30 sobre `G:\osm-geo-se-streets2`.
Especificação do processo: [dne-geo-join.md](./dne-geo-join.md).

| Motivo | Linhas | % do `ambiguo` |
|--------|-------:|---------------:|
| `fora_do_footprint` | 22 241 | 99,8 % |
| `extensao_longa` | 37 | 0,2 % |
| `empate_de_tamanho` | 3 | 0,01 % |
| **Total** | **22 281** | |

> **Nota de correção.** Uma versão anterior deste levantamento reportava um quarto motivo,
> `conflito_municipio` (8 374 linhas), vindo de uma fase de "âncora local" por bairro/CEP. Aquela
> fase tinha um defeito e foi removida — ver §Como esta amostra derrubou uma feature. Os números
> aqui são os do pipeline atual.

## `fora_do_footprint` — 22 241 linhas (99,8 %)

O nome casou em algum lugar da UF, mas nenhum cluster caiu na pegada de âncoras daquele município.

| `log_nu` | Logradouro | Município | Bairro | CEP | Cand. | km da mancha |
|---|---|---|---|---|---:|---:|
| 1001241 | Rodovia Pedro Eroles | Arujá | Caputera | 07434090 | 1 | 0 |
| 1001242 | Rodovia Pedro Eroles | Arujá | São Bento | 07440550 | 1 | 0 |
| 1001260 | Condomínio Vila Serena | Avaré | Vila Cidade Jardim | 18703182 | 2 | 0 |
| 1001293 | Rua Camilo Chagas | Campinas | Núcleo Residencial São Judas Tadeu | 13059738 | 1 | 0 |
| 1001299 | Caminho de Servidão 3 | Campinas | Chácaras Morumbi | 13059817 | 1 | 0 |
| 1001300 | Caminho de Servidão 4 | Campinas | Chácaras Morumbi | 13059818 | 1 | 0 |
| 1001301 | Caminho de Servidão 5 | Campinas | Chácaras Morumbi | 13059819 | 1 | 0 |
| 1001318 | Rua Paulo Leminski | Campinas | Residencial Campo Florido | 13069432 | 1 | 0 |
| 1001337 | Rua Pedro Pires | Cotia | Cachoeira | 06723010 | 8 | 0 |
| 1001338 | Rua Benedito Alves de Oliveira | Cotia | Nhambuca ( Caucaia do Alto) | 06727620 | 5 | 0 |
| 1001342 | Rua Visconde do Rio Branco | Cotia | Jardim dos Palmares (Caucaia do Alto) | 06728615 | 41 | 0 |
| 1001345 | Rua Eusébio de Queiroz | Cotia | Jardim dos Palmares (Caucaia do Alto) | 06728640 | 2 | 0 |
| 1001346 | Travessa Visconde do Rio Branco | Cotia | Jardim dos Palmares (Caucaia do Alto) | 06728620 | 42 | 0 |
| 1001360 | Viela José Soares de Oliveira | Cotia | Jardim dos Pereiras (Caucaia do Alto) | 06728128 | 8 | 0 |
| 1001366 | Rua Padre Donizetti | Cotia | Graça | 06721530 | 5 | 0 |
| 1001374 | Passagem Caracol | Diadema | Taboão | 09930335 | 1 | 0 |
| 1001483 | Estrada Moinho Velho | Embu das Artes | Jardim Tomé | 06805170 | 1 | 0 |
| 1001484 | Estrada Moinho Velho | Embu das Artes | Moinho Velho | 06805235 | 1 | 0 |
| 1001485 | Estrada Moinho Velho | Embu das Artes | Moinho Real | 06846835 | 1 | 0 |
| 1001534 | Rua da ETE | Guaratinguetá | Jardim Rony | 12506055 | 2 | 0 |

Três padrões saltam da amostra:

1. **`km da mancha = 0` em quase todas.** O candidato está dentro do raio de espalhamento do
   município, só não numa célula ocupada. É buraco na pegada, não rua de outra cidade — coerente com
   a medição geral: 92 % do motivo está a menos de 1 km da mancha.
2. **Distrito longe da sede.** `Caucaia do Alto` aparece 4 vezes: é distrito de Cotia, a ~20 km da
   área urbana principal. As âncoras de Cotia concentram-se na sede, e o distrito fica descoberto.
   O mesmo com `Moinho Velho` em Embu das Artes.
3. **Loteamento novo.** `Residencial Campo Florido`, `Chácaras Morumbi`, `Núcleo Residencial São
   Judas Tadeu` — bairros recentes, com `candidatos=1`, na borda da malha mapeada.

## `extensao_longa` — 37 linhas

Guarda de 15 km: o cluster escolhido ficou grande demais **e** havia mais de um candidato. Pega o
encadeamento do single-link (ver [dne-geo-join.md](./dne-geo-join.md) §Fase 5).

| `log_nu` | Logradouro | Município | CEP | Cand. | Extensão |
|---|---|---|---|---:|---|
| 1029832 | Rua Dois | Guarulhos | 07142203 | 31 | 25 km |
| 1168560 | Rua Dois | Guarulhos | 07273105 | 31 | 25 km |
| 1207758 | Avenida Marginal Rodovia Doutor Manoel Hypólito Rego | Bertioga | 11263005 | 2 | 21 km |
| 1221464 | Rua Dois | Guarulhos | 07155725 | 31 | 25 km |
| 1269610 | Rua Dois | Guarulhos | 07144489 | 31 | 25 km |
| 500075 | Rua Dois | Guarulhos | 07145475 | 31 | 25 km |
| 500191 | Rua Dois | Guarulhos | 07241445 | 31 | 25 km |
| 500256 | Rua Dois | Guarulhos | 07130543 | 31 | 25 km |
| 500288 | Rua Dois | Guarulhos | 07123394 | 31 | 25 km |
| 500337 | Rua Dois | Guarulhos | 07179164 | 31 | 25 km |
| 538830 | Rodovia Raposo Tavares | Ourinhos | 19915022 | 2 | 17 km |
| 538969 | Rodovia Raposo Tavares | Ourinhos | 19912000 | 2 | 17 km |
| 545403 | Rodovia Padre Manoel da Nóbrega | Praia Grande | 11715000 | 2 | 21 km |
| 545451 | Avenida Presidente Castelo Branco | Praia Grande | 11707450 | 2 | 20 km |
| 545452 | Avenida Presidente Castelo Branco | Praia Grande | 11704600 | 2 | 20 km |
| 545453 | Avenida Presidente Castelo Branco | Praia Grande | 11706500 | 2 | 20 km |
| 545454 | Avenida Presidente Castelo Branco | Praia Grande | 11705550 | 2 | 20 km |
| 545455 | Avenida Presidente Castelo Branco | Praia Grande | 11709350 | 2 | 20 km |
| 545816 | Avenida Presidente Castelo Branco | Praia Grande | 11708400 | 2 | 20 km |
| 545914 | Avenida Presidente Castelo Branco | Praia Grande | 11700800 | 2 | 20 km |

Os 37 são de **dois tipos diferentes**, e a amostra mostra que a guarda está pagando um preço:

- **`Rua Dois` em Guarulhos** (10 das 20) — nome genérico espalhado pela cidade, 31 candidatos.
  Rejeição **certa**: não há como saber qual das 31 é.
- **Rodovias e avenidas de orla** (`Raposo Tavares`, `Padre Manoel da Nóbrega`, `Presidente Castelo
  Branco` em Praia Grande) — vias que **realmente têm** 17–21 km, com apenas 2 candidatos.
  Rejeição **provavelmente errada**: a guarda exige `> 15 km` **e** `> 1 candidato`, e essas têm 2.

Vale considerar subir a exigência de candidatos (`> 2`?) ou isentar `kind` de rodovia. Não medido.

## `empate_de_tamanho` — 3 linhas

Dois ou mais clusters no footprint com exatamente o mesmo peso (`way_node_count` somado). São as
únicas linhas que merecem de fato o rótulo "ambíguo". Como são só 3, seguem **todas**:

| `log_nu` | Logradouro | Município | Bairro | CEP | Cand. |
|---|---|---|---|---|---:|
| 507036 | Rua das Rosas | Itapevi | Vila das Flores | 06680661 | 7 |
| 599629 | Travessa José de Oliveira | São Paulo | Vila Malvina | 05203205 | 2 |
| 882757 | Rua Particular | Birigui | Chácara de Recreio Vila Rica | 16208040 | 2 |

`Rua Particular` é um nome que o DNE usa como marcador genérico — não é caso de melhorar o
desempate, é caso de não existir resposta.

## Como esta amostra derrubou uma feature

Ao montar a tabela do então-quarto motivo, `conflito_municipio`, o padrão não fechava:

```
Rua Camilo Chagas    | Campinas | perdeu para Conchal
Caminho de Servidão 3| Campinas | perdeu para Ubatuba      ← 300 km de distância
Rua Pedro Pires      | Cotia    | perdeu para Itu
Rua Serra Azul de Minas | Cotia | perdeu para Guarulhos
```

Disputa de divisa acontece entre vizinhos. Campinas e Ubatuba não são vizinhos.

A causa: a "âncora local" definia a vizinhança como *centroide + raio*, com **raio = distância ao
ponto mais afastado**. Medido em SP, o raio máximo das vias resolvidas de um bairro tem mediana de
**1,3 km** — mas p90 de **344 km**. Basta uma via mal casada no conjunto para o raio explodir e a
vizinhança passar a aceitar qualquer coisa. Na prática o raio mediano em uso era **146 km** e a
distância mediana aceita, **55 km**.

Trocado por vizinho-mais-próximo (imune a outlier), a fase passou a render **36 linhas em 341 813**
— e 34 em MG. Aumentar a tolerância piorava: a 5 km ganhava 491 e revertia 716 por conflito.

Motivo de ser tão pouco: **redundância**. A 2ª volta já recria o footprint incluindo os clusters
resolvidos, com dilatação de ~1,1 km. A vizinhança local cobria quase o mesmo espaço. A fase foi
removida; o comentário no lugar dela em [`dne-geo-join.js`](../../dne-geo-join.js) registra o porquê.

**Lição:** raio de um conjunto não pode ser `max` quando o conjunto pode conter erro. E a validação
que expôs isso não foi um teste — foi olhar 20 linhas de amostra e estranhar o resultado.

## Pista aberta: clusters disputados na linha de base

A investigação deixou um número não explicado: **8 691 clusters (4,07 %) são usados por linhas de
mais de um município** já no pipeline atual, sem nenhuma âncora local. Uma via física pertence a uma
cidade só; no mínimo um desses lados está errado.

Não é o mesmo que o resíduo `ambiguo` — são linhas hoje marcadas como `ok`. Vale um passe de
verificação, com a mesma regra que a fase removida usava no fim: quando dois municípios reivindicam
o mesmo cluster, quem tem a evidência mais forte fica, o outro volta a ficar vazio.
