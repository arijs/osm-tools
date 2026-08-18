# 2026-08-18 — a UF que o retângulo errou, e a âncora de outra cidade

Dois defeitos independentes, os dois achados investigando a mesma queixa no DDSOFT:
o destaque da via não aparecia ao clicar em "Ver no mapa" num endereço de
Patrocínio / MG. Um explica por que **não havia traçado**; o outro, por que o
pouco que havia apontava para **a cidade errada**.

## Prompt

> verifique pra mim o que falta para que o app mostre as vias em destaque quando
> o usuário clica em "ver no mapa" em um endereço, como a Avenida Faria Pereira
> em Patrocínio/MG por exemplo

e, depois de diagnosticado e carregado o dado do lado do DDSOFT:

> Enquanto o outro agente termina no ddsoft-online, podemos fazer o que falta no
> osm-tools

Contexto do lado consumidor:
`ddsoft-online/docs/changelog/2026-08-18-geometria-de-via-mg-atribuicao-de-uf.md`.

---

# Parte 1 — o rótulo de UF discordava do filtro do run

## O defeito

`ufFromPoint` desempata caixas sobrepostas pela de **menor área**, e as caixas se
sobrepõem de propósito. Rodando `--uf=MG`, uma via em Patrocínio (−18,93 / −46,97)
cai na caixa de MG **e** na de GO:

| UF | retângulo | área |
|---|---|---|
| GO | −19,50…−12,40 × −53,25…−45,90 | **52°²** |
| BA | −18,35…−8,53 × −46,62…−37,34 | **91°²** |
| MG | −22,95…−14,20 × −51,05…−39,75 | **99°²** |

GO é menor, GO ganhava. A via era **mantida** no run (`passesUfFilter` aceita
ponto dentro de caixa permitida) e escrita em `OSM_LOGRADOURO_GEOM_GO`. Filtro e
rótulo decidiam por contas diferentes, e o consumidor que lê `..._{UF}` nunca
achava o traçado.

Não é borda: a caixa de GO cobre o Triângulo, o Alto Paranaíba e o Noroeste de
MG; a da BA cobre o norte. Do lado do DDSOFT, **29.505 dos 96.426** ways que o
join de MG referencia ficaram sem polyline, e Patrocínio — cidade inteira de um
cliente — com 3,5% de cobertura real.

## É regressão de 05/08

Antes de `620e02a`, `ufFromPoint` tinha `bboxes = bboxes || UF_BBOX_SE` — só
SP/RJ/MG/ES. Em Patrocínio só MG casava, e o rótulo saía certo: o extract do
Sudeste de 30/07 tem a Av. Faria Pereira (way 154253319) em `OSM_LOGRADOURO_MG`.
Ao trocar o default para a tabela nacional das 27 UFs, GO entrou na disputa e
passou a vencer. A fatia `--only=mg` de 12/08 gravou a mesma via em
`OSM_LOGRADOURO_GEOM_GO`.

## O que foi implementado

`uf-br.js`:

- `pointOfOptions(options)` — extraído de `resolveUf`, sem mudança de comportamento;
- **`resolveUfFiltered(options, ufAllow)`** — tag/IBGE mandam (dado explícito da
  feature; quem descarta é o filtro, não o rótulo); faltando os dois, quem nomeia é
  a **caixa permitida** que contém o ponto, que é o mesmo critério do
  `passesUfFilter`. Sem filtro, comportamento idêntico ao de antes.

`extract-geocode-pbf.js`: as **11** chamadas de `ufBr.resolveUf(...)` que nomeiam
dataset ou preenchem coluna de UF (estado, município, bairro, addr point e
logradouro — nos caminhos de node, way e relation) passam a `resolveUfFiltered(...,
ctx.ufAllow)`. `OSM_LOGRADOURO_GEOM_{UF}` sai do mesmo `uf` do logradouro, então
vem de carona.

## Medição com dado real

Os 29.504 ways de MG que ficaram sem traçado, lidos do próprio artefato em disco e
reclassificados:

| rótulo | antes (`resolveUf`) | depois (`--uf=MG`) |
|---|---|---|
| GO | 18.489 | — |
| BA | 11.007 | — |
| ES | 7 | — |
| MG | 1 | **29.504** |

## Efeito colateral aceito

Num run `--uf=MG`, uma via realmente goiana que caia dentro do retângulo de MG
passa a ser rotulada MG. Ela **já era mantida** no run e já poluía a pasta; o que
muda é o arquivo em que cai. Retângulo não separa MG de GO — o certo é polígono
de UF, e ficou anotado como pendência em `docs/geo/extrair-geom-brasil.md`.

---

# Parte 2 — âncora de outra cidade envenenando a pegada

## O defeito

Fase 2: "nome que existe em um só `loc_nu` do DNE e forma um só cluster no OSM →
aquele cluster é daquele município". A presunção quebra quando o nome é raro **no
DNE** mas existe no OSM em outra cidade. `Catuaí` só aparece em Patrocínio no DNE
de MG e casou com o único cluster homônimo do estado, a ~500 km, em Malacacheta.

Uma âncora errada não erra sozinha: ela alarga a pegada do município, a pegada
passa a aceitar candidatos de lá, e cada acerto falso vira centroide de bairro na
2ª volta e autoriza o próximo. Em Patrocínio, **46 ruas** ficaram com centroide em
outra cidade — entre elas a `Rua Afonso Pena`, com o traçado de Águas Formosas
(530 km), e a `Avenida General Astolfo`, em Belo Horizonte.

O `geo_status` dessas linhas era **`ok`**. É o pior modo de falha do pipeline:
não aparece em nenhum resíduo, e o consumidor desenha no mapa com ar de acerto.

## O que foi implementado

`geo-cluster.js` — **`trimOutliers(points, maxKm, cell)`**: moda espacial. Os
pontos vão para uma grade grossa (`CELL_MASSA = 0,05°` ≈ 5,5 km), ganha a célula
mais povoada, sobrevive quem está a ≤ `maxKm` do centro dela. Média ou centroide
seriam arrastados pelo próprio ponto que se quer expulsar. Nunca devolve vazio.

`dne-geo-join.js`:

- opção **`--ancora-raio-km=60`** (0 desliga). 60 km cabe o maior município de MG
  (raio ~56 km) e o espalhamento de um distrito; não cabe a âncora de outra cidade,
  que erra por centenas de km;
- poda aplicada **antes** de construir a pegada e o centro/raio de diagnóstico —
  os dois são "onde este município fica";
- poda aplicada também na **reconstrução da 2ª volta**, senão um acerto duvidoso da
  1ª volta vira licença para os seguintes;
- log e relatório: pontos podados, localidades afetadas e `ancora_raio_km`.

## Medição: dois re-joins completos

Métrica de qualidade interna — linha `ok` cujo centroide cai a **mais de 60 km da
massa das `ok` do próprio município** (isto é: rua casada em outra cidade):

| | MG antes | MG depois | SP antes | SP depois |
|---|---|---|---|---|
| `geo_status=ok` | 68.453 | 60.038 | 271.692 | 266.480 |
| `ok` a >60 km da massa | **13.444 (19,64%)** | **37 (0,06%)** | **12.429 (4,57%)** | **58 (0,02%)** |
| municípios contaminados | 73 / 73 | 3 / 73 | 212 / 252 | 7 / 252 |
| pontos-âncora podados | — | 1.487 em 73 loc. | — | 2.328 em 212 loc. |

O `ok` cai (−12,3% em MG, −1,9% em SP) porque **o que sai era casamento errado**:
em MG, 13.407 acertos falsos deixaram de existir e o `ok` só encolheu 8.415 — ou
seja, ~5.000 linhas passaram a casar no lugar certo. O resto virou
`fora_do_footprint`, que é ambíguo honesto e continua elegível às recuperações
(envelope, vizinho CEP-5).

Patrocínio, o caso que originou tudo:

| | antes | depois |
|---|---|---|
| linhas `ok` | 887 | 870 |
| `ok` com centroide em outra cidade | **46** | **0** |
| `Rua Afonso Pena` (1096527/1096529) | ok a 530 km, 14 ways | **ok a 1 km, 10 ways** |
| `Rua Marechal Floriano` (1097299) | ambíguo | **ok a 0 km, 5 ways** |
| `Juriti`, `Catuaí`, `Independência`, `General Astolfo`, `Inácio de Oliveira Campos` | ok em outra cidade | ambíguo |
| `Avenida Faria Pereira` (1096495) | ok a 1 km, 21 ways | ok a 1 km, 21 ways (intacta) |

---

## Como foi testado

| suíte | resultado |
|---|---|
| `npm test` (150 testes) | **148 ok, 0 falhas, 2 skip** — os 2 skips são pré-existentes (fixture `.osm` grande ausente) |

Testes novos:

- `test/uf-br.test.js` — 4 casos: o defeito preservado como documentação
  (`resolveUf` devolvendo GO em Patrocínio/Uberlândia e BA em Montes Claros) e a
  correção sob `--uf=MG`; tag e IBGE continuam mandando; sem filtro nada muda e
  ponto fora das caixas permitidas cai no comportamento antigo; `--region=sudeste`
  não achata vizinho legítimo (Franca e Rio Claro seguem SP).
- `test/geo-cluster.test.js` — 3 casos de `trimOutliers`: fica com a massa e
  descarta as duas âncoras distantes; não poda espalhamento legítimo de município
  grande (sede + distrito a 40 km); desligado (`maxKm=0`), degenerado (1 ponto,
  `null`) e nunca vazio.

Validação de campo (fora da suíte, depende de artefatos em `G:`): a
reclassificação dos 29.504 ways de MG e os dois re-joins completos da tabela acima.

## Arquivos

| arquivo | mudança |
|---|---|
| `uf-br.js` | `pointOfOptions`, `resolveUfFiltered`, export |
| `extract-geocode-pbf.js` | 11 chamadas → `resolveUfFiltered(..., ctx.ufAllow)` |
| `geo-cluster.js` | `trimOutliers`, `CELL_MASSA`, exports |
| `dne-geo-join.js` | `--ancora-raio-km`, poda nas duas construções de pegada, log e relatório |
| `test/uf-br.test.js`, `test/geo-cluster.test.js` | testes novos |
| `docs/geo/dne-geo-join.md` | Fase 2 reescrita com a poda e os números |
| `docs/geo/extrair-geom-brasil.md` | seção nova sobre o rótulo de UF por fatia |

Artefatos gerados (fora do git): `G:\dne-geo-mg-join3`, `G:\dne-geo-sp-join3`.
Os anteriores (`G:\dne-geo-mg-join2`, `G:\dne-geo-conectores-fuzzy`) ficaram para
comparação.

## Falta fazer

1. **Recarregar o DDSOFT com o join novo.** O banco hoje tem o join antigo
   (`mg-join2`): geometria e pontos foram carregados de manhã, e as 46 ruas de
   Patrocínio em cidade errada continuam lá. A ordem é `osm:dne:load-via`
   (`geom`, depois `ponto`/`ligacao` regerados por `dne-via-cruzamentos`) e
   `osm:dne:enrich-geo` para os centróides, mais o backfill de `locais_tenant`.
   Vale medir antes/depois do lado do banco, como foi feito hoje.
2. **Polígono de UF**, no lugar do retângulo. A Parte 1 faz o rótulo concordar com
   o filtro do run — o que resolve o pipeline, que sempre roda filtrado —, mas
   `resolveUf` sem filtro continua entregando Patrocínio para GO.
3. **Re-extrair a geometria com o rótulo corrigido** — *em andamento desde 18/08
   14:27* (`extract-brasil-way-geom.js --only=mg --force`). O run já confirma a
   correção: a Av. Faria Pereira (way 154253319) está sendo gravada em
   `OSM_LOGRADOURO_GEOM_MG`, e a pasta de saída não tem mais `..._GO` nem `..._BA`.
   Com a re-extração, a opção `--geom-uf` que se cogitou no loader do DDSOFT deixa
   de ser necessária: quem junta datasets vizinhos era remendo para o rótulo errado.
   Falta ainda re-extrair as demais fatias (SP e as regiões) para o mesmo benefício.
4. **A poda tem um teto conhecido:** se a maioria das âncoras de um município for
   errada, a massa vencedora é a errada. Não foi observado em MG nem em SP (73 e
   252 municípios conferidos), mas a métrica de "ok longe da própria massa" é barata
   e vale virar saída fixa do relatório.
5. **Nada foi commitado.** A árvore já tinha trabalho não commitado antes desta
   sessão (`README.md`, `docs/geo/*`, `extract-geocode-pbf.js`, `package.json`,
   `test/extract-geocode.test.js`, mais os arquivos novos de `via-cruzamentos`), e
   separar o que é de quem é decisão do dono da branch.
