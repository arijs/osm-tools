# Melhoria extração coordenadas

Precisamos investigar e ver um jeito de melhorar o cruzamento dos logradouros no PBF do OSM com a base do DNE

Atualmente a combinação dos dados extraídos do OSM com a base do DNE é feita em PHP diretamente no app consumidor:

Import PHP: `osm:dne:enrich-geo --uf=SP --shard=1` (ver ddsoft).

Eu quero alterar pra fazermos algo mais genérico, algo que consiga combinar o máximo possível, senão todos, os logradouros entre o OSM e o DNE, para que assim todos os logradouros do DNE tenham coordenadas e bounding boxes.

A ideia é combinar a leitura das duas fontes de dados em TXT e gerar uma nova, contendo todos os dados de logradouros do DNE mais as coordenadas e os bounding boxes extraídos do OSM.

Aqui temos uma pasta com os dados extraídos do OSM:
G:\osm-geo-se-streets

Aqui temos uma pasta com a base inteira do DNE:
D:\dev\ddsoft\ddsoft-online\_ignore\Delimitado

Veja o arquivo Leiautes_delimitador-utf8.txt .

Abaixo vou dar um exemplo que a combinação foi encontrada:

```json
{
	"codigo_dne": "603661",
	"nivel": "logradouro",
	"nome": "Maracujá Natal",
	"tipo_logradouro": "Rua",
	"log_complemento": null,
	"cep": "04863010",
	"uf": "SP",
	"municipio_nome": "São Paulo",
	"id_municipio": "4558",
	"distrito_nome": null,
	"bairro_nome": "Vila Natal",
	"lat": -23.764714735714,
	"lng": -46.707315571429,
	"lat_min": -23.7678506,
	"lat_max": -23.7633246,
	"lng_min": -46.7116332,
	"lng_max": -46.7042099,
	"dne": {
		"log_nu": 603661,
		"bai_nu": 26895,
		"loc_nu": 9668,
		"ufe_sg": "SP"
	}
}
```

Mas abaixo, há vários exemplos de endereços não encontrados, inclusive algumas das avenidas mais famosas do país.

```json
{
	"codigo_dne": "591259",
	"nivel": "logradouro",
	"nome": "Estoril",
	"tipo_logradouro": "Rua",
	"log_complemento": null,
	"cep": "04773090",
	"uf": "SP",
	"municipio_nome": "São Paulo",
	"id_municipio": "4558",
	"distrito_nome": null,
	"bairro_nome": "Veleiros",
	"lat": null,
	"lng": null,
	"lat_min": null,
	"lat_max": null,
	"lng_min": null,
	"lng_max": null,
	"dne": {
		"log_nu": 591259,
		"bai_nu": 26543,
		"loc_nu": 9668,
		"ufe_sg": "SP"
	}
}
```

```json
{
	"codigo_dne": "580665",
	"nivel": "logradouro",
	"nome": "Augusta",
	"tipo_logradouro": "Rua",
	"log_complemento": "- até 697 - lado ímpar",
	"cep": "01305000",
	"uf": "SP",
	"municipio_nome": "São Paulo",
	"id_municipio": "4558",
	"distrito_nome": null,
	"bairro_nome": "Consolação",
	"lat": null,
	"lng": null,
	"lat_min": null,
	"lat_max": null,
	"lng_min": null,
	"lng_max": null,
	"dne": {
		"log_nu": 580665,
		"bai_nu": 25422,
		"loc_nu": 9668,
		"ufe_sg": "SP"
	}
}
```

```json
{
	"codigo_dne": "608719",
	"nivel": "logradouro",
	"nome": "Paulista",
	"tipo_logradouro": "Avenida",
	"log_complemento": "- até 610 - lado par",
	"cep": "01310000",
	"uf": "SP",
	"municipio_nome": "São Paulo",
	"id_municipio": "4558",
	"distrito_nome": null,
	"bairro_nome": "Bela Vista",
	"lat": null,
	"lng": null,
	"lat_min": null,
	"lat_max": null,
	"lng_min": null,
	"lng_max": null,
	"dne": {
		"log_nu": 608719,
		"bai_nu": 25243,
		"loc_nu": 9668,
		"ufe_sg": "SP"
	}
}
```

```json
{
	"codigo_dne": "363200",
	"nivel": "logradouro",
	"nome": "Atlântica",
	"tipo_logradouro": "Avenida",
	"log_complemento": "- até 1020 - lado par",
	"cep": "22010000",
	"uf": "RJ",
	"municipio_nome": "Rio de Janeiro",
	"id_municipio": "3969",
	"distrito_nome": null,
	"bairro_nome": "Copacabana",
	"lat": null,
	"lng": null,
	"lat_min": null,
	"lat_max": null,
	"lng_min": null,
	"lng_max": null,
	"dne": {
		"log_nu": 363200,
		"bai_nu": 12182,
		"loc_nu": 7043,
		"ufe_sg": "RJ"
	}
}
```

```json
{
	"codigo_dne": "691763",
	"nivel": "logradouro",
	"nome": "do Contorno",
	"tipo_logradouro": "Avenida",
	"log_complemento": "- até 1191 - lado ímpar",
	"cep": "30110001",
	"uf": "MG",
	"municipio_nome": "Belo Horizonte",
	"id_municipio": "3036",
	"distrito_nome": null,
	"bairro_nome": "Centro",
	"lat": null,
	"lng": null,
	"lat_min": null,
	"lat_max": null,
	"lng_min": null,
	"lng_max": null,
	"dne": {
		"log_nu": 691763,
		"bai_nu": 3305,
		"loc_nu": 2754,
		"ufe_sg": "MG"
	}
}
```

---

# Plano de implementação

## 1. Diagnóstico — medido, não suposto (2026-07-30)

Sondagens rodadas sobre `G:\osm-geo-se-streets` + `Delimitado` (SP como amostra):

| Medida | Valor |
|--------|------:|
| Ways OSM em `OSM_LOGRADOURO_SP.TXT` | 776 591 |
| Ways com `city` preenchido | **35 em 300 000** (0,01 %) |
| Ways com `suburb` preenchido | 168 em 300 000 (0,06 %) |
| Ways com `postcode` preenchido | 60 em 300 000 (0,02 %) |
| Linhas DNE `LOG_LOGRADOURO_SP` | 341 813 (252 `loc_nu`) |
| Nomes OSM distintos (SP) | 293 592 |
| Nomes com bbox agregado > ~20 km (homônimos espalhados) | 23 083 |
| **Teto de cobertura só por nome exato** | **75,8 %** das linhas DNE (259 202/341 813) |

Sondagem dos casos citados acima:

| Nome | Ways no OSM/SP | `loc_nu` no DNE/SP | Extensão do conjunto |
|------|---------------:|-------------------:|----------------------|
| `rua augusta` | 46 | 12 | −23,7…−20,0 lat (estado inteiro) |
| `avenida paulista` | 207 | 19 | −23,7…−20,2 lat |
| `rua estoril` | 12 | 8 | −23,7…−20,0 lat |
| `avenida do contorno` | 78 | 7 | −23,5…−19,8 lat |

**A causa da falha não é ausência de dado — é ambiguidade.** Avenida Paulista *está* no OSM; só que "avenida paulista" também está em outros 18 municípios de SP e a way não carrega município nenhum. O match atual (PHP) acerta `Rua Maracujá Natal` porque esse nome é único no estado, e erra as avenidas famosas porque nome famoso = nome repetido. Qualquer melhoria começa por **atribuir município à way**.

Duas descobertas que economizam trabalho:

- `OSM_MUNICIPIO.TXT` tem 2 904 linhas, 1 768 IBGE distintos, 1 673 com ponto — e **zero bbox real** (`lat_min == lat_max` em 100 % das linhas). Não há polígono nem extensão municipal disponível hoje.
- `LOG_VAR_LOG` (denominações alternativas) rende **3** nomes que casam com o OSM em SP inteiro. Não vale implementar variantes.

## 2. Estratégia: footprint municipal por bootstrapping

Não baixar malha do IBGE, não fazer point-in-polygon, não rodar mais um passe no PBF. Os próprios dados já resolvem: **os nomes únicos ancoram o mapa dos municípios, e o mapa resolve os nomes ambíguos.**

1. Nome que existe em **um só** `loc_nu` do DNE e forma **um só** cluster no OSM → âncora: sabemos que aquele punhado de ways está naquele município.
2. As âncoras de um município viram seu *footprint* (grade de células ocupadas).
3. Nome ambíguo → escolhe-se o cluster que cai dentro do footprint do município que o DNE indica.

Viabilidade medida em SP:

| Medida | Valor |
|--------|------:|
| Nomes-âncora | 126 096 |
| Municípios/distritos com footprint | **252 de 252 (100 %)** |
| Pontos-âncora por município | p10 = 27 · p50 = 320 · p90 = 1 585 |
| Municípios com < 20 âncoras (precisam de fallback) | 12 |

Ou seja: o footprint sai de graça e cobre todo mundo. Só 12 municípios ficam ralos.

## 3. Algoritmo (uma passada por UF, em memória)

```
carrega DNE (latin1)  → LOG_LOCALIDADE, LOG_BAIRRO, LOG_LOGRADOURO_{UF}
carrega OSM (utf8)    → OSM_LOGRADOURO_{UF}.TXT, indexado por name_norm
  ↓
1. clusteriza ways por nome           (grid 0,02° + vizinhança 8 → componentes)
2. âncoras: nome 1:1 DNE↔cluster      → cluster recebe loc_nu
3. footprint por loc_nu               (grade 0,01° ≈ 1,1 km, dilatada 1 célula)
4. resolve ambíguos                   (cluster com maior fração dentro do footprint)
5. repete 3–4 uma vez                 (footprint cresce com o que foi resolvido)
6. agrega e emite                     (bbox união do cluster; centro ponderado por way_node_count)
7. fallback em cascata                (bairro → município), com origem/precisão marcadas
```

Detalhes que não são óbvios:

- **Chave de nome DNE:** `norm(TLO_TX + ' ' + LOG_NO)` **e** `norm(LOG_NO)` — o OSM às vezes traz o tipo, às vezes não. Reusar [`name-norm.js`](../../name-norm.js) sem alterar.
- **Distritos:** `LOC_IN_TIPO_LOC != 'M'` não tem `MUN_NU`; herdam o footprint do `LOC_NU_SUB`. Foram 1 470 das 3 138 localidades do SE — ignorar isso perde metade das localidades.
- **Uma rua, N linhas DNE:** `Rua Augusta` são dezenas de `LOG_NU` por seccionamento/CEP. Todas recebem a **mesma** geometria do cluster. Fatiar por faixa de numeração exigiria `OSM_ADDR_POINT` — fora de escopo (ver §7).
- **Homônimo dentro do mesmo município** (duas `Rua Um` em bairros diferentes): sobram N clusters no footprint → 2ª volta usando o centroide do `bai_nu` (calculado das ruas já resolvidas do mesmo bairro); sem desempate, marca `geo_confianca` baixa e usa o cluster maior.
- ~~**Fallback obrigatório:**~~ **cortado no §8.1** — linha sem cluster fica vazia com `geo_status`, não recebe centroide de bairro nem de município.

## 4. Artefato de saída

> **Movido.** O contrato completo, com as 25 colunas e exemplos de linha reais, está em
> **[dne-geo-join.md](./dne-geo-join.md)** — que também corrige as colunas de fallback
> (`geo_origem`/`geo_metodo`/`geo_confianca`) descritas aqui antes do corte do §8.1.
>
> Resumo: `DNE_GEO_LOGRADOURO_{UF}.TXT` = `LOG_LOGRADOURO` do DNE + localidade/bairro/IBGE
> desnormalizados + geometria + `geo_status` / `geo_regra` / `geo_kind` / `osm_ways` / `osm_clusters`.
> Subprodutos: `DNE_GEO_BAIRRO_{UF}.TXT` e `DNE_GEO_RELATORIO_{UF}.json`.

## 5. Etapas

| # | Entrega | Arquivo |
|---|---------|---------|
| 1 | Leitor DNE (latin1 → objetos, localidade/bairro/logradouro) | `dne-reader.js` |
| 2 | Clusterização de ways por nome | `street-clusters.js` |
| 3 | Footprint municipal + resolução ambígua (o núcleo) | `muni-footprint.js` |
| 4 | CLI de junção + escrita dos 3 artefatos | `dne-geo-join.js` |
| 5 | Rodar SP → relatório → aferir contra o teto de 75,8 % | — |
| 6 | RJ, MG, ES | — |
| 7 | Changelog `docs/changelog/AAAA-MM-DD-dne-geo-join.md` + atualizar [estado-atual.md](./estado-atual.md) e [bairro-logradouro.md](./bairro-logradouro.md) | — |

CLI pretendida:

```bash
node dne-geo-join.js --dne=D:\dev\ddsoft\ddsoft-online\_ignore\Delimitado ^
  --osm=G:\osm-geo-se-streets --muni=G:\osm-geo-se ^
  --out=G:\dne-geo --uf=SP
```

Substitui o `osm:dne:enrich-geo` como fonte de verdade: o PHP passa a **carregar** `DNE_GEO_LOGRADOURO_{UF}.TXT` em vez de cruzar dado em SQL.

## 6. Testes

- Unitários com fixtures pequenas (`test/`): cluster separa dois grupos de mesma rua a 50 km; footprint dilata borda; ambíguo escolhe o cluster dentro do footprint; distrito herda footprint do pai; leitor DNE decodifica `José` de latin1.
- **Regressão dos casos deste documento** (os 5 JSONs acima viram fixture de aceite): `Rua Augusta 01305000`, `Avenida Paulista 01310000`, `Rua Estoril 04773090`, `Avenida Atlântica 22010000`, `Avenida do Contorno 30110001` precisam sair com `geo_origem=osm_way` e centro dentro de ~2 km do ponto conhecido. Mais o caso que já funciona (`Maracujá Natal`, `603661`) como não-regressão.
- Aceite de volume: relatório de SP com `osm_way` ≥ 70 % das linhas e `geo_origem` vazio = 0.

## 7. Limites conhecidos (assumidos, não esquecidos)

| Limite | Consequência | Quando atacar |
|--------|--------------|---------------|
| Teto de 75,8 % por nome exato | ~24 % das linhas caem no fallback de bairro/município | Só com fuzzy/abreviatura — medir o ganho antes de codar |
| Geometria por rua inteira, não por seccionamento | Todos os CEPs de uma rua compartilham bbox | Quando `OSM_ADDR_POINT` for extraído |
| Memória: SP inteiro em memória (~400 MB est.) | `--max-old-space-size` necessário | Se estourar: shard por letra inicial do `name_norm` |
| Só o Sudeste tem extract PBF | Demais UFs saem só com centroide de município | Ao processar outros PBF regionais |
| Ruas de divisa | Podem cair no município vizinho | Aceitável; `geo_confianca` sinaliza |

Fora de escopo desta fase: variantes `LOG_VAR_LOG` (medido: rende 3 nomes), malha IBGE, point-in-polygon, índice espacial, número de porta.

## 8. Revisão do plano

Espere, mesmo que o fallback seja claramente identificado, é completamente inútil colocar uma coordenada que nós não saibamos que seja as coordenadas exatas de cada via.
Eu sei que o OSM possui todas as coordenadas, pois obviamente ele precisa delas pra poder renderizar o mapa na tela. O problema talvez seja só casar os nomes do OSM com os nomes do DNE. Mas precisamos fazer uma amostragem, encontrar exemplos de vias do OSM sem correspondente do DNE, e ver se conseguimos estratégias melhores pra encontrar essas vias faltantes.

### 8.1. Fallback por centroide: cortado

Concordo, e é corte, não ajuste. Coordenada de centroide de município ou de bairro responde a uma pergunta que ninguém fez e envenena qualquer uso a jusante (raio de entrega, ordenação por distância, mapa). Linha sem casamento sai com `lat`/`lng` **vazios** e uma coluna `geo_status` dizendo por quê — `sem_nome_osm`, `ambiguo`, `uf_sem_extract`. Some do plano: `geo_origem=bairro|municipio`, `bairro_centroide`, `municipio_centroide` e o critério de aceite "`geo_origem` vazio = 0".

### 8.2. Amostragem: um município só, sem ambiguidade no caminho

Para isolar "nome não casa" de "município desconhecido", rodei tudo dentro da bbox da capital paulista (−24,01…−23,35 / −46,84…−46,36) contra o DNE de `loc_nu=9668`.

| | |
|---|---:|
| Ways OSM na bbox | 165 308 |
| Ways descartadas (`cycleway`, `footway`, `construction`, `busway`…) | 5 631 |
| Nomes OSM endereçáveis distintos | 64 548 |
| Linhas DNE da capital | 53 824 |
| **Casamento por nome exato** | **80,5 %** das linhas |

E os quatro casos "não encontrados" do topo deste documento, dentro da bbox:

| Nome | Ways na capital |
|------|----------------:|
| `rua augusta` | 33 |
| `avenida paulista` | 65 |
| `rua estoril` | 5 |

**Você está certo:** as ruas estão lá, com nome idêntico. O `enrich-geo` atual não as encontra por ambiguidade estadual, não por ausência. O teto de 75,8 % que eu tinha escrito no §1 estava contaminado por isso — o número honesto, por município, é ~80 % **antes** de qualquer melhoria de nome.

### 8.3. Onde estão os 19,5 % que sobram — medido, com os dois residuais

**Residual DNE** (nomes da capital sem way OSM de nome igual), classificado:

| Classe | Linhas | % do DNE | Diagnóstico |
|--------|-------:|---------:|-------------|
| Tipo de logradouro divergente | 1 554 | 2,9 % | DNE `Travessa Goiás` ↔ OSM `Rua Goiás`; DNE `Via de Pedestre Bitínia` ↔ OSM `Rua Bitínia` |
| Grafia (z/s, y/i, ph/f, dobradas) | ~2 800 | 5,2 % | DNE `Luiz Gushiken` ↔ OSM `Luis Gushiken`; `Affonso` ↔ `Afonso`; `Ernest Renam` ↔ `Ernest Renan` |
| Nome parcial / token a mais | ~870 | 1,6 % | DNE `Rua Luiza Helena Bairros` ↔ OSM `Rua Luiza Helena de Bairros` |
| Abreviatura | 8 | 0,0 % | OSM abrevia (`Rua Dr José…`, `Rua S Nazário`) — existe, mas é irrelevante |
| **Sem candidato nenhum** | **~4 940** | **9,2 %** | por tipo DNE: `rua` 2 338 · `praça` 1 259 · `travessa` 723 · `via de pedestre` 244 · `viela` 99 |

Sobre os itens **sem candidato:** Ver seção 9 "Residual DNE sem candidatos" abaixo.

**Residual OSM** (nomes na bbox sem correspondente no DNE da capital) — 28 063 nomes / 61 499 ways, e a amostragem explica quase tudo:

| Origem | Nomes | Exemplos |
|--------|------:|----------|
| Município vizinho dentro da bbox grosseira | 14 789 | `Avenida dos Autonomistas` (Osasco), `Avenida Guido Aliberti` (Santo André), `Avenida Barão de Mauá` (Mauá) |
| Rodovia / estrutura viária | 361 | `Rodoanel Mário Covas` (631 ways), `Marginal Tietê Expressa`, `Corredor ABD` |
| Duplicata não endereçável | 5 631 ways | `Ciclovia Avenida Indianópolis`, `Calçada Partilhada Avenida Luis Gushiken` |
| Resto | ~12,9 k | mistura de vizinhos fora do meu filtro, `(Local)`/`(Central)`/`(Expressa)` no nome, e vias que o DNE realmente não tem |

Ou seja: **o residual OSM quase não contém rua da capital que o DNE tenha**. O gargalo é o outro lado — DNE que o OSM não nomeia.

### 8.4. Onde a premissa "o OSM tem tudo" quebra

Ele tem toda a **geometria**, sim. Mas a chave da junção é o **nome**, e nome é *tag*, não geometria. Três buracos concretos que a amostra mostrou:

1. **Praça: 1 259 linhas do DNE, quase nenhuma no arquivo.** Praça no OSM raramente é `highway=*`; é `place=square` / `leisure=park` / polígono. Nosso extract só pega way com `highway`, então praça simplesmente não foi extraída. **Isso não é dado ausente no OSM — é dado que a gente não pediu.**
2. **Viela / travessa / via de pedestre (~1 070 linhas):** ou não têm `name` no OSM, ou têm nome genérico (`Via de Pedestre`, `Travessa`, sem qualificador). Não há como casar por nome; nem o OSM sabe qual é qual.
3. **`Rua` sem candidato (2 338 linhas, 4,3 %):** loteamento novo, numeração de travessa (`1ª Travessa Edalberto dos Santos`), nome só no cadastro dos Correios. Esse é o buraco genuíno do OSM.

### 8.5. Estratégias melhores — com ganho medido, não estimado

Cascata de casamento por nome, cada degrau só roda no que sobrou do anterior, **sempre dentro do mesmo município**:

| # | Regra | Ganho medido (capital) | Risco |
|---|-------|----------------------:|-------|
| 1 | Nome exato normalizado | 80,5 % | — |
| 2 | **Núcleo sem tipo**: tira `Rua/Travessa/Viela/Via de Pedestre` dos dois lados e casa o resto; o tipo vira desempate, não chave | **+2,9 %** | baixo |
| 3 | **Chave fonética PT-BR**: `z→s`, `y→i`, `ph→f`, `h` mudo, `c/qu→k`, `x→ch`, `m$→n`, letras dobradas colapsadas | **+1,7 %** | **0,93 %** de colisão entre núcleos OSM distintos |
| | **Subtotal determinístico** | **85,1 %** | |
| 4 | Contenção de tokens com token raro obrigatório + candidato único | +1,0 % | médio |
| 5 | Levenshtein no núcleo, limiar escalado por tamanho, exigindo candidato único e margem sobre o 2º | +5,2 % bruto | **alto** — gerou `flor de cereja`→`flor de cera`, `mércia`→`meca` |

O degrau 3 é o achado que vale: substitui *heurística com limiar* por *regra determinística*. `Luiz`/`Luis`, `Sousa`/`Souza`, `Braz`/`Brás`, `Airton`/`Ayrton` colapsam na mesma chave, e a taxa de colisão medida em 59 827 núcleos foi 0,93 % — e as colisões são justamente pares que **deveriam** colapsar (`washington luis | washington luiz`). Fuzzy por distância de edição fica **fora** até termos guarda geográfica (§8.6); o ganho de 5,2 % não compensa o tipo de erro que ele produz.

**Sinais ainda não explorados** — os dois primeiros já estão codados, só não foram rodados:

| Sinal | Custo | O que resolve |
|-------|-------|---------------|
| `--datasets=addr` (`addr:street` + `addr:postcode` nos nós de numeração) | re-extract, código já existe ([`extract-geocode-pbf.js:319`](../../extract-geocode-pbf.js#L319)) | dá nome **e CEP** a via cujo way não tem `name`; e permite recortar a rua por faixa de numeração |
| `alt_name` / `old_name` / `short_name` / `name:pt-BR` | 1 linha em [`extract-geocode-pbf.js:202`](../../extract-geocode-pbf.js#L202) (hoje só `name:pt`, `name`, `official_name`) | denominação anterior/popular do lado OSM — que é exatamente o que o `LOG_VAR_LOG` tentava dar do lado DNE e não deu |
| Praça como área (`place=square`, `leisure=park`, `highway=pedestrian` em polígono) | dataset novo no extract | as 1 259 linhas de praça da capital |
| CEP como guarda geográfica | derivado do que já casou | prefixo de CEP é espacialmente contíguo; serve para **validar/descartar** candidato fuzzy, nunca para gerar coordenada |

### 8.6. Consequência para o plano

- §1: trocar "teto de 75,8 %" por **80,5 % exato / 85,1 % determinístico, por município** — o número estadual media ambiguidade junto e não serve de meta.
- §3: passo 6 deixa de ser "casa nome exato" e vira a cascata 1→3 do §8.5. Fuzzy fica desligado por padrão, atrás de `--fuzzy`.
- §4: sai `geo_origem`/`geo_metodo` com fallback; entra `geo_status` (`ok` / `sem_nome_osm` / `ambiguo` / `sem_extract`) + `geo_regra` (`exato` / `nucleo` / `fonetico`) para auditoria.
- §5: entra etapa 0 — re-extract com `--datasets=logradouro,addr` e tags de nome alternativo, **antes** de codar a junção, porque muda o teto.
- §6: aceite vira "≥ 85 % das linhas com `geo_status=ok` na capital, 0 linha com coordenada inventada".
- §7: o limite honesto passa a ser **~9 % de logradouros do DNE sem nome correspondente no OSM** — e a resposta a esses é ficar vazio, não chutar.

## 9. Resudial DNE sem candidatos

Abaixo, eu coloquei a pesquisa manual que eu fiz pra cada um dos endereços sem candidatos informados na amostragem. Muitas são vias realmente muito pequenas (passagem, viela, travessa), várias não existem nem no google maps.

Vamos deixar estas em branco por enquanto, mas em um sistema que faça geocoding e geocoding reverso, podemos adicionar uma funcionalidade pra se o usuário receber um endereço sem coordenadas, que ele possa solicitar, sob demanda, pra encontrar vias próximas usando a similaridade do CEP. Não é algo que compensa trazer na primeira busca do endereço porque vai adicionar muito peso pra algo raramente utilizado. Também não compensa fazer um mapa precompilado pois não há uma demanda muito forte por isso ainda.

   DNE "passagem alfredo tanca braga" (1 linhas, cep 05360122)
Rua Professor Alfredo Tianca Braga
-23.561588469193307, -46.743874255265666
// https://www.openstreetmap.org/way/168766971

   DNE "viela roberto de barros" (1 linhas, cep 05360153)
Avenida São Remo
-23.56231788076297, -46.74556502754197
// é uma viela não desenhada no OSM nem no Google Maps
// https://www.openstreetmap.org/way/544343923

   DNE "viela pires brandao" (1 linhas, cep 05360095)
R. Pires Brandão - Vila Butantã, São Paulo - SP, 05360-090
-23.56255987247737, -46.743994134989876
// Neste caso, foi achado somente a "rua" Pires Brandão, mas o número do CEP é
// **extremamente similar** - com isso dá pra ter confiança que quem for lá
// procurar essa viela, vai estar **muito** perto do lugar certo.
// https://www.openstreetmap.org/way/149888636

   DNE "viela kenkiti simomoto" (1 linhas, cep 05347015)
Av. Kenkiti Simomoto - Jaguaré, São Paulo - SP, 05347-010
-23.55174290942879, -46.73940007945743
// https://www.openstreetmap.org/way/40254253

   DNE "travessa philomeno antonio conceicao ianetta" (1 linhas, cep 04337095)
Tv. Philomeno Antonio Conceição Lanetta, 89-1 - Americanópolis
-23.667199538798783, -46.649484603277166
// endereço acima veio do google
// No OSM: Travessa Particular
// https://www.openstreetmap.org/way/116688641

   DNE "travessa gina alves dias" (1 linhas, cep 04193315)
R. François Bunel, 299-265 - Jardim Sao Saverio, São Paulo - SP, 04193-310
-23.64994119246096, -46.610092887293355
// Essa travessa nem o Google Maps tem por nome, mas ele mostra uma travessa
// sem nome muito perto da rua François Bunel, novamente com o CEP extremamente
// similar. Essa confiança não é suficiente pra jogar estas coordenadas
// diretamente na "travessa gina alves dias", mas eu faria uma associação:
// colocaria o "log_nu" como sugerido ou relacionado da rua François Bunel por
// causa da similaridade do número do CEP. Assim, o usuário que procurar por
// esta via pode receber a sugestão sem precisar fazer outra consulta ou olhar
// no mapa.

   DNE "rua vitoria regia branca" (1 linhas, cep 08395323)
R. Flores da Primavera, 95 - Conj. Promorar Rio Claro, São Paulo - SP, 08395-325
R. Vitória Régia, 53-41 - Jardim Rodolfo Pirani, São Paulo - SP, 08310-345
-23.629795802439833, -46.45909858571785
// Este caso aqui eu encontrei ele pela seguinte forma: Os cinco primeiros dígitos
// do CEP compreendem uma região, com o tamanho aproximado de um bairro médio.
// Eu vi um CEP muito parecido 08395-325. E colado nessa rua, tem uma rua chamada
// Rua Vitória Régia. (Mas no OSM ela está com o nome "Rua Vito Régia" -
// https://www.openstreetmap.org/way/172822220).
// Então neste caso, eu ficaria muito confortável de colocar as coordenadas desta
// via para o cep "08395323".

   DNE "travessa miuta mangueira pereira" (1 linhas, cep 04428190)
R. Árvore de Bálsamo, 19-13 - Americanópolis, São Paulo - SP, 04428-180
-23.67743324665735, -46.65705359623455
// Novamente fui por um CEP muito similar. Eu colocaria o "log_nu" dessa via do
// DNE como uma via sugerida ou relacionada, para assim termos um palpite das
// coordenadas reais o mais próximas possíveis.

   DNE "area rural" (1 linhas, cep 05999899)
// OK esse parece ser genuinamente um caso especial, um número de CEP exótico e
// um nome que aparenta ser especial também, sem um tipo de logradouro comum.
// Esse eu realmente dou por perdido, pois parece ser uma descrição genérica,
// não um local específico.

   DNE "rua pranto de poeta" (1 linhas, cep 08381015)
Rua Piramide do Piques - Jardim Iguatemi, São Paulo - SP, 08381-020
-23.60732402328148, -46.434350111184585
// Nem o google maps tem essa rua "pranto de poeta" de novo, então não é uma
// preocupação grande ter a coordenada exata dessa via. Mas eu pegaria uma via
// com o cep mais próximo e colocaria como via sugerida ou relacionada.
// https://www.openstreetmap.org/way/387452297

   DNE "rua solo de clarineta" (1 linhas, cep 03623015)
R. Vera - Vila Granada, São Paulo - SP, 03623-000
-23.518895562589417, -46.51711487620642
// cep mais próximo como sugerido ou relacionado
// https://www.openstreetmap.org/way/295173694
// A rua Solo de Clarineta aparece no Google Maps.
// Via equivalente no OSM: está com o nome de "Rua Santa Cirila"
// https://www.openstreetmap.org/way/295173719

   DNE "avenida intersetorial" (1 linhas, cep 05508004)
R. Prof. Gabriel Silvestre Teixeira de Carvalho - Butantã, São Paulo - SP, 05508-006
-23.570013957112195, -46.73746784685573
// https://www.openstreetmap.org/way/495777432
// cep mais próximo como sugerido ou relacionado.
// O google maps possui essa via, ela é paralela com a Av. Prof. Antônio Barros
// Ulhôa Cintra. Essa via está desenhada no OSM, mas está sem nome
// https://www.openstreetmap.org/way/293798033

## 10. Melhorar o extractor antes de rodar de novo

> **Implementado e rodado em 2026-07-30.** Extract concluído em `G:\osm-geo-se-streets2`
> (`eof: true`, logradouro 1 265 470, bairro 30 906, addr 205 660). Resultado medido no §10.6.
> Ver [changelog](../changelog/2026-07-30-logradouro-kind-e-name-alt.md).

### 10.1. Quanto isso custa hoje — taxa de casamento por `TLO_TX` na capital

| `TLO_TX` | Linhas DNE | Casadas | % |
|----------|-----------:|--------:|--:|
| Alameda | 279 | 254 | **91,0 %** |
| Avenida | 2 591 | 2 339 | **90,3 %** |
| Rua | 41 684 | 35 969 | **86,3 %** |
| Estrada | 236 | 174 | 73,7 % |
| Passagem | 134 | 96 | 71,6 % |
| Travessa | 4 441 | 3 046 | 68,6 % |
| Largo | 71 | 47 | 66,2 % |
| Viaduto | 77 | 49 | 63,6 % |
| Viela | 692 | 383 | 55,3 % |
| **Ponte** | 24 | 9 | **37,5 %** |
| **Via de Pedestre** | 527 | 197 | **37,4 %** |
| **Praça** | **2 703** | 971 | **35,9 %** |
| **Parque** | 25 | 7 | **28,0 %** |
| **Vila** | 151 | 32 | **21,2 %** |

Rua/Avenida/Alameda ficam em ~90 % — o extractor faz bem o que se propôs a fazer. O que despenca é justamente o que **não é linha**: praça, parque, vila, via de pedestre. Em SP inteiro são **9 631 praças** no DNE; na capital, 1 732 delas não têm candidato.

Prova de que é o extractor e não o OSM: a capital inteira tem só **458 ways `highway=pedestrian`** no nosso arquivo. Praça no OSM não é `highway` — é `place=square`, `leisure=park`, ou área fechada sem `highway` nenhum. Nunca entrou porque nunca foi pedida.

### 10.2. A mudança: alargar o predicado, não o pipeline

O portão hoje é uma condição só, em [`extract-geocode-pbf.js:350`](../../extract-geocode-pbf.js#L350):

```js
if (wantDataset(ctx.datasets, 'logradouro') && tags.highway && name) {
```

Proposta — uma função e duas trocas de `tags.highway` por ela:

```js
/** Shapes que o DNE trata como logradouro (TLO_TX Praça/Largo/Parque/Vila…). */
function logradouroKind(tags) {
	if (tags.highway) return tags.highway;            // rua, avenida, pedestrian…
	if (tags.place === 'square') return 'square';      // praça, largo
	if (tags.leisure === 'park' || tags.leisure === 'garden') return 'park';
	if (tags.landuse === 'village_green') return 'park';
	return '';
	// ponytail: só way (fechada ou não). Relation multipolygon fica de fora —
	// custaria um 3º passe e vale 25 linhas de "Parque" na capital.
	// Se um dia doer: resolver members da relation no mesmo two-pass dos streets.
}
```

| Onde | De | Para |
|------|----|------|
| [`:350`](../../extract-geocode-pbf.js#L350) portão do dataset | `tags.highway` | `logradouroKind(tags)` |
| [`:480`](../../extract-geocode-pbf.js#L480) coluna 4 da linha | `tags.highway` | `logradouroKind(tags)` |
| [`README-colunas.md`](../../../osm-geo-se-streets/README-colunas.md) | coluna `highway` | coluna `kind` + `osm_type` no fim |

**Nada mais muda.** `geomFromNodeIds` já calcula bbox e centroide a partir dos nós — para uma way fechada isso dá exatamente o polígono da praça. O two-pass já agenda `nodeIds` sem olhar que tag a way tem ([`:351`](../../extract-geocode-pbf.js#L351)). Sem passe novo, sem dataset novo, sem custo de runtime perceptível: é um teste de tag a mais por way.

**Praça como nó:** boa parte das praças no Brasil está mapeada como nó `place=square`, sem extensão. Vale emitir no mesmo arquivo dentro de `processFeatureNode` ([`:212`](../../extract-geocode-pbf.js#L212)), com `kind=square`, `osm_type=node` e `way_node_count=1` — ponto exato, bbox degenerada. Isso é dado honesto: a coordenada é real, só não tem área. Não confundir com o fallback de centroide que foi cortado no §8.1.

### 10.3. A guarda que precisa vir junto

`leisure=park` traz `Parque Villa-Lobos`, `Parque do Ibirapuera` e mais uns milhares de parques que **não são logradouro**. Se entrarem no mesmo índice de nomes sem qualificação, um `Rua Ibirapuera` do DNE pode casar com o parque.

Regra no join, uma linha: candidato com `kind` de área (`square`, `park`) só vale para linha DNE cujo `TLO_TX` seja **Praça, Largo, Parque, Jardim, Vila ou Área**. Para `Rua`/`Avenida`, só `kind` de via. Casamento fica *kind-aware*, e o ruído novo não encosta no que já funciona a 86–91 %.

### 10.4. Empacotar tudo numa rodada só

O extract são ~2 passes sobre 813 MB e 141 M nós. Não vale rodar duas vezes — as três mudanças entram juntas:

| # | Mudança | Tamanho |
|---|---------|---------|
| 1 | `logradouroKind` + coluna `kind`/`osm_type` (§10.2) | ~15 linhas |
| 2 | Colunas `name_alt` / `name_alt_norm` com `alt_name;short_name;old_name;name:pt-BR` — **sem** mexer na cadeia do `name` principal em [`:202`](../../extract-geocode-pbf.js#L202) | ~6 linhas |
| 3 | `--datasets=logradouro,bairro,addr` (código do `addr` já existe em [`:319`](../../extract-geocode-pbf.js#L319), nunca foi rodado) | 0 |

```bash
set NODE_OPTIONS=--max-old-space-size=8192
node extract-geocode-pbf.js G:\sudeste-260725.osm.pbf ^
  --out=G:\osm-geo-se-streets2 ^
  --datasets=logradouro,bairro,addr
```

**Pasta nova.** Sem `--resume` o extract apaga todos os `OSM_*.TXT` da pasta, e `G:\osm-geo-se-streets` é a linha de base contra a qual os números deste documento foram medidos — sem ela não dá para provar que a mudança melhorou.

### 10.5. Como saber se funcionou

- Teste unitário do predicado: way com `place=square` e nome entra; way com `leisure=park` entra como `park`; way sem nenhum dos dois e sem `highway` fica de fora; `highway` ganha de `place` quando os dois existem.
- Re-rodar a medição do §10.1 na pasta nova. Critério: **Praça sai de 35,9 %** e Rua/Avenida **não caem**. Se Rua cair, o `kind` vazou para o match — é a guarda do §10.3 que falhou.
- `writerCounts` do checkpoint com `OSM_ADDR_POINT_SP` > 0 (hoje `addr: 0` em [`extract-checkpoint.json`](../../../osm-geo-se-streets/extract-checkpoint.json)) confirma que o dataset `addr` de fato rodou.

### 10.6. Resultado medido — `G:\osm-geo-se-streets2` (2026-07-30)

Extract completo (`eof: true`): logradouro **1 265 470** (era 1 240 490), bairro 30 906,
addr **205 660**. Na bbox da capital: 170 604 features (+5 296), sendo **73 nomes de
`square`** e **4 533 de `park`**, mais 4 671 nomes que só existem em `name_alt` e
7 147 nomes distintos em `addr:street`.

| `TLO_TX` | Linhas | Antes | Depois | Δ | |
|----------|-------:|------:|-------:|--:|--|
| **Praça** | 2 703 | 35,9 % | **76,1 %** | +1 087 | ✅ |
| **Parque** | 25 | 28,0 % | **68,0 %** | +10 | ✅ |
| **Largo** | 71 | 66,2 % | **77,5 %** | +8 | ✅ |
| **Ponte** | 24 | 37,5 % | **54,2 %** | +4 | ✅ |
| Viaduto | 77 | 63,6 % | 70,1 % | +5 | |
| Estrada | 236 | 73,7 % | 76,3 % | +6 | |
| Vila | 151 | 21,2 % | 23,8 % | +4 | |
| Travessa | 4 441 | 68,6 % | 70,2 % | +72 | |
| Rua | 41 684 | 86,3 % | **87,4 %** | +466 | não caiu |
| Avenida | 2 591 | 90,3 % | **91,5 %** | +33 | não caiu |
| Alameda | 279 | 91,0 % | 91,4 % | +1 | não caiu |

**Critério de aceite do §10.5 cumprido:** Praça saiu de 35,9 % para 76,1 %, e nenhum
tipo caiu — Rua e Avenida até subiram, por conta do `name_alt`.

Cascata determinística completa (§8.5 + §10) sobre o extract novo:

| Degrau | Linhas | Ganho | Acumulado |
|--------|-------:|------:|----------:|
| Nome exato (via) | 43 315 | 80,5 pp | 80,5 % |
| Área `square`/`park`, com guarda kind-aware | 1 109 | +2,1 pp | 82,5 % |
| `name_alt` | 601 | +1,1 pp | 83,7 % |
| `addr:street` | 33 | +0,1 pp | 83,7 % |
| Núcleo sem tipo | 1 320 | +2,5 pp | 86,2 % |
| Chave fonética PT-BR | 895 | +1,7 pp | **87,8 %** |
| Sem candidato | 6 551 | | 12,2 % |

**Meta do §8.6 sobe de 85,1 % para 87,8 %**, tudo determinístico, sem fuzzy.

Três leituras honestas do que veio abaixo do esperado:

- **`addr:street` rendeu +0,1 pp**, não os "% recuperados" que eu previa no §8.5. Como *fonte de nome* ele é quase irrelevante — 7 147 nomes na capital, quase todos já conhecidos. O valor dele é outro: **`addr:postcode` como guarda geográfica** e nomear way sem `name` por proximidade — nenhum dos dois é casamento por nome, e nenhum foi testado ainda.
- **A guarda kind-aware custou 2 linhas.** Sem ela seriam 1 111 casamentos de área em vez de 1 109. O risco era menor do que eu estimei — mas continua sendo seguro de graça, então fica.
- **Praça como nó rendeu 2 features** na capital. O ganho de praça veio quase todo de `leisure=park` (4 533 nomes) — no Brasil a praça costuma estar mapeada como parque, não como `place=square`.

