# 2026-08-21 — o join passa a medir quanta linha `ok` cai fora do município

Execução da tarefa descrita em
[tarefa-validacao-poligono-municipal.md](../tarefa-validacao-poligono-municipal.md).
Continuação de [2026-08-18-uf-por-poligono.md](./2026-08-18-uf-por-poligono.md)
(malha de UF) e de
[2026-08-18-uf-por-filtro-e-ancora-podada.md](./2026-08-18-uf-por-filtro-e-ancora-podada.md)
(o defeito da âncora de outra cidade e o `--ancora-raio-km`).

## Prompt

> Adicionar ao `dne-geo-join.js` uma **verificação pós-join** que mede quantas
> linhas `geo_status=ok` têm centróide **fora do polígono real do município** a
> que a linha pertence, e reporta isso no `DNE_GEO_RELATORIO_{UF}.json` e no log.
> **Não** é para mudar a lógica do join nesta tarefa. É medição, não filtro. […]
> `scripts/build-mun-poly.js` […] `mun-poly.js` — API espelhando `uf-poly.js` […]
> quebra por faixa de distância da borda (`<1`, `1–5`, `5–25`, `>25` km) […] flag
> para desligar (`--sem-validacao-poligono`); e se o `mun-poly.json` não existir,
> **degrade, não quebre**.

## O problema

O pior modo de falha do pipeline é silencioso: a linha sai `ok`, com coordenada,
e o consumidor desenha no mapa com ar de acerto. Não aparece em resíduo nenhum.

A defesa de hoje contra o falso positivo distante é a **pegada por âncoras** —
células do OSM em volta do que casou sozinho. É uma aproximação do município
construída com o mesmo dado que se quer validar: quando erra, erra calada. O
`--ancora-raio-km=60` de 18/08 levou MG de 19,64 % para 0,06 % de âncora podada,
mas não elimina a categoria.

O polígono do IBGE é **independente do join**, e por isso enxerga o resíduo.

## O que foi implementado

### `mun-poly.json` — a malha municipal, versionada

- Fonte: **API de malhas v3 do IBGE**, `estados/{cod}?qualidade=maxima&intrarregiao=municipio`,
  baixada em **21/08/2026**, uma resposta por UF. URL, data, licença e recorte
  ficam gravados no próprio arquivo, como no `uf-poly.json`.
- **Recorte: 636 municípios** — os que têm logradouro no DNE. Os outros 4 934 são
  CEP único, sem rua, e nunca viram uma linha para validar.
- Simplificação **Douglas-Peucker eps = 0,003°** (~333 m), 4 casas (~11 m).
- **1 048 KB**, 60 360 pontos.

O eps foi escolhido medindo, não por analogia com o `uf-poly.json` — que usa
0,005°. Rodando a verificação sobre o mesmo join de RJ com cada malha:

| eps | tamanho | fora | < 1 km | 1–5 km | 5–25 km | > 25 km |
|---|---:|---:|---:|---:|---:|---:|
| malha crua (referência da tarefa) | 4,2 MB | 6 837 | 1 183 | 1 539 | 3 030 | 1 085 |
| 0,005° | 676 KB | 7 198 | **1 536** | 1 528 | 3 046 | 1 088 |
| 0,004° | 821 KB | 7 039 | 1 384 | 1 524 | 3 047 | 1 084 |
| **0,003°** | **1 048 KB** | **6 926** | **1 278** | 1 519 | 3 046 | 1 083 |
| 0,002° | 1 473 KB | 6 859 | 1 247 | 1 483 | 3 046 | 1 083 |

As duas faixas que **decidem** alguma coisa (5–25 e > 25) são insensíveis ao eps:
3 046 e 1 083 em todas as linhas da tabela. Quem se mexe é só a faixa de ruído,
porque um erro de simplificação de 555 m empurra para "fora" a via de divisa que
está a 300 m de dentro — e a 0,005° isso infla `< 1 km` em 26 %.

0,003° é onde a curva dobra: de 0,003° para 0,002° são +40 % de arquivo para
mover a faixa de ruído 2,4 %. E deixa folga para o passo seguinte (o polígono
como **filtro**, com tolerância de borda de 1–2 km): 333 m de imprecisão cabem
nessa tolerância, 555 m comem um terço dela.

O orçamento estourou o do `uf-poly.json` (398 KB) de qualquer jeito — mesmo a
0,005° são 676 KB. São 636 polígonos municipais, não 27 estaduais; a mesma
tolerância guarda muito mais perímetro por km² de área.

### `scripts/build-mun-poly.js`

```bash
npm run mun:poly -- --dne="D:\…\Delimitado"          # 27 UFs, recorte DNE
npm run mun:poly -- --dne=… --uf=RJ,SP               # só essas UFs
npm run mun:poly -- --todos                          # sem recorte (~4 MB)
npm run mun:poly -- --in=malha-rj.json               # de um arquivo local
npm run mun:poly -- --cache=DIR --eps=0.003          # reusa a malha crua
```

Baixa **por UF e concatena**: são 27 respostas de ~0,3 a 8,4 MB, cada uma
verificável, e a que falhar se repete sozinha. `--cache` guarda a malha crua, o
que torna a escolha do eps um experimento de segundos em vez de minutos.
Reusa o `simplify` (Douglas-Peucker iterativo) do `build-uf-poly.js`.

O recorte sai do próprio DNE — `LOG_LOCALIDADE.TXT` + `LOG_LOGRADOURO_{UF}.TXT`,
com distrito herdando o IBGE do município de subordinação, exatamente como o
join faz. Sem `--dne` grava todos os 5 570 e diz isso no log.

### `mun-poly.js`

```js
dentroDoMunicipio(ibge, lat, lng)   // true | false | null
distanciaDaBordaKm(ibge, lat, lng)  // km até o segmento de divisa mais próximo
disponivel() / meta() / usarArquivo(caminho)
```

Convenções, todas testadas:

- **`null` é "não sei"** — município fora do recorte, malha ausente, coordenada
  inválida. Quem chama não pode transformar isso em "fora".
- **Ponto exatamente sobre a divisa é dentro dos dois** municípios que a
  compartilham, como no `uf-poly.js`. A tolerância é a mesma (`EPS = 1e-7`).
- **Buraco se resolve por paridade**: os anéis internos do `MultiPolygon` entram
  na mesma lista que o externo e o par-ímpar cuida do resto — o ponto dentro do
  buraco cruza os dois anéis, paridade par, fora.
- `distanciaKm` é equirretangular local (o mesmo fator de km/grau do
  `geo-cluster.js`) e mede até o **segmento**, não até o vértice.

**Sem grade, ao contrário do `uf-poly.js`** — e essa é a única divergência
consciente em relação ao texto da tarefa. Lá a pergunta é "qual UF?", uma busca
entre 27 polígonos, e a grade célula → candidatos existe para não varrer os 27.
Aqui **o município já vem dado** (coluna 14 do DNE): não há busca, só o teste
contra um polígono de ~95 pontos. Medido no re-join de RJ: 70 797 consultas, com
a distância da borda calculada para as 6 926 que caem fora, em **~450 ms**.
Grade seria índice para uma busca que não existe. A carga é preguiçosa e o shape
de cada município é montado na primeira pergunta sobre ele — um join de UF toca
algumas dezenas dos 636.

### A verificação no `dne-geo-join.js`

Roda depois do `writer.flush()`, sobre as linhas já resolvidas.
**Não toca em `geo_status` nem na escolha de candidato** — há teste comparando a
saída byte a byte com e sem a verificação ligada.

```
[poly] fora do município: 6926/70797 ok (9.78%)  <1km=1278 1-5km=1519 5-25km=3046 >25km=1083
```

E no `DNE_GEO_RELATORIO_{UF}.json`, sob `validacao_poligono`:

```json
{
  "malha": { "fonte": "…intrarregiao=municipio", "baixado_em": "2026-08-21",
             "simplificacao": "Douglas-Peucker eps=0.003° (~333 m), 4 casas",
             "recorte": "636 municípios com logradouro no DNE", "municipios": 636 },
  "ok": 70797, "avaliados": 70797, "sem_ibge": 0, "sem_poligono": 0,
  "fora": 6926, "fora_pct": 9.78,
  "fora_por_faixa": { "ate_1km": 1278, "de_1_a_5km": 1519,
                      "de_5_a_25km": 3046, "mais_de_25km": 1083 },
  "fora_por_municipio": { "Rio de Janeiro": 1331, "Nova Iguaçu": 789, "…": 0 },
  "fora_exemplos": [ { "log_nu": "373543", "nome": "Avenida das Camélias",
                       "cidade": "Volta Redonda", "ibge": "3306305",
                       "cep": "27281050", "lat": -22.4992, "lng": -44.6745,
                       "km_fora": 53.3, "geo_regra": "exato" } ]
}
```

A **faixa** é o que separa ruído de divisa de erro de verdade — sem ela o total
não decide nada. `fora_por_municipio` mostra onde o erro é massa, não acaso: em
RJ, 1 331 linhas só no Rio, 789 em Nova Iguaçu.

Flags novas:

| flag | efeito |
|---|---|
| `--sem-validacao-poligono` | desliga a medição (loga que desligou) |
| `--mun-poly=ARQ` | malha alternativa (padrão: `./mun-poly.json`) |
| `--validacao-exemplos=N` | tamanho da amostra das piores (padrão 30; 0 desliga) |

Sem `mun-poly.json` o join **degrada**: loga `[poly] [skip] malha municipal
ausente (…) — ver scripts/build-mun-poly.js` e segue igual. O arquivo é grande e
versionado; nenhum run pode depender dele para terminar.

## Antes / depois

Antes, o run de RJ terminava assim:

```
[6/6] gravando…
OK: 70797/103723 (68.3%)  exato=59382 area=307 …
```

— 6,8 mil linhas com coordenada em outra cidade, e nada no pipeline enxergando.
Foi preciso alguém tropeçar por acaso, montando um bbox para um recorte PMTiles.

Depois:

```
[6/6] gravando…
[poly] fora do município: 6926/70797 ok (9.78%)  <1km=1278 1-5km=1519 5-25km=3046 >25km=1083
OK: 70797/103723 (68.3%)  exato=59382 area=307 …
```

O join produz exatamente o mesmo `DNE_GEO_LOGRADOURO_RJ.TXT` — o que mudou é que
agora ele conta o que já estava errado.

## Como foi testado

### Manual — o critério de aceite da tarefa

```bash
node dne-geo-join.js --dne="D:\dev\ddsoft\ddsoft-online\_ignore\Delimitado" \
    --osm="G:\osm-geo-br-rj" --out="G:\dne-geo-rj-join4" --uf=RJ
```

| critério | referência (21/08, malha crua) | medido | |
|---|---:|---:|---|
| linhas `ok` fora do polígono | ~6 837 | **6 926** | ✅ +1,3 % |
| < 1 km | 1 183 | 1 278 | ✅ faixa de ruído, inflada pelo eps |
| 1–5 km | 1 539 | 1 519 | ✅ |
| 5–25 km | 3 030 | 3 046 | ✅ |
| > 25 km | 1 085 | 1 083 | ✅ |
| `Rua das Colinas` / Macaé / 27960271 | 45 km | **44,7 km**, faixa > 25 km | ⚠️ ver abaixo |
| sem `mun-poly.json`, o join roda igual | — | saída byte a byte idêntica | ✅ |

⚠️ **Ressalva honesta sobre um critério.** A tarefa pede que `Rua das Colinas`
apareça "entre as piores". Ela é detectada, a 44,7 km, na faixa que não tem
desculpa — mas é a **190ª pior de 6 926**, e a 26ª só dentro de Macaé (as piores
de Macaé estão a ~50 km; as piores de RJ, em Teresópolis e Volta Redonda, a
53 km). Com a amostra padrão de 30 ela não aparece; com `--validacao-exemplos=200`
aparece. Não forcei a amostra a contê-la: qualquer regra que colocasse
especificamente essa linha num top-30 estaria escolhendo o dado para caber no
teste. O que o teste automatizado exige é o que importa — que ela seja detectada,
em Macaé, a ~45 km, na faixa > 25 km.

Também rodado com `--validacao-exemplos=99999` para conferir a distribuição por
município e o ranking completo.

### Automatizado

`npm test` — **182 testes, 180 passando, 2 skipped** (fixtures grandes de bz2,
pré-existentes, precisam de `npm run fixtures:large`).

**[`test/mun-poly.test.js`](../../test/mun-poly.test.js)** — 11 testes novos:

| teste | o que trava |
|---|---|
| malha carregada, com fonte e recorte declarados | a URL do IBGE, `intrarregiao=municipio`, codarea de 7 dígitos em todas as chaves |
| ponto dentro e ponto fora | centro do Rio é do Rio e não de Niterói, e vice-versa |
| **ponto exatamente sobre a divisa** | vértice compartilhado (achado no dado, não fixado) é dentro **dos dois**, e a distância da borda ali é zero |
| ponto sobre o segmento, não sobre o vértice | o meio de uma aresta também é dentro |
| **buraco no MultiPolygon** | coroa dentro, buraco fora, borda do buraco dentro — shape sintético |
| `distanciaKm` mede até o segmento | meio grau ao sul de uma aresta = 55,6 km |
| "não sei" é `null`, e nunca `false` | município fora do recorte, lat/lng `null`/`NaN` |
| **malha ausente degrada, não quebra** | `usarArquivo` de caminho inexistente → tudo `null`, e volta ao normal depois |
| 200 mil consultas sob o teto | rede de segurança contra a consulta virar varredura dos 636 |
| **resíduo de RJ bate com a medição de 21/08** | aceite completo sobre `G:\dne-geo-rj-join3` (pula com `t.skip` se o arquivo não estiver no disco) |
| Macaé não contém a coordenada da Rua das Colinas | o caso que fez a tarefa existir, isolado |

**[`test/dne-geo-join.test.js`](../../test/dne-geo-join.test.js)** — 6 testes novos,
com malha sintética escrita no diretório temporário da fixture:

| teste | o que trava |
|---|---|
| validação conta as `ok` fora do município | `fora`, faixa, `fora_por_municipio` e os campos do exemplo (log_nu, ibge, cep, km, regra) |
| município fora do recorte é "não sei", não "fora" | `sem_poligono` sobe, `fora` não |
| sem malha o join roda igual | `validacao_poligono === null` e o run termina |
| **`--sem-validacao-poligono` não mexe no join** | `geo_status`, `geo_regra` e a **saída byte a byte** iguais com e sem |
| `--validacao-exemplos` limita a amostra | corte em N, piores primeiro |
| `faixaBorda` | as fronteiras exatas de 1, 5 e 25 km |

Mais as três flags novas no teste de `parseCli`.

## Arquivos

| arquivo | |
|---|---|
| `mun-poly.js` | novo — point-in-polygon e distância da borda por código IBGE |
| `mun-poly.json` | novo — 636 municípios, 1 048 KB, IBGE 21/08/2026 |
| `scripts/build-mun-poly.js` | novo — baixa por UF, simplifica, recorta pelo DNE |
| `dne-geo-join.js` | `validarPoligonoMunicipal` + `faixaBorda`, 3 flags, bloco no relatório |
| `test/mun-poly.test.js` | novo — 11 testes |
| `test/dne-geo-join.test.js` | +6 testes, +3 asserções em `parseCli` |
| `package.json` | `mun:poly`, `test:mun`; `test:join` passa a incluir `mun-poly.test.js` |
| `docs/geo/dne-geo-join.md` | fase 7 e o campo novo do relatório |

## O que NÃO foi feito (de propósito)

Substituir a pegada por âncoras pelo polígono **dentro** do join, como filtro de
candidato. A tarefa mede o teto disso e ele é baixo como *recuperador* — das
15 694 ambíguas de RJ, só 4,8 % têm exatamente um candidato dentro do município,
e 84,1 % não têm nenhum (o OSM simplesmente não tem a rua ali). O valor do
polígono é como **filtro**: eliminar por construção a categoria do falso positivo
distante, hoje defendida por heurística que deixa passar ~10 %. Quando for feito,
**dar folga de borda** — cortar sem tolerância joga fora as ~1 200 linhas
legítimas a menos de 1 km da divisa; entre 1 e 2 km parece o ponto.

Também não é deste changelog o problema do lado `ddsoft-online`: o
`osm:dne:enrich-geo` só preenche `lat` NULL por padrão e o `DneOsmGeoEnricher`
nunca escreve NULL, então as linhas que o join corrigido rebaixou de `ok` para
`ambiguo` mantêm no banco a coordenada velha (~8 400 em RJ). Está registrado no
fim da [tarefa](../tarefa-validacao-poligono-municipal.md).

## Commit

```
feat(geo): o join mede quanta linha ok cai fora do polígono do município
```
