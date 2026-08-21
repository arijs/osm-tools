# Tarefa: validação pós-join por polígono municipal do IBGE

> Prompt de handoff. Escrito em 21/08/2026 por uma sessão que investigou o geo do
> DNE a pedido do usuário. Contém tudo o que a sessão nova precisa saber; não
> depende de histórico de conversa.

## Objetivo

Adicionar ao `dne-geo-join.js` uma **verificação pós-join** que mede quantas
linhas `geo_status=ok` têm centróide **fora do polígono real do município** a que
a linha pertence, e reporta isso no `DNE_GEO_RELATORIO_{UF}.json` e no log.

**Não** é para mudar a lógica do join nesta tarefa. É medição, não filtro. O
filtro (substituir a pegada por âncoras pelo polígono) é uma tarefa maior,
descrita no fim deste documento.

## Por que isso importa

O pior modo de falha deste pipeline é silencioso: a linha sai como `ok`, com
coordenada, e o consumidor desenha no mapa com ar de acerto. Não aparece em
resíduo nenhum. Já aconteceu duas vezes.

**1. Âncora de outra cidade** — corrigido em 18/08, ver
`docs/changelog/2026-08-18-uf-por-filtro-e-ancora-podada.md` Parte 2. Um nome raro
no DNE casava com o único cluster homônimo do estado, a centenas de km; a âncora
errada alargava a pegada do município, e cada acerto falso autorizava o próximo.
Em Patrocínio, 46 ruas com centróide em outra cidade, todas `ok`. O `trimOutliers`
mais `--ancora-raio-km=60` levou isso de 19,64% para 0,06% em MG.

**2. O resíduo que sobrou.** Medido em 21/08 sobre o re-join de RJ **já com**
`ancora_raio_km=60` (pasta `G:\dne-geo-rj-join3`): **6.837 das 70.797 linhas `ok`
(9,7%) têm centróide fora do polígono do próprio município.**

Distribuição por distância da borda do município:

| distância da borda | linhas | % das fora |
|---|---|---|
| < 1 km | 1.183 | 17,3% |
| 1–5 km | 1.539 | 22,5% |
| 5–25 km | 3.030 | 44,3% |
| **> 25 km** | **1.085** | **15,9%** |

As duas primeiras faixas são ruído honesto: via de divisa, centróide caindo do
lado de fora por pouco, malha simplificada. As duas últimas (**4.115 linhas**)
não têm essa desculpa. Exemplos reais, todos `ok` no arquivo:

| logradouro | cidade | CEP | coordenada gravada | fora |
|---|---|---|---|---|
| Rua das Colinas | Macaé | 27960271 | -22,8400 / -42,0608 | 45 km |
| Rua Nova | Guapimirim | 25946172 | -22,7832 / -43,3948 | 38 km |
| Rua Quatro | Nova Iguaçu | 26050282 | -22,8880 / -43,1027 | 34 km |
| Rua Alan Kardec | Guapimirim | 25949305 | -22,7304 / -43,3330 | 30 km |

Hoje nada no pipeline enxerga isso. Foi preciso alguém tropeçar por acaso,
montando um bbox para um recorte PMTiles.

## O encaixe (por que é barato)

- **Cada linha do DNE já carrega o código IBGE do município**, coluna 14 do
  `DNE_GEO_LOGRADOURO_{UF}.TXT` (1-based, delimitador `@`). Conferido: 103.723 de
  103.723 linhas de RJ têm código de 7 dígitos válido. Distrito herda o IBGE do
  município de subordinação (já implementado, `dne-geo-join.js` linha ~193).
  **Não há nome a casar.**
- **A malha vem da mesma API do IBGE que gerou o `uf-poly.json`**, trocando o
  parâmetro `intrarregiao`:

      https://servicodados.ibge.gov.br/api/v3/malhas/estados/33?formato=application/vnd.geo+json&qualidade=maxima&intrarregiao=municipio

  (`33` = RJ). Baixado em 21/08: **HTTP 200, 816 KB, 1,2 s, 92 features, 36.982
  pontos**, `properties.codarea` = IBGE de 7 dígitos, geometria `MultiPolygon`.
- **O point-in-polygon indexado já existe**: `uf-poly.js` faz grade
  célula → candidatos, O(1) amortizado. O `scripts/build-uf-poly.js` é o molde
  para um `build-mun-poly.js`.

## A tarefa

1. **`scripts/build-mun-poly.js`** (ou uma flag no `build-uf-poly.js`) que baixa a
   malha municipal e grava `mun-poly.json` versionado, no mesmo padrão do
   `uf-poly.json`: Douglas-Peucker eps 0,005°, coordenadas com 4 casas, URL e data
   da baixa gravadas no próprio arquivo.
   - Orçamento de tamanho: o `uf-poly.json` ficou em 398 KB para 27 polígonos e
     23.631 pontos. Municipal são 5.570 polígonos. **Meça antes de decidir**: se
     não couber, restrinja aos municípios que têm logradouro no DNE — são
     **636 no Brasil inteiro** (36 em RJ; o resto do país é CEP único, sem rua).
   - Baixar por UF e concatenar é mais seguro que puxar o Brasil de uma vez.

2. **`mun-poly.js`** — API espelhando `uf-poly.js`: carrega o JSON, indexa numa
   grade, expõe algo como `dentroDoMunicipio(ibge, lat, lng)` e
   `distanciaDaBordaKm(ibge, lat, lng)`. Sem rede em runtime, sem dependência nova.

3. **Verificação no `dne-geo-join.js`**, ao fim do run:
   - conta linhas `ok` fora do polígono do próprio IBGE;
   - **quebra por faixa de distância da borda** (`<1`, `1–5`, `5–25`, `>25` km) — a
     faixa é o que separa ruído de divisa de erro de verdade, e sem ela o número
     não serve para decidir nada;
   - grava no `DNE_GEO_RELATORIO_{UF}.json` e loga um resumo;
   - **opcional, útil**: uma amostra das N piores no relatório (nome, cidade, CEP,
     coordenada, km fora), para o diagnóstico não exigir um script novo toda vez;
   - flag para desligar (`--sem-validacao-poligono`); e se o `mun-poly.json` não
     existir, **degrade, não quebre** — loga `[skip]` e segue.

4. **Não alterar** `geo_status` nem a escolha de candidato nesta tarefa.

## Critérios de aceite

- Rodar o join de RJ com os insumos abaixo e o relatório reportar **~6.837 linhas
  `ok` fora do polígono**, com a distribuição por faixa batendo com a tabela
  acima. O número de referência foi medido com a malha `qualidade=maxima` **sem**
  simplificar; alguma diferença depois do Douglas-Peucker é esperada, mas a ordem
  de grandeza tem que bater.
- `Rua das Colinas` / Macaé / CEP 27960271 tem que aparecer entre as piores.
- Teste unitário do point-in-polygon com ponto dentro, fora, e **exatamente sobre
  a divisa** — o `uf-poly.js` já teve de decidir esse caso, siga a mesma convenção
  (`test/uf-poly.test.js`).
- Sem `mun-poly.json`, o join roda igual e loga o skip.

## Insumos no disco

| o que | onde |
|---|---|
| join de RJ já corrigido (`ancora_raio_km=60`) | `G:\dne-geo-rj-join3\DNE_GEO_LOGRADOURO_RJ.TXT` |
| join de MG e SP corrigidos | `G:\dne-geo-mg-join3`, `G:\dne-geo-sp-join3` |
| extract OSM de RJ (shards) | `G:\osm-geo-br-rj\OSM_LOGRADOURO_RJ` |
| geometria re-rotulada por polígono de UF | `G:\osm-geo-br-uf` |
| DNE Delimitado | `D:\dev\ddsoft\ddsoft-online\_ignore\Delimitado` |
| malha municipal de RJ já baixada | `C:\Users\Rafael\AppData\Local\Temp\malha-rj.json` |

Comando que gerou o join de RJ, para reproduzir:

```bash
node dne-geo-join.js --dne="D:\dev\ddsoft\ddsoft-online\_ignore\Delimitado" \
    --osm="G:\osm-geo-br-rj" --out="G:\dne-geo-rj-join3" --uf=RJ
```

## Armadilhas

- **Coluna 14 é 1-based** no `cut` e no `awk`; em split por `@` em JS ou Python é
  o índice 13. Layout: 1 log_nu, 2 uf, 3 loc_nu, 4 bai_nu, 6 nome, 8 CEP, 9 tipo,
  12 cidade, 13 bairro, **14 IBGE**, 15 lat, 16 lng, 17–20 bbox, 21 geo_status,
  22 regra, 26 osm_way_ids.
- **Só 36 cidades aparecem no arquivo de RJ** e 636 no Brasil. Não é bug: o DNE só
  lista logradouro para município com CEP por rua. O resto é CEP único. Cuidado ao
  usar `localidades.total` do relatório como denominador — são 11.204, e isso faz
  a cobertura de pegada parecer catastrófica quando não é.
- **A malha do IBGE usa `[lng, lat]`** (GeoJSON); o resto do pipeline usa
  `[lat, lng]`.
- `MultiPolygon` tem anel externo e buracos. Ray casting com todos os anéis
  concatenados trata buraco corretamente por paridade, mas confirme no teste.
- **Não confunda com a UF por polígono**, que já existe (`uf-poly.js`,
  `docs/changelog/2026-08-18-uf-por-poligono.md`). Aquilo decide a UF de um ponto
  do OSM na extração. Isto valida o município de uma linha já casada.

## Contexto adicional, se precisar

- `docs/changelog/2026-08-18-uf-por-filtro-e-ancora-podada.md` — o defeito da
  âncora e o `--ancora-raio-km`.
- `docs/changelog/2026-08-18-uf-por-poligono.md` — a malha de UF, o orçamento de
  tamanho, a decisão de divisa e de mar.
- `ddsoft-online/docs/changelog/2026-08-18-geometria-de-via-mg-atribuicao-de-uf.md`
  — o lado consumidor, e por que "contar o join" mede a coisa errada.

## O que NÃO é esta tarefa (mas foi medido, aproveite)

Substituir a pegada por âncoras pelo polígono **dentro** do join, como filtro de
candidato. Foi medido em 21/08 sobre as 15.694 ambíguas de RJ:

| | linhas | % |
|---|---|---|
| 1 candidato dentro do município — recuperável | 747 | 4,8% |
| vários dentro — segue ambíguo | 1.754 | 11,2% |
| **nenhum dentro — o OSM não tem a rua ali** | **13.193** | **84,1%** |

Como **recuperador** de ambíguas o polígono rende pouco, porque o teto é cobertura
do OSM: Macaé tem 3.002 logradouros no DNE e 1.562 nomes distintos no OSM num raio
de 15 km. O valor do polígono está em ser **filtro** — eliminar por construção a
categoria do falso positivo distante, hoje defendida por heurística que deixa
passar ~10%.

Se for fazer o filtro depois, **dê folga de borda**: cortar sem tolerância joga
fora as 1.183 linhas legítimas a menos de 1 km da divisa. Algo entre 1 e 2 km
parece o ponto, pelos números acima.

## Estado do banco em 21/08 (contexto, não é tarefa)

Há um problema separado, do lado do `ddsoft-online`: o `osm:dne:enrich-geo` **só
preenche `lat` NULL** por padrão, e o `DneOsmGeoEnricher` nunca escreve NULL. Logo
as linhas que o join corrigido rebaixou de `ok` para `ambiguo` mantêm no banco a
coordenada velha e errada — em RJ são ~8.400. A correção é

```sql
UPDATE dne_idx_logradouro
   SET lat = NULL, lng = NULL,
       lat_min = NULL, lat_max = NULL, lng_min = NULL, lng_max = NULL
 WHERE ufe_sg = 'RJ';
```

antes do `enrich-geo --overwrite`. Não é tarefa deste documento, mas se você medir
o banco e os números não baterem com os arquivos, é por isso.
