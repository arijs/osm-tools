# 2026-08-18 — a UF do ponto sai do polígono, não do retângulo

Continuação direta de
[2026-08-18-uf-por-filtro-e-ancora-podada.md](./2026-08-18-uf-por-filtro-e-ancora-podada.md):
aquilo foi paliativo declarado, isto é a correção de fato.

## Prompt

> Resolver a UF por **geometria real** — polígono de UF (ou município → IBGE → UF), com
> point-in-polygon —, mantendo a API pública de `uf-br.js` […] `passesUfFilter` deve passar a
> usar a **mesma** fonte de verdade da nomeação. […] Precisa ser O(1) amortizado: indexe os
> polígonos numa grade (célula → lista de candidatos) e só rode point-in-polygon nos
> candidatos. […] Sem rede em runtime e sem dependência nova […] Ponto exatamente sobre a
> divisa e ponto no mar precisam de resposta definida: escolha uma e teste-a.

## O que estava errado depois do paliativo

O paliativo fez o rótulo concordar com o **filtro do run**: sem tag e sem IBGE, quem nomeava
era a caixa permitida que contivesse o ponto. Isso conserta o pipeline (que sempre roda
filtrado) e foi validado em campo, mas deixou dois buracos:

1. `resolveUf` **sem filtro** continuava entregando Patrocínio para GO e Montes Claros para BA.
2. Com filtro, o retângulo permitido nomeia **tudo** que cai dentro dele. O retângulo de MG
   cobre pedaços de SP, RJ, ES, GO e BA — então o run `--uf=MG` passou a chamar de MG a praça
   do Bodegão (Rio) e a Praça 207 (Brasília).

Medido no artefato do run de 18/08 (`G:\osm-geo-br-geom\mg\OSM_LOGRADOURO_MG`, 1 007 314
linhas, todas gravadas como MG):

| onde o ponto está de verdade | linhas | % |
|---|---:|---:|
| MG | 339 065 | 33,7 % |
| SP | 286 313 | 28,4 % |
| RJ | 153 926 | 15,3 % |
| GO | 109 597 | 10,9 % |
| ES | 65 849 | 6,5 % |
| DF | 31 183 | 3,1 % |
| BA | 21 069 | 2,1 % |
| PR / MT / MS | 312 | 0,03 % |
| **XX** | **0** | — |

## O que foi implementado

### `uf-poly.json` — a malha, versionada

- Fonte: **API de malhas v3 do IBGE**, `paises/BR?qualidade=maxima&intrarregiao=UF`,
  baixada em **18/08/2026**. Licença: dado público do IBGE, uso livre com citação da fonte —
  a URL e a data ficam gravadas no próprio arquivo.
- Simplificação **Douglas-Peucker, eps = 0,005°** (~550 m), coordenadas com 4 casas (~11 m).
- **Orçamento de tamanho: 398 KB**, 23 631 pontos (a malha crua tem 52 mil pontos / 1,0 MB).
  A escolha do eps foi medida nas 32 coordenadas dos testes:

  | eps | tamanho | erros |
  |---|---:|---|
  | 0,002° | 730 KB | nenhum |
  | **0,005°** | **398 KB** | **nenhum** |
  | 0,01° | 216 KB | Juazeiro / BA vira **PE** |
  | 0,02° | 111 KB | Juazeiro / BA vira **PE** |

  Ou seja: 0,005° é o ponto onde parar. Metade do tamanho de 0,002° sem perder nenhuma
  resposta, e a metade seguinte já move a divisa do São Francisco por cima de uma cidade.
- Uma UF por linha no JSON — atualizar a malha rende um diff legível.

### `scripts/build-uf-poly.js` — como regerar

`node scripts/build-uf-poly.js` baixa, simplifica e reescreve `uf-poly.json`
(`--in=arquivo.json`, `--eps=`, `--decimals=`). Só `https` do próprio Node; **nenhuma
dependência nova**, e o runtime nunca vai à rede.

### `uf-poly.js` — point-in-polygon indexado

Grade de **0,25°** montada na primeira consulta (~250 ms):

- célula inteiramente dentro de uma UF → a resposta está na célula, **sem PIP**;
- célula cortada por divisa/costa → lista de candidatas, e só aí roda PIP;
- célula sem UF nenhuma → `''`.

Só **2 170 de 28 440** células (7,6 %) são ambíguas. Par-ímpar sobre todos os anéis da UF, o
que resolve buraco (enclave) sem código extra. Memo de uma posição porque o extract pergunta o
mesmo ponto duas vezes seguidas (rótulo e filtro).

Casos de borda, escolhidos e testados:

- **ponto exatamente sobre a divisa** conta como dentro das duas UFs, e vence a primeira em
  ordem alfabética — arbitrário, mas idêntico entre execuções, que é o que um pipeline
  retomável exige;
- **ponto fora de todos os polígonos** (mar, plataforma, fora do Brasil) responde `''` e o
  chamador cai no retângulo antigo — assim nada que hoje tem rótulo passa a ser `XX`.

### `uf-br.js` — mesma API, outra fonte de verdade

| função | antes | agora |
|---|---|---|
| `resolveUf` | tags → IBGE → retângulo | tags → IBGE → **polígono** → retângulo (só fora do polígono) |
| `resolveUfFiltered` | idem, com a caixa **permitida** nomeando | polígono, igual com ou sem filtro; a muleta da caixa permitida ficou só para o ponto fora de todos os polígonos |
| `passesUfFilter` | ponto em caixa permitida → mantém | **polígono**: fora das UFs permitidas → **descarta** (retângulo só fora do polígono) |
| `ufFromPoint`, `UF_BBOX` | decidiam | continuam exportados, como atalho e último recurso |

Assinaturas e ordem de precedência intactas. Novos: `ufFromPointPoly`, `ufFromPointGeo`.

**A decisão que o prompt pediu para documentar:** feature que o polígono põe fora de todas as
UFs permitidas é **descartada do run**, não renomeada. Nada se perde nacionalmente — as oito
fatias da receita cobrem as 27 UFs, e a feature aparece na fatia dela. O que muda é que a
pasta de uma fatia deixa de conter vizinhança geométrica; só sobra vizinho **declarado por tag
ou IBGE**, onde o dado explícito manda e o nome do arquivo está certo.

## Antes / depois

| coordenada | lugar | antes (`resolveUf`) | agora |
|---|---|---|---|
| −18,9375 / −46,9856 | Patrocínio / MG | **GO** | MG |
| −18,9186 / −48,2772 | Uberlândia / MG | **GO** | MG |
| −16,7350 / −43,8617 | Montes Claros / MG | **BA** | MG |
| −20,5386 / −47,4008 | Franca / SP | SP (por sorte da área) | SP |
| −16,6799 / −49,2550 | Goiânia / GO | GO | GO |
| −22,4149 / −47,5651 | Rio Claro / SP | SP | SP |
| −15,8159 / −48,1097 | Ceilândia / DF | DF (por sorte da área) | DF |
| −22,9231 / −43,6918 | Praça do Bodegão, Rio | MG no run `--uf=MG` | RJ, e fora do run |

## Desempenho

Medido nesta máquina, Node 22:

| | |
|---|---|
| montagem da grade | ~250 ms, uma vez por processo |
| consulta | **~1,0 milhão/s** em pontos aleatórios sobre o Brasil (500 mil em 503 ms) |
| retângulo (o que havia antes) | ~5 M/s |

O job `mg` levava ~1h30. Mesmo num limite absurdo de 50 milhões de chamadas, o polígono custa
~50 s — **menos de 2 %**, e o teto pedido era 10 %. A grade é o que segura isso: sem ela,
varrer 27 polígonos por ponto seria ~50× mais caro.

## Como foi testado

Automatizado (`npm test`): **160 testes, 158 passam, 2 skip** — antes eram 150/148/2; os 2
skips continuam sendo as fixtures `.osm` grandes ausentes, pré-existentes.

- `test/uf-br.test.js` — os quatro casos do paliativo ficaram, com uma mudança: as três
  asserções que documentavam o defeito passaram de `resolveUf` para `ufFromPoint`, porque o
  defeito agora só existe no retângulo puro. Acrescentados: a tabela de seis coordenadas do
  prompt; 20 coordenadas de divisa cobrindo as cinco regiões (Petrolina/Juazeiro a 3 km de
  distância, Barra do Garças/Aragarças a 1 km); DF como enclave de GO; mar e `XX`; e o
  descarte de Catalão/GO num run `--uf=MG`.
- `test/uf-poly.test.js` (novo) — sanidade da grade (27 UFs, < 20 % de células ambíguas), a
  regra da divisa lida da própria malha, fora-do-Brasil, determinismo (mesma semente, mesma
  saída, duas rodadas idênticas) e o teto grosseiro de desempenho (200 mil consultas < 3 s).

Campo: reclassificação de 1 007 314 linhas do `OSM_LOGRADOURO_MG` já extraído (tabela acima).
**`XX` não apareceu em nenhuma linha** — o requisito era que não crescesse.

Manual: 47 coordenadas de capitais e cidades de divisa conferidas uma a uma, incluindo Fernando
de Noronha (→ PE) e dois pontos de mar aberto (→ vazio).

## O que fica em aberto

- **Re-extrair as fatias** com o rótulo novo (fora do escopo desta tarefa: é execução, horas
  por fatia). A fatia `mg` de 18/08 tem 668 mil linhas de vizinhos rotuladas MG.
- **Município → IBGE → UF** continua não implementado; o polígono de UF já resolve o caso que
  motivou tudo, e a malha municipal são 5 570 polígonos (dezenas de MB).
- A malha é de 2026-08-18 e não se atualiza sozinha. Divisa nova (elas mudam raramente) pede
  rodar `scripts/build-uf-poly.js` de novo.
