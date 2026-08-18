# 2026-08-18 — o join e o enrich passam a ler a pasta re-rotulada

Fecha a trinca do dia: [o polígono](./2026-08-18-uf-por-poligono.md) →
[o re-rótulo dos artefatos](./2026-08-18-relabel-uf-sem-reextrair.md) → **o consumidor
apontado para eles**.

## Prompt

> faça a atualização do join/enrich

## O que faltava

O re-rótulo de ontem cobriu só `logradouro` e `geom`, que era o que a pasta
`G:\osm-geo-br-geom` tinha. Mas o join lê `OSM_ADDR_POINT_{UF}` e o `osm:dne:enrich-geo`
(legado) lê `OSM_BAIRRO` — datasets que só existem nos extracts regionais de 30/07–13/08, com
o mesmo rótulo de retângulo. Sem eles, apontar o join para a pasta nova seria trocar um dado
errado por um dado faltando.

## O que foi implementado

### `scripts/relabel-uf.js` — mais duas famílias, e um guarda

- **Tabela de colunas por dataset.** Cada um põe id/tipo/uf/coordenada num lugar
  (`README-colunas.md`): logradouro em 0/19/4/10-11, bairro em 1/0/4/8-9, addr em 0/–/–/1-2.
  Virou `SPEC`, e o laço de re-rótulo ficou um só para as quatro famílias.
- **`OSM_BAIRRO` é dataset único** (a UF é coluna, não sufixo): o que muda nele é a coluna,
  não o nome do arquivo. **`OSM_ADDR_POINT_{UF}` não tem coluna `uf`** — o rótulo antigo é o
  sufixo do nome do dataset.
- **`--dirs=`** aponta fatias avulsas: as pastas `G:\osm-geo-br-{regiao}` não são subpastas de
  uma base comum. **`--datasets=`** escolhe as famílias.
- **Guarda contra regravar por cima.** Rodar de novo a mesma família na mesma `--out`
  reabriria o shard `000001` do zero e comeria o que estava lá. Agora recusa, listando o que
  já existe; `--force` libera. (Achado escrevendo isto, não em produção.)

### Docs apontados para a pasta nova

`operacao-comandos.md`, `proximo-passo-brasil.md`, `dne-geo-join.md` e
`geometria-via-destaque.md`: `--osm=`, `--geom=`, `--geom-dir=` e o `--dir=` do enrich passam a
ser `G:\osm-geo-br-uf` (e `G:\dne-geo-uf` para o enrich, que lê a saída do join). A tabela
"qual pasta regional serve qual UF" ficou marcada como superada — são 27 UFs numa pasta só.

## As corridas

### 1. `bairro` + `addr` das nove pastas regionais → `G:\osm-geo-br-uf` (9,2 s)

| família | lidas | duplicadas | mudaram de UF | gravadas |
|---|---:|---:|---:|---:|
| addr | 205.660 | 0 | **17.175** | 205.660 |
| bairro | 103.687 | 50.130 | **13.280** | 53.557 |

Maiores: `SP→MG` 14.249 (addr) e 3.093 (bairro), `SC→RS` 1.234, `PR→SP` 1.089, `ES→BA` 1.002,
`RJ→MG` 872+781, `PB→PE` 768, `GO→MG` 537.

Os 2.696 bairros que caíram em `XX` **já vinham sem UF e sem coordenada** na origem (bairros
mapeados como way sem centróide — Rocinha e Vidigal estão entre eles). Não é regressão: é o
mesmo buraco do extract, agora visível.

### 2. Join de MG, mesmo código, só trocando a pasta OSM

| `--osm=` | logradouros com coordenada | cobertura |
|---|---:|---:|
| `G:\osm-geo-br-sudeste` (rótulo antigo) | 60.038 | 46,6 % |
| `G:\osm-geo-br-uf` (polígono) | **95.856** | **74,3 %** |

**+35.818 logradouros de MG ganharam coordenada** só por o dado estar no arquivo certo. O que
mudou por baixo: o join passou a ver 339.879 linhas de OSM em MG em vez de 190.674, e
`ambiguo.fora_do_footprint` caiu de 22.519 para 11.883 — a mancha do município ficou completa,
então a via deixou de parecer fora dela.

(O relatório de produção de 30/07 marcava 63.378 / 49,2 %, mas aquele número mistura código
antigo com dado antigo; a tabela acima é código de hoje nas duas linhas.)

### 3. Join nacional das 27 UFs → `G:\dne-geo-uf` (~35 min, 0 falhas)

Contra a produção atual (`G:\dne-geo-br`): **777.574 → 883.382** logradouros com coordenada,
**63,9 % → 72,6 %** de 1.216.330 linhas de DNE. **+105.808.**

| UF | antes | depois | | UF | antes | depois |
|---|---:|---:|---|---|---:|---:|
| AC | 84,5 % | 85,9 % | | PB | 66,3 % | 73,8 % |
| AL | 53,8 % | 67,4 % | | PE | 19,2 % | **61,5 %** |
| AM | 66,9 % | 71,2 % | | PI | 62,7 % | 63,0 % |
| AP | 72,0 % | 74,5 % | | PR | 79,0 % | 83,6 % |
| BA | 64,2 % | 66,7 % | | RJ | 65,1 % | **68,3 %** |
| CE | 55,6 % | 63,8 % | | RN | 68,4 % | 71,0 % |
| DF | 25,1 % | **37,7 %** | | RO | 79,4 % | 80,1 % |
| ES | 66,8 % | 70,4 % | | RR | 92,0 % | 93,7 % |
| GO | 61,6 % | 64,5 % | | RS | 61,8 % | **76,9 %** |
| MA | 66,0 % | 69,9 % | | SC | 88,7 % | 91,2 % |
| MG | 49,2 % | **74,3 %** | | SE | 68,9 % | 72,9 % |
| MS | 80,1 % | 80,9 % | | SP | 74,3 % | **78,0 %** |
| MT | 75,3 % | 75,3 % | | TO | 45,2 % | 39,5 % |
| PA | 43,2 % | **66,2 %** | |  | |  |

Duas UFs aparecem piores nessa tabela — **TO (−490) e MT (−2)** —, mas a comparação é injusta:
`G:\dne-geo-br` foi gerado com o código de 30/07. Rodando as duas com o código de hoje, só
trocando a pasta OSM:

| UF | `--osm=` regional | `--osm=G:\osm-geo-br-uf` |
|---|---:|---:|
| TO | 3.291 (38,6 %) | **3.360 (39,5 %)** |
| MT | 16.895 (72,1 %) | **17.636 (75,3 %)** |

TO **melhorou** +69. O que ele perdeu foi lixo: o dataset de TO tinha 51.144 linhas e agora tem
25.496, porque 11.507 ways eram do PA e 9.239 do MA (o retângulo de TO é pequeno, então ele
vencia o desempate contra PA/MA). As linhas de DNE que casavam com aqueles ways casavam com via
de outro estado — `sem_nome_osm` subiu de 4.315 para 4.526 exatamente por isso, e é o número
honesto.

O maior salto é **PE, 19,2 % → 61,5 %**: a caixa da PB cobria o agreste pernambucano e levava
70.237 logradouros embora.

## Como foi testado

Automatizado (`npm test`): **165 testes, 163 passam, 2 skip** — eram 163/161/2.

`test/relabel-uf.test.js` ganhou: bairro re-rotulado na coluna sem trocar de arquivo, addr
reparticionado sem coluna `uf`, `--datasets` acrescentando à mesma pasta, `--dirs` apontando
fatia avulsa, e o guarda recusando regravar família já existente.

Campo: as três corridas acima, mais as duas conferências de TO e MT com o código de hoje.

## O que fica em aberto

- **Load no DDSOFT**: `osm:dne:enrich-geo --dir=G:\dne-geo-uf` (é comando do outro repositório;
  aqui só a doc mudou).
- As pastas antigas (`G:\osm-geo-br-*`, `G:\dne-geo-br`) continuam intactas, como histórico.
- Re-extract com o código novo continua sendo o único jeito de trazer o que o filtro de
  retângulo nunca deixou entrar (Noronha, Trindade, faixas de AP e PI).
