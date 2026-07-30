# 2026-07-30 — âncora local por bairro/CEP no `dne-geo-join`

## Prompt original

> Implemente!

Referindo-se ao plano da seção "Plano para o resíduo `ambiguo`" de `docs/geo/dne-geo-join.md`.

## O que foi implementado

**Fase 5b** no `dne-geo-join.js`: 3ª volta sobre as linhas `fora_do_footprint`.

1. Junta as vias **já resolvidas** por `bai_nu` e por `loc_nu|CEP-5`.
2. Para cada linha do resíduo, monta a âncora local (mínimo 3 vizinhas; bairro primeiro, CEP como
   fallback) — centro e raio.
3. Aceita o candidato mais próximo se cair dentro de `raio + --local-tol-km` (default 1 km).
4. `geo_regra` ganha sufixo: `exato+local_bairro`, `nucleo+local_cep`, etc.

**Resolução de conflito** — a parte que faz a regra valer. Uma via física é de uma cidade só; se a
âncora local levou dois municípios ao mesmo cluster, no máximo um está certo:

- quem casou pelo footprint ganha do `+local`;
- entre dois `+local`, ganha o mais próximo da própria vizinhança;
- o perdedor volta a `ambiguo`, com motivo `conflito_municipio`.

Opções novas: `--sem-ancora-local`, `--local-tol-km=N`.

## Por que o conflito não é detalhe

O gate da spec era: medir a razão ganho/risco antes de ligar. Sem a resolução de conflito, a âncora
local **reprovava**:

| Estratégia | `ok` | Δ | Clusters usados por 2+ municípios | Δ |
|------------|-----:|--:|----------------------------------:|--:|
| Baseline (footprint só) | 263 478 | — | 8 691 | — |
| Dilatação 2 (testada antes, descartada) | 266 957 | +3 479 | 12 143 | +3 452 |
| Âncora local **sem** conflito | 275 915 | +12 437 | 15 315 | **+6 624** |
| **Âncora local com conflito** | **267 541** | **+4 063** | **8 691** | **0** |

Recuperar 12 437 e reverter 8 374 parece desperdício, mas é o que separa recuperar de inventar:
mais linhas que a dilatação e **zero** cluster disputado a mais.

## Resultado por UF

| UF | Antes | Depois | Recuperadas | Revertidas por conflito |
|----|------:|-------:|------------:|------------------------:|
| SP | 77,1 % | **78,3 %** | +4 063 | 8 374 |
| RJ | 71,9 % | **73,4 %** | +1 557 | 4 732 |
| MG | 54,7 % | **57,3 %** | +3 267 | 7 793 |
| ES | 74,9 % | **75,6 %** | +228 | 309 |

Capital paulista: 88,6 % → **88,8 %**. O ganho é maior no interior, onde a pegada de âncoras é mais
esburacada — MG subiu 2,6 pontos.

## Como foi testado

**Suite completa:** `node --test test/*.test.js` → **88 testes, 86 pass, 0 fail, 2 skip** (os 2 skip
são fixtures grandes, já puladas antes).

**Testes novos** em `test/dne-geo-join.test.js`:

- A fixture ganhou `Rua Periferia`: existe no OSM a ~3 km ao sul, **fora** da pegada dilatada, mas
  perto das vias já resolvidas do mesmo bairro. Assertiva: sai `ok` com `geo_regra` =
  `exato+local_bairro`.
- Teste espelho com `ancoraLocal: false`: a mesma linha fica `ambiguo`, **sem coordenada**, e o
  relatório acusa `fora_do_footprint = 1`. Garante que o ganho vem da fase nova, não de outro
  caminho.

**Verificação na base real de SP:**

- Os 4 casos de regressão saem com as coordenadas inalteradas (`Augusta`, `Paulista`, `Estoril`,
  `Maracujá Natal`).
- Detector de bbox > 15 km: só as vias longas legítimas (`Rodovia Arão Sahm`), nenhuma nova.
- Detector de área casada com `TLO_TX` de via: 0.
- Razão ganho/risco medida contando geometrias idênticas reivindicadas por `loc_nu` diferentes.

**Armadilha repetida, registrada na fixture:** a primeira versão do teste falhou porque
`Rua Periferia`, sendo nome único com um cluster só, **virava âncora do próprio município** e entrava
na pegada sem passar pela 3ª volta. Foi preciso dar a ela um homônimo distante. É o mesmo erro que já
tinha acontecido com o teste do distrito — está comentado nos dois lugares.

## Arquivos

- `dne-geo-join.js` — fase 5b, resolução de conflito, `--sem-ancora-local`, `--local-tol-km`
- `test/dne-geo-join.test.js` — fixture ampliada + teste espelho
- `docs/geo/dne-geo-join.md` — seção do resíduo reescrita com o resultado

## Sobra no disco

Saídas em `G:\dne-geo-local` (versão nova). `G:\dne-geo`, `G:\dne-geo-d2` e `G:\dne-geo-d3` são as
execuções de comparação — podem ser apagadas.

## O que ainda fica de fora

Os 18 218 `ambiguo` restantes de SP: 8 374 são `conflito_municipio` (dois municípios com direito ao
mesmo cluster — precisaria de um sinal geográfico melhor que o CEP, tipo `addr:postcode` do OSM),
e o resto é candidato longe demais de qualquer vizinhança conhecida. Ficam vazios.
