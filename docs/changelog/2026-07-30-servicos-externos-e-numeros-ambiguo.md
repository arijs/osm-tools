# 2026-07-30 — números do `ambiguo` atualizados + pesquisa de serviços externos

## Prompt original

> acho que precisa atualizar a seção de ambíguo com os novos números, e será que podemos consultar
> algum serviço externo pra recuperar as coordenadas dos logradouros que faltam? Veja se existem
> serviços que façam isso gratuitamente

## 1. Números do `ambiguo` corrigidos

A seção ainda mostrava a decomposição de **antes** da âncora local. Atualizada com o relatório de
`G:\dne-geo-local`:

| Motivo | Antes | Agora |
|--------|------:|------:|
| `fora_do_footprint` | 22 241 | 9 804 |
| `conflito_municipio` | — (não existia) | 8 374 |
| `extensao_longa` | 37 | 37 |
| `empate_de_tamanho` | 3 | 3 |
| **Total** | **22 281** | **18 218** |

Faixas de distância também refeitas: 16 682 (92 %) continuam a menos de 1 km da mancha do município.

## 2. Serviços externos — pesquisados e **testados na nossa base**

Não avaliei por documentação: consultei os serviços com os CEPs que efetivamente faltam.

### Descartados

- **Nominatim / Photon / Overpass** — são o próprio OSM. Nossa lacuna é rua que o OSM não nomeia;
  falham nas mesmas. E a [política de uso do Nominatim](https://operations.osmfoundation.org/policies/nominatim/)
  proíbe geocodificação em massa na instância pública (1 req/s, 1 máquina, 4 req/min para scripts
  recorrentes) — volume nosso exigiria instância própria.
- **[BrasilAPI](https://brasilapi.com.br/docs) `/cep/v2`** — testado em 5 CEPs de SP, **inclusive
  Avenida Paulista**: `location.coordinates` veio **vazio em todos**. Devolve só o endereço, que já
  temos da mesma base DNE.
- **[banco-ceps](https://github.com/gpfconfea/banco-ceps)** (MIT, offline) — coordenadas derivadas de
  Nominatim + AwesomeAPI + scraping; a própria documentação chama o scraping de "variável e
  inconsistente". Herda os erros das duas fontes.
- **Comerciais** (Google, HERE, Mapbox, LocationIQ, OpenCage, Geoapify) — free tiers de 2,5 k a
  250 k/mês, mas quase todos proíbem **armazenar** o resultado. Como o produto é base persistida, o
  licenciamento mata antes do preço.

### Viável, com ressalva: AwesomeAPI CEP

Base própria (Correios + IBGE), não OSM — por isso tem chance real de cobrir nossa lacuna.

**Aferição contra 40 logradouros que já validamos** (`ok`, `exato`, cluster único, ≥3 ways):
40/40 responderam; erro mediano **0,16 km**; p90 1,38 km; 85 % dentro de 500 m; **pior caso 319 km**.

O pior caso é erro deles, comprovado: CEP `14165352` = `Rua Joaquim Ferreira, Sertãozinho`. Nossa
coordenada cai em Sertãozinho (−21,11, −47,98); a deles, na zona sul da capital (−23,75, −46,73) —
contradizendo o campo `city` da própria resposta.

**Teste nas 40 linhas que faltam na capital:** 40/40 responderam, **37 (93 %) caem dentro do
município certo**, 3 fora. Os 3 têm o mesmo padrão: `Praça Cidade de Itu` recebeu coordenada **na
cidade de Itu**; `Rua Ezequiel Wanderley` caiu a 100 km. Para CEP que eles não têm, a resposta
parece ser geocodificação ingênua pelo nome.

### Conclusão registrada no doc

O erro deles é **detectável com o que já construímos**: a pegada municipal e a âncora local rejeitam
exatamente esse tipo de resposta. Desenho proposto — consultar só as faltantes, validar contra o
footprint, aceitar como `geo_regra=cep_externo` com **bbox vazia** (é ponto, não traçado), rejeitado
continua vazio. Rendimento estimado: ~93 % das faltantes, ~5 600 das 6 008 da capital.

**Não implementado**, por dois bloqueios que não são técnicos:

- **Volume:** free tier é [100 mil req/mês com chave](https://docs.awesomeapi.com.br/aviso-sobre-limites);
  o Sudeste precisa de ~164 mil.
- **Licenciamento:** a documentação não diz se pode armazenar/redistribuir a coordenada. Base
  persistida no ddsoft exige resposta escrita deles antes.

## Como foi testado

Documentação e pesquisa — nenhum código de produção alterado, nada a rodar na suite.
As medições foram feitas com scripts descartáveis no scratchpad, consultando as APIs públicas com
500 ms entre requisições (80 requisições no total, bem dentro de qualquer uso justo):

- comparação BrasilAPI × AwesomeAPI nos mesmos 5 CEPs (2 que temos, 3 que faltam);
- aferição de 40 logradouros resolvidos contra a AwesomeAPI;
- teste de 40 logradouros faltantes com validação por bbox do município.

## Arquivos

- `docs/geo/dne-geo-join.md` — seção do `ambiguo` com números novos + nova seção "Serviço externo
  para o que falta?"

## Fontes

- https://operations.osmfoundation.org/policies/nominatim/
- https://brasilapi.com.br/docs
- https://docs.awesomeapi.com.br/api-cep
- https://docs.awesomeapi.com.br/aviso-sobre-limites
- https://github.com/gpfconfea/banco-ceps
- https://www.cepaberto.com/
- https://scrap.io/free-geocoding-api-comparison-2026
