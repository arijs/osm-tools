# 2026-08-02 — passo a passo pós-extract Brasil → DNE

## Prompt original

> Na pasta G:\, eu já rodei o comando "extract-geocode-pbf.js", uma pasta com os estados e
> municípios do Brasil ('osm-geo-br-admin'), e cinco pastas divididas pelas regiões do Brasil
> ('osm-geo-br-*'). Qual o próximo passo eu devo fazer agora para obter as coordenadas dos
> logradouros e importar todos esses dados nas tabelas dne do ddsoft? Leia a documentação
> existente do projeto, e crie um novo documento com o passo-a-passo mais simples pra eu seguir.

## O que foi implementado

Documentação operacional (sem mudança de código):

1. **`docs/geo/proximo-passo-brasil.md`** — fluxo em 4 caixas (extract → join → load → busca),
   inventário real de `G:\`, script PowerShell para achatar shards, loop de `dne-geo-join.js`
   por UF, comandos ddsoft `osm:locais:enrich-geo` + `osm:dne:enrich-geo`, checklist e
   armadilhas (não usar legacy match; join só lê flat).
2. Links em `docs/geo/README.md` e `docs/geo/operacao-comandos.md`.

## Comparação antes/depois

| Antes | Depois |
|-------|--------|
| Pipeline espalhado em vários docs (SE-centric) | Um guia “você está aqui” para o extract BR regional |
| Join documentado só com `osm-geo-se-streets2` flat | Explicitado: shards → flatten → join → `G:\dne-geo-br` |
| Próximo passo genérico no README | Ponteiro para o guia Brasil |

## Como foi testado

- Leitura cruzada de `docs/geo/*`, `dne-geo-join.js` (só abre `OSM_LOGRADOURO_{UF}.TXT` flat).
- Inspeção de `G:\osm-geo-br-*` e `G:\dne-geo-local` (admin eof; regiões sharded; SE flat;
  join SE já existente).
- Conferência de `LOG_LOGRADOURO_*.TXT` no Delimitado do ddsoft (27 UFs).

## Testes criados/alterados

Nenhum (só docs).
