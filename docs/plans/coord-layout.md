# Plano: mapa de lat/lon ao longo do stream (geocode layout)

> Plano aprovado e versionado no repositório. Implementação em `coord-layout.js` + `index0.js`.

## Problema

Hoje o script só **conta** sinais (`nodeWithLatLon`, `addrStreet`, …). Isso diz *se* há material de geocoding, mas não **onde no arquivo** as coordenadas aparecem nem se estão **ordenadas** (geograficamente contíguas na ordem do XML) ou **espalhadas**.

Salvar todas as lat/lon do planet é inviável. O stats já foi pensado para **situar conteúdo** (bzip membro/bloco, `chunkPos`, pilha XML). Esta fase estende isso com um **mapa compacto de coordenadas × posição no stream**.

## Objetivo

Responder, com armazenamento limitado (O(blocos) + O(amostras fixas)):

1. Em que **trechos do `.bz2`** (membro/bloco bzip, bytes lidos) concentram-se nodes com `lat`/`lon`?
2. As coordenadas ao longo do XML são **sequenciais** (vizinhos no arquivo ≈ vizinhos no mapa) ou **quase aleatórias**?
3. Quando entram **sinais de endereço** (`addr:*` / `name` / `place`), em que posições isso ocorre?
4. Dados úteis no `*-stats.json` / `*-results.json` para retomar e inspecionar.

Fora de escopo: geocoder, índice completo, gravação de todos os pontos.

## Desenho

### Resolução

| Nível | Unidade | Custo |
|-------|---------|--------|
| Bloco bzip | cada `readBlock` com nodes | 1 registro por bloco com dados |
| Amostra global | reservoir fixo | ex. 512 pontos |
| Sequência | entre nodes consecutivos | O(1) estado |

### Distância em graus (não km)

```js
jumpDeg = Math.sqrt(dLat * dLat + dLon * dLon)  // wrap de lon opcional
```

Não é Mercator nem haversine — plano de graus equiretangular. Limiar default `jumpSmallDeg = 0.01`.

### Node com endereço

Pending em open `node` → flags em tags filhas → close `node` atualiza layout.

### Persistência

`coordLayout` em stats e results; resume restaura.

## Critérios de sucesso

1. Grade sequencial → `pctSmallJumps` alto  
2. Ordem embaralhada → `meanJumpDeg` alto  
3. Stats pequeno  
4. Progresso com `lay …`  
5. Resume preserva layout  

## Status

- [x] Plano salvo em `docs/plans/coord-layout.md`
- [ ] Implementação + testes + docs de referência dos campos
