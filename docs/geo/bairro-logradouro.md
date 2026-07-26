# Próxima fase: coordenadas de bairro e logradouro

## Por que **não** é `UPDATE locais` (ainda)

| Camada | Papel | Bairro / logradouro hoje |
|--------|-------|---------------------------|
| `dne_idx_bairro` / `dne_idx_logradouro` | Índice de **busca** DNE, regenerável | **Sim** — dezenas/centenas de milhares de linhas |
| `locais` | Catálogo global materializado | Quase só até **município**; bairro/logradouro entram no **accept lazy** |
| `locais_tenant` | Endereço operacional do tenant | Número/complemento e materializações privadas |

`locais` **já tem** colunas `lat`/`lng`/bbox, mas **não há cadastro em massa** de tipos 10 e 11.  
Gravar geo “em `locais`” para bairro/logradouro exigiria **criar** essas linhas (ou mudar o produto). O caminho alinhado ao modelo atual:

> **Pôr geo no índice DNE** → a busca já lê `dne_idx_*` → depois, no accept, copiar geo para `locais` / `locais_tenant` se quiser.

## Schema atual do índice (sem geo)

`dne_idx_bairro` (resumo): `bai_nu`, `ufe_sg`, `loc_nu`, `bai_no`, `nome_norm`, faixas CEP, …  
`dne_idx_logradouro`: `log_nu`, `ufe_sg`, `loc_nu`, `bai_nu_ini`, `tlo_tx`, `log_no`, `nome_norm`, `cep`, `log_complemento`, …

**Não há** `lat`/`lng` nessas tabelas (conferido 2026-07).

### Migration (ddsoft) — especificação canônica

A especificação de produto e o DDL conceitual estão em **ddsoft-online**  
`docs/locais-tenant-e-dne/estrutura-dados-endereco.md` **§11.4** (e ponteiro no §7).

Resumo: `lat`/`lng` + bbox opcional em `dne_idx_bairro` e `dne_idx_logradouro` (`DOUBLE NULL`); migration Doctrine à mão (tabelas fora do MWB); opcional `geo_origem` / `geo_atualizado_em`. Índice espacial **não** no 1º passo.

## Extract OSM (osm-tools)

### Comando recomendado (pasta **separada**)

```bash
node extract-geocode-pbf.js G:\sudeste-260725.osm.pbf ^
  --out=G:\osm-geo-se-streets ^
  --datasets=bairro,logradouro

set NODE_OPTIONS=--max-old-space-size=8192
```

**Por que pasta separada:** sem `--resume`, o extract **apaga todos** os `OSM_*.TXT` da pasta. Não misturar com `G:\osm-geo-se` dos municípios já validados.

### O que esperar

| Dataset | Volume SE (ordem de grandeza) | Geometria |
|---------|------------------------------:|-----------|
| Bairro | dezenas de k no OSM; ~39 k no DNE | node com ponto (single-pass) |
| Logradouro | ~1,2 M ways nomeadas no extract antigo; ~606 k no DNE SE | **two-pass** (2× PBF); arquivos por UF |

Ver [extract-e-artefatos.md](./extract-e-artefatos.md) para resume/two-pass.

### Resume do extract neste cenário

| | |
|--|--|
| Soft-stop na pass 1 | Checkpoint de offset existe; pending de logradouro **não** serializado |
| Soft-stop antes/durante pass 2 | **Não** resume de forma confiável — recomeçar do zero na pasta de streets |
| Recomendação | Rodar logradouro **até o fim** numa sessão (ou implementar pending em disco depois) |

## Match (CLI a implementar no ddsoft)

Nome sugerido: **`osm:dne:enrich-geo`** (paralelo a `osm:locais:enrich-geo` e `dne:locais:enrich-cep`).

```bash
php bin/console osm:dne:enrich-geo --dir=G:\osm-geo-se-streets --dataset=bairro --dry-run
php bin/console osm:dne:enrich-geo --dir=G:\osm-geo-se-streets --dataset=logradouro --uf=SP
# … RJ, MG, ES
```

### Estratégia de casamento

| Nível | Ordem de match | Notas |
|-------|----------------|-------|
| **Bairro** | `ufe_sg` + `nome_norm` + `loc_nu` | `loc_nu` via município OSM (nome/IBGE → `dne_idx_localidade.mun_nu` / `locais.codigo_ibge` → `codigo_dne`) |
| **Logradouro** | `ufe_sg` + `nome_norm` + `loc_nu` (+ `bai_nu` se suburb bater) | Normalizar tipo: DNE tem `tlo_tx` (“Rua”) + `log_no`; OSM muitas vezes só `name` com ou sem tipo |

Política: só preencher `lat IS NULL`; `--overwrite` explícito; relatório `matched` / `ambiguous` / `unmatched` (TXT de auditoria).

### Dificuldade esperada

| Fácil | Difícil |
|-------|---------|
| Capitais com nomenclatura estável | Grafia DNE ≠ OSM |
| Bairro com nome único no município | Homônimos |
| Logradouro com nome longo + município | “Rua X” vs “X”; several LOG_NU same street name |
| | Ways sem `name`; UF residual `XX` se two-pass falhar |

Meta realista no 1º passe: dezenas de % de cobertura em logradouro; bairro um pouco melhor se nomes alinharem.

## Depois do match no índice

1. **Busca:** `BuscaEnderecoService` / DTO `LocalEnderecoCandidato` incluir `lat`/`lng` quando `origem=DNE` e índice tiver geo.  
2. **Accept / materialização:** ao criar linha em `locais` ou `locais_tenant`, copiar geo do `dne_idx_*` (ou re-match).  
3. **Não** preencher número de porta (tipo 12) em massa no global — tenant/GPS.

## O que **não** fazer nesta fase

- Tratar bairro/logradouro como `osm:locais:enrich-geo` sem materializar  
- Truncar IBGE de distrito para “ajudar” município (já corrigido no extract de mun)  
- Apagar `G:\osm-geo-se` dos municípios ao gerar streets  
- Confiar em resume mid two-pass sem pending em disco  

## Checklist de implementação (quando codar)

- [ ] Migration `lat`/`lng` (+ bbox opcional) em `dne_idx_bairro` e `dne_idx_logradouro`  
- [ ] Extract `bairro,logradouro` → `G:\osm-geo-se-streets`  
- [ ] `LocaisOsmGeoEnricher` ou `DneOsmGeoEnricher` + CLI `osm:dne:enrich-geo`  
- [ ] Testes unitários de normalização de nome de rua + match por UF  
- [ ] Dry-run SP → apply → métricas de cobertura  
- [ ] Docs ddsoft §11 + changelog  
- [ ] (Opcional) resume two-pass com pending em arquivo  
- [ ] (Opcional) wipe seletivo por dataset no extract  

## Ligações

- [estado-atual.md](./estado-atual.md) — o que já funciona em município  
- [match-estado-municipio.md](./match-estado-municipio.md) — padrão da CLI de enrich  
- ddsoft: `docs/locais-tenant-e-dne/estrutura-dados-endereco.md` (camadas DNE vs `locais`)  
- ddsoft: `docs/locais-tenant-e-dne/geo-osm-pipeline.md` (visão produto)
