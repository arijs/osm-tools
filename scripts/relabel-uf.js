#!/usr/bin/env node
'use strict';

/**
 * Re-rotula por POLÍGONO os artefatos já extraídos, sem re-extrair o PBF.
 *
 * ─── por que ──────────────────────────────────────────────────────────────
 * A UF é campo DERIVADO da coordenada, e a coordenada está gravada em cada
 * linha. Os extracts de 12–18/08/2026 foram rotulados por retângulo (ou pela
 * caixa permitida do run, no paliativo de 18/08) e ficaram errados: no
 * `OSM_LOGRADOURO_MG` da fatia `mg`, só 339.065 das 1.007.314 linhas estavam
 * mesmo em MG. Reclassificar o que está no disco custa minutos; re-extrair
 * custa ~1h35 por fatia.
 *
 * ─── o que ele NÃO recupera ──────────────────────────────────────────────
 * O que o filtro antigo nunca deixou entrar. `passesUfFilter` era retângulo, e
 * quatro UFs têm área fora do próprio retângulo: Fernando de Noronha (PE),
 * Trindade e Martim Vaz (ES) e faixas de ~8 km (AP) e ~4 km (PI). Isso só volta
 * num re-extract com o código novo, onde quem filtra é o polígono.
 *
 * ─── uma divergência de propósito ────────────────────────────────────────
 * O extract respeita tag/IBGE antes da geometria; aqui não dá — a linha gravada
 * não guarda as tags. Então **o polígono decide**, e o rótulo antigo só
 * sobrevive quando o ponto cai fora de todos os polígonos (mar, plataforma). Na
 * prática isso deixa a saída mais limpa que um re-extract: a feature com
 * `addr:state` de UF vizinha vai para a UF onde ela geograficamente está.
 *
 * ─── uso ─────────────────────────────────────────────────────────────────
 *   node scripts/relabel-uf.js --base=G:\osm-geo-br-geom --out=G:\osm-geo-br-uf
 *   node scripts/relabel-uf.js --base=... --out=... --only=mg,sp
 *   node scripts/relabel-uf.js --base=... --dry-run
 *
 * Só toca `OSM_LOGRADOURO_{UF}` e `OSM_LOGRADOURO_GEOM_{UF}` (os datasets que
 * têm sufixo de UF e coordenada por linha). Nada é apagado: a saída é uma pasta
 * nova, e a mesma feature vista por duas fatias entra uma vez só.
 */

var fs = require('fs');
var path = require('path');
var txtAt = require('../txt-at-writer');
var ufBr = require('../uf-br');
var ufPoly = require('../uf-poly');

var COL_ID = 0;
var COL_UF = 4;
var COL_LAT = 10;
var COL_LNG = 11;
var COL_TYPE = 19;

/** node/way/relation em 2 bits, para a chave de deduplicação caber num número */
var TIPO = { node: 0, way: 1, relation: 2 };

function parseCli(argv) {
	var o = { base: null, out: null, only: null, shardLines: 20000, dryRun: false, quiet: false };
	for (var i = 0; i < argv.length; i++) {
		var a = argv[i];
		if (a.indexOf('--base=') === 0) o.base = a.slice(7);
		else if (a.indexOf('--out=') === 0) o.out = a.slice(6);
		else if (a.indexOf('--only=') === 0) {
			o.only = a.slice(7).split(/[,+\s]+/).map(function (s) {
				return s.trim().toLowerCase();
			}).filter(Boolean);
		} else if (a.indexOf('--shard-lines=') === 0) o.shardLines = Number(a.slice(14)) | 0;
		else if (a === '--dry-run') o.dryRun = true;
		else if (a === '--quiet') o.quiet = true;
	}
	return o;
}

/** Fatias: subpastas de --base que tenham algum dataset OSM_LOGRADOURO_*. */
function acharFatias(base, only) {
	var out = [];
	fs.readdirSync(base).sort().forEach(function (nome) {
		var dir = path.join(base, nome);
		if (!fs.statSync(dir).isDirectory()) return;
		if (only && only.indexOf(nome.toLowerCase()) < 0) return;
		var temDataset = fs.readdirSync(dir).some(function (d) {
			return d.indexOf('OSM_LOGRADOURO_') === 0;
		});
		if (temDataset) out.push({ nome: nome, dir: dir });
	});
	return out;
}

/** Datasets de uma fatia, em ordem fixa — a saída precisa ser determinística. */
function acharDatasets(dir, geom) {
	return fs.readdirSync(dir).sort().filter(function (d) {
		var ehGeom = d.indexOf('OSM_LOGRADOURO_GEOM_') === 0;
		if (d.indexOf('OSM_LOGRADOURO_') !== 0) return false;
		if (geom !== ehGeom) return false;
		return fs.existsSync(path.join(dir, d, 'MANIFEST.json'));
	}).map(function (d) {
		return path.join(dir, d);
	});
}

/** Percorre as linhas de um dataset fatiado, shard a shard (memória limitada). */
function lerDataset(dsDir, onRow) {
	var man = JSON.parse(fs.readFileSync(path.join(dsDir, 'MANIFEST.json'), 'utf8'));
	var shardDir = path.join(dsDir, man.shard_dir);
	for (var i = 0; i < man.shards.length; i++) {
		var linhas = fs.readFileSync(path.join(shardDir, man.shards[i].file), 'utf8').split('\n');
		for (var j = 0; j < linhas.length; j++) {
			var linha = linhas[j];
			if (!linha || linha === '\r') continue;
			if (linha.charCodeAt(linha.length - 1) === 13) linha = linha.slice(0, -1);
			onRow(linha.split('@'));
		}
	}
}

/** Primeiro ponto da polyline do GEOM: absoluto, em 1e-6 de grau. */
function primeiroPonto(polyline) {
	if (!polyline) return null;
	var fim = polyline.indexOf(';');
	var par = (fim < 0 ? polyline : polyline.slice(0, fim)).split(',');
	if (par.length < 2) return null;
	var lat = Number(par[0]) / 1e6;
	var lng = Number(par[1]) / 1e6;
	if (!isFinite(lat) || !isFinite(lng)) return null;
	return { lat: lat, lng: lng };
}

/**
 * UF de uma linha de logradouro: polígono, e o rótulo gravado só quando o ponto
 * cai fora de todos (mar, plataforma) — ali o rótulo antigo é o que existe.
 */
function ufDaLinha(campos) {
	var lat = Number(campos[COL_LAT]);
	var lng = Number(campos[COL_LNG]);
	if (campos[COL_LAT] !== '' && isFinite(lat) && isFinite(lng)) {
		var u = ufPoly.ufFromPointPoly(lat, lng);
		if (u) return u;
	}
	var antigo = campos[COL_UF];
	return antigo && ufBr.isKnownUf(antigo) ? antigo : 'XX';
}

function relabel(options) {
	var fatias = acharFatias(options.base, options.only);
	if (!fatias.length) throw new Error('nenhuma fatia com OSM_LOGRADOURO_* em ' + options.base);

	var writer = options.dryRun
		? null
		: txtAt.createTxtAtWriter(options.out, {
			shardLines: options.shardLines,
			sourcePbf: 'relabel-uf de ' + options.base
		});

	var resumo = {
		base: options.base,
		out: options.dryRun ? null : options.out,
		fatias: fatias.map(function (f) { return f.nome; }),
		malha: { fonte: ufPoly.gridStats().fonte, baixado_em: ufPoly.gridStats().baixado_em },
		logradouro: { lidas: 0, duplicadas: 0, mudaram: 0, gravadas: 0, por_uf: {}, de_para: {} },
		geom: { lidas: 0, duplicadas: 0, sem_irmao: 0, gravadas: 0, por_uf: {} }
	};

	function conta(mapa, chave) {
		mapa[chave] = (mapa[chave] || 0) + 1;
	}

	// Passo 1: logradouro. Guarda a UF de cada way para o GEOM usar a MESMA
	// resposta — o primeiro nó do traçado e o centróide da via caem em UFs
	// diferentes numa via de divisa, e o GEOM precisa ficar irmão do logradouro.
	var vistos = new Set();
	var ufDoWay = new Map();
	var ufIndex = [];
	var ufCodigo = Object.create(null);

	function indiceDe(uf) {
		if (ufCodigo[uf] === undefined) {
			ufCodigo[uf] = ufIndex.length;
			ufIndex.push(uf);
		}
		return ufCodigo[uf];
	}

	fatias.forEach(function (fatia) {
		acharDatasets(fatia.dir, false).forEach(function (ds) {
			lerDataset(ds, function (campos) {
				resumo.logradouro.lidas++;
				var id = Number(campos[COL_ID]);
				var tipo = TIPO[campos[COL_TYPE]] === undefined ? 3 : TIPO[campos[COL_TYPE]];
				var chave = id * 4 + tipo;
				if (vistos.has(chave)) {
					resumo.logradouro.duplicadas++;
					return;
				}
				vistos.add(chave);
				var antigo = campos[COL_UF];
				var uf = ufDaLinha(campos);
				if (uf !== antigo) {
					resumo.logradouro.mudaram++;
					conta(resumo.logradouro.de_para, antigo + '→' + uf);
				}
				if (tipo === TIPO.way) ufDoWay.set(id, indiceDe(uf));
				campos[COL_UF] = uf;
				conta(resumo.logradouro.por_uf, uf);
				resumo.logradouro.gravadas++;
				if (writer) writer.write('OSM_LOGRADOURO_' + uf, campos);
			});
		});
	});
	vistos = null;

	// Passo 2: geom. Segue o irmão; se o osm_id não apareceu no logradouro (não
	// deveria acontecer), decide pelo primeiro ponto do traçado.
	var vistosGeom = new Set();
	fatias.forEach(function (fatia) {
		acharDatasets(fatia.dir, true).forEach(function (ds) {
			lerDataset(ds, function (campos) {
				resumo.geom.lidas++;
				var id = Number(campos[COL_ID]);
				if (vistosGeom.has(id)) {
					resumo.geom.duplicadas++;
					return;
				}
				vistosGeom.add(id);
				var uf;
				var idx = ufDoWay.get(id);
				if (idx !== undefined) {
					uf = ufIndex[idx];
				} else {
					resumo.geom.sem_irmao++;
					var p = primeiroPonto(campos[1]);
					uf = (p && ufPoly.ufFromPointPoly(p.lat, p.lng)) || 'XX';
				}
				conta(resumo.geom.por_uf, uf);
				resumo.geom.gravadas++;
				if (writer) writer.write('OSM_LOGRADOURO_GEOM_' + uf, campos);
			});
		});
	});

	if (!writer) return Promise.resolve(resumo);
	return writer.flush().then(function () {
		// contrato de colunas: o consumidor espera achar isso ao lado dos dados
		for (var i = 0; i < fatias.length; i++) {
			var readme = path.join(fatias[i].dir, 'README-colunas.md');
			if (fs.existsSync(readme)) {
				fs.copyFileSync(readme, path.join(options.out, 'README-colunas.md'));
				break;
			}
		}
		fs.writeFileSync(
			path.join(options.out, 'RELABEL-SUMMARY.json'),
			JSON.stringify(resumo, null, 2),
			'utf8'
		);
		return resumo;
	});
}

function tabela(mapa, limite) {
	var chaves = Object.keys(mapa).sort(function (a, b) { return mapa[b] - mapa[a]; });
	if (limite) chaves = chaves.slice(0, limite);
	return chaves.map(function (k) { return k + '=' + mapa[k]; }).join(' ');
}

function main() {
	var o = parseCli(process.argv.slice(2));
	if (!o.base || (!o.out && !o.dryRun)) {
		console.error('uso: node scripts/relabel-uf.js --base=<pasta dos extracts> --out=<pasta nova> [--only=mg,sp] [--dry-run]');
		process.exit(2);
	}
	var t0 = Date.now();
	relabel(o).then(function (r) {
		if (o.quiet) return;
		console.log('fatias: ' + r.fatias.join(', '));
		console.log(
			'logradouro: ' + r.logradouro.lidas + ' lidas, ' + r.logradouro.duplicadas +
			' duplicadas, ' + r.logradouro.mudaram + ' mudaram de UF, ' + r.logradouro.gravadas + ' gravadas'
		);
		console.log('  por UF : ' + tabela(r.logradouro.por_uf));
		console.log('  de→para: ' + tabela(r.logradouro.de_para, 15));
		console.log(
			'geom: ' + r.geom.lidas + ' lidas, ' + r.geom.duplicadas + ' duplicadas, ' +
			r.geom.sem_irmao + ' sem irmão, ' + r.geom.gravadas + ' gravadas'
		);
		console.log('  por UF : ' + tabela(r.geom.por_uf));
		console.log('em ' + ((Date.now() - t0) / 1000).toFixed(1) + ' s' + (o.dryRun ? ' (dry-run, nada gravado)' : ''));
	}, function (err) {
		console.error(err.stack || String(err));
		process.exit(1);
	});
}

if (require.main === module) main();

module.exports = {
	relabel: relabel,
	ufDaLinha: ufDaLinha,
	primeiroPonto: primeiroPonto,
	acharFatias: acharFatias,
	acharDatasets: acharDatasets
};
