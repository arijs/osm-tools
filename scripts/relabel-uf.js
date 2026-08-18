#!/usr/bin/env node
'use strict';

/**
 * Re-rotula por POLÍGONO os artefatos já extraídos, sem re-extrair o PBF.
 *
 * ─── por que ──────────────────────────────────────────────────────────────
 * A UF é campo DERIVADO da coordenada, e a coordenada está gravada em cada
 * linha. Os extracts de 30/07 a 18/08/2026 foram rotulados por retângulo (ou
 * pela caixa permitida do run, no paliativo de 18/08) e ficaram errados: no
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
 *   node scripts/relabel-uf.js --dirs=G:\osm-geo-br-norte,G:\osm-geo-br-sul --out=...
 *   node scripts/relabel-uf.js --base=... --datasets=bairro,addr --out=...
 *   node scripts/relabel-uf.js --base=... --dry-run
 *
 * Datasets com coordenada por linha: `logradouro`, `geom`, `addr` e `bairro`.
 * Os três primeiros são reparticionados por UF (o nome do arquivo muda);
 * `OSM_BAIRRO` é um dataset só, então o que muda é a coluna `uf`.
 *
 * Nada é apagado: a saída é uma pasta nova, e a mesma feature vista por duas
 * fatias entra uma vez só. Rodar de novo com outros `--datasets` acrescenta à
 * mesma pasta (datasets diferentes, arquivos diferentes).
 */

var fs = require('fs');
var path = require('path');
var txtAt = require('../txt-at-writer');
var ufBr = require('../uf-br');
var ufPoly = require('../uf-poly');

/**
 * Onde ficam id/tipo/uf/coordenada em cada dataset (README-colunas.md).
 * -1 = a coluna não existe nesse dataset.
 */
var SPEC = {
	logradouro: { prefixo: 'OSM_LOGRADOURO_', particiona: true, id: 0, tipo: 19, uf: 4, lat: 10, lng: 11 },
	addr: { prefixo: 'OSM_ADDR_POINT_', particiona: true, id: 0, tipo: -1, uf: -1, lat: 1, lng: 2 },
	bairro: { prefixo: 'OSM_BAIRRO', particiona: false, id: 1, tipo: 0, uf: 4, lat: 8, lng: 9 },
	// geom não tem coordenada útil por si: segue a UF do irmão em logradouro
	geom: { prefixo: 'OSM_LOGRADOURO_GEOM_', particiona: true, id: 0, tipo: -1, uf: -1, lat: -1, lng: -1 }
};
var ORDEM = ['logradouro', 'geom', 'addr', 'bairro'];

/** node/way/relation em 2 bits, para a chave de deduplicação caber num número */
var TIPO = { node: 0, way: 1, relation: 2 };

function parseCli(argv) {
	var o = {
		base: null, dirs: null, out: null, only: null,
		datasets: null, shardLines: 20000, dryRun: false, quiet: false, force: false
	};
	for (var i = 0; i < argv.length; i++) {
		var a = argv[i];
		if (a.indexOf('--base=') === 0) o.base = a.slice(7);
		else if (a.indexOf('--out=') === 0) o.out = a.slice(6);
		else if (a.indexOf('--dirs=') === 0) o.dirs = a.slice(7).split(',').map(trim).filter(Boolean);
		else if (a.indexOf('--only=') === 0) o.only = a.slice(7).split(/[,+\s]+/).map(minusc).filter(Boolean);
		else if (a.indexOf('--datasets=') === 0) o.datasets = a.slice(11).split(/[,+\s]+/).map(minusc).filter(Boolean);
		else if (a.indexOf('--shard-lines=') === 0) o.shardLines = Number(a.slice(14)) | 0;
		else if (a === '--dry-run') o.dryRun = true;
		else if (a === '--force') o.force = true;
		else if (a === '--quiet') o.quiet = true;
	}
	return o;
}

function trim(s) { return String(s).trim(); }
function minusc(s) { return String(s).trim().toLowerCase(); }

function temDataset(dir) {
	return fs.readdirSync(dir).some(function (d) {
		return d.indexOf('OSM_LOGRADOURO_') === 0 || d.indexOf('OSM_BAIRRO') === 0 ||
			d.indexOf('OSM_ADDR_POINT_') === 0;
	});
}

/**
 * Fatias a percorrer: `--dirs` explícito, ou as subpastas de `--base` que
 * tenham datasets — e `--base` sozinho, quando ele mesmo é uma fatia.
 */
function acharFatias(options) {
	if (options.dirs && options.dirs.length) {
		return options.dirs.map(function (d) {
			return { nome: path.basename(d), dir: d };
		});
	}
	var base = options.base;
	if (temDataset(base)) return [{ nome: path.basename(base), dir: base }];
	var out = [];
	fs.readdirSync(base).sort().forEach(function (nome) {
		var dir = path.join(base, nome);
		if (!fs.statSync(dir).isDirectory()) return;
		if (options.only && options.only.indexOf(nome.toLowerCase()) < 0) return;
		if (temDataset(dir)) out.push({ nome: nome, dir: dir });
	});
	return out;
}

/** Datasets de uma família numa fatia, em ordem fixa (saída determinística). */
function acharDatasets(dir, familia) {
	var spec = SPEC[familia];
	return fs.readdirSync(dir).sort().filter(function (d) {
		if (familia === 'logradouro' && d.indexOf(SPEC.geom.prefixo) === 0) return false;
		if (spec.particiona) {
			if (d.indexOf(spec.prefixo) !== 0) return false;
		} else if (d !== spec.prefixo) return false;
		var alvo = path.join(dir, d);
		return fs.existsSync(alvo);
	}).map(function (d) {
		return { chave: d, dir: dir };
	});
}

/** Percorre as linhas de um dataset (fatiado ou flat), arquivo a arquivo. */
function lerDataset(dir, chave, onRow) {
	var res = txtAt.resolveDatasetPaths(dir, chave);
	if (!res || !res.paths || !res.paths.length) return;
	for (var i = 0; i < res.paths.length; i++) {
		var linhas = fs.readFileSync(res.paths[i], 'utf8').split('\n');
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
 * UF de uma linha: polígono, e o rótulo antigo só quando o ponto cai fora de
 * todos (mar, plataforma) — ali o rótulo antigo é o que existe.
 */
function ufDaLinha(campos, spec, ufDoNome) {
	if (spec.lat >= 0 && campos[spec.lat] !== '' && campos[spec.lat] !== undefined) {
		var lat = Number(campos[spec.lat]);
		var lng = Number(campos[spec.lng]);
		if (isFinite(lat) && isFinite(lng)) {
			var u = ufPoly.ufFromPointPoly(lat, lng);
			if (u) return u;
		}
	}
	var antigo = spec.uf >= 0 ? campos[spec.uf] : ufDoNome;
	return antigo && ufBr.isKnownUf(antigo) ? antigo : 'XX';
}

function chaveDedup(campos, spec) {
	var id = Number(campos[spec.id]);
	var tipo = spec.tipo >= 0 ? TIPO[campos[spec.tipo]] : 0;
	return id * 4 + (tipo === undefined ? 3 : tipo);
}

function relabel(options) {
	var fatias = acharFatias(options);
	if (!fatias.length) throw new Error('nenhuma fatia com dataset OSM_* em ' + (options.base || options.dirs));
	var familias = options.datasets && options.datasets.length ? options.datasets : ORDEM;
	familias = ORDEM.filter(function (f) { return familias.indexOf(f) >= 0; });
	if (!familias.length) throw new Error('--datasets sem nada conhecido (use logradouro,geom,addr,bairro)');

	// regravar uma família que já está na pasta reabriria o shard 000001 do zero
	// e comeria o que estava lá; o modo de acrescentar é rodar com --datasets
	// de famílias que ainda não foram gravadas
	if (!options.dryRun && !options.force && fs.existsSync(options.out)) {
		var jaTem = fs.readdirSync(options.out).filter(function (nome) {
			return familias.some(function (f) {
				var p = SPEC[f].prefixo;
				if (f === 'logradouro' && nome.indexOf(SPEC.geom.prefixo) === 0) return false;
				return SPEC[f].particiona ? nome.indexOf(p) === 0 : nome === p;
			});
		});
		if (jaTem.length) {
			throw new Error(
				'a pasta de saída já tem ' + jaTem.slice(0, 5).join(', ') +
				(jaTem.length > 5 ? ' (+' + (jaTem.length - 5) + ')' : '') +
				' — use --datasets com as famílias que faltam, outra --out, ou --force para regravar'
			);
		}
	}

	var writer = options.dryRun
		? null
		: txtAt.createTxtAtWriter(options.out, {
			shardLines: options.shardLines,
			sourcePbf: 'relabel-uf de ' + (options.base || options.dirs.join(','))
		});

	var stats = ufPoly.gridStats();
	var resumo = {
		base: options.base || null,
		dirs: options.dirs || null,
		out: options.dryRun ? null : options.out,
		fatias: fatias.map(function (f) { return f.nome; }),
		datasets: familias,
		malha: { fonte: stats.fonte, baixado_em: stats.baixado_em },
		por_familia: {}
	};
	familias.forEach(function (f) {
		resumo.por_familia[f] = { lidas: 0, duplicadas: 0, mudaram: 0, gravadas: 0, por_uf: {}, de_para: {} };
		if (f === 'geom') resumo.por_familia[f].sem_irmao = 0;
	});

	function conta(mapa, chave) {
		mapa[chave] = (mapa[chave] || 0) + 1;
	}

	/** UF de cada way vista em logradouro — o GEOM tem de ficar irmão dela. */
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

	familias.forEach(function (familia) {
		var spec = SPEC[familia];
		var r = resumo.por_familia[familia];
		var vistos = new Set();
		fatias.forEach(function (fatia) {
			acharDatasets(fatia.dir, familia).forEach(function (ds) {
				// sufixo do nome do dataset: rótulo antigo de quem não tem coluna uf
				var ufDoNome = spec.particiona ? ds.chave.slice(spec.prefixo.length) : '';
				lerDataset(ds.dir, ds.chave, function (campos) {
					r.lidas++;
					var chave = chaveDedup(campos, spec);
					if (vistos.has(chave)) {
						r.duplicadas++;
						return;
					}
					vistos.add(chave);

					var antigo = spec.uf >= 0 ? campos[spec.uf] : ufDoNome;
					var uf;
					if (familia === 'geom') {
						var idx = ufDoWay.get(Number(campos[spec.id]));
						if (idx !== undefined) {
							uf = ufIndex[idx];
						} else {
							r.sem_irmao++;
							var p = primeiroPonto(campos[1]);
							uf = (p && ufPoly.ufFromPointPoly(p.lat, p.lng)) || antigo || 'XX';
						}
					} else {
						uf = ufDaLinha(campos, spec, ufDoNome);
					}

					if (uf !== antigo) {
						r.mudaram++;
						conta(r.de_para, (antigo || '?') + '→' + uf);
					}
					if (familia === 'logradouro' && campos[spec.tipo] === 'way') {
						ufDoWay.set(Number(campos[spec.id]), indiceDe(uf));
					}
					if (spec.uf >= 0) campos[spec.uf] = uf;
					conta(r.por_uf, uf);
					r.gravadas++;
					if (writer) {
						writer.write(spec.particiona ? spec.prefixo + uf : spec.prefixo, campos);
					}
				});
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
		var sumPath = path.join(options.out, 'RELABEL-SUMMARY.json');
		var doc = { runs: [] };
		if (fs.existsSync(sumPath)) {
			try {
				doc = JSON.parse(fs.readFileSync(sumPath, 'utf8'));
				if (!Array.isArray(doc.runs)) doc = { runs: [] };
			} catch (_) {
				doc = { runs: [] };
			}
		}
		doc.runs.push(resumo);
		fs.writeFileSync(sumPath, JSON.stringify(doc, null, 2), 'utf8');
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
	if ((!o.base && !o.dirs) || (!o.out && !o.dryRun)) {
		console.error(
			'uso: node scripts/relabel-uf.js --base=<pasta> --out=<pasta nova> [--only=mg,sp]\n' +
			'     node scripts/relabel-uf.js --dirs=<pasta,pasta> --out=<pasta nova>\n' +
			'     [--datasets=logradouro,geom,addr,bairro] [--shard-lines=20000] [--dry-run] [--force]'
		);
		process.exit(2);
	}
	var t0 = Date.now();
	var rodando;
	try {
		rodando = relabel(o);
	} catch (e) {
		// erro de pré-condição (pasta sem dataset, saída já gravada): mensagem seca
		console.error(e.message);
		process.exit(2);
		return;
	}
	rodando.then(function (r) {
		if (o.quiet) return;
		console.log('fatias: ' + r.fatias.join(', '));
		r.datasets.forEach(function (f) {
			var d = r.por_familia[f];
			if (!d.lidas) return;
			console.log(
				f + ': ' + d.lidas + ' lidas, ' + d.duplicadas + ' duplicadas, ' +
				d.mudaram + ' mudaram de UF, ' + d.gravadas + ' gravadas' +
				(d.sem_irmao !== undefined ? ', ' + d.sem_irmao + ' sem irmão' : '')
			);
			console.log('  por UF : ' + tabela(d.por_uf));
			if (d.mudaram) console.log('  de→para: ' + tabela(d.de_para, 15));
		});
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
	acharDatasets: acharDatasets,
	SPEC: SPEC
};
