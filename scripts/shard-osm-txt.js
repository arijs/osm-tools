#!/usr/bin/env node
'use strict';

/**
 * Parte arquivos flat OSM_*.TXT em shards estilo extract-geocode-pbf
 * (pasta BASE/{N}-linhas/000001.txt + MANIFEST.json).
 *
 * Uso típico — Sudeste flat → shards de 20k (igual às outras regiões):
 *
 *   node scripts/shard-osm-txt.js --dir=G:\osm-geo-br-sudeste --shard-lines=20000
 *   node scripts/shard-osm-txt.js --dir=G:\osm-geo-br-sudeste --shard-lines=20000 --remove-flat
 *   node scripts/shard-osm-txt.js --dir=G:\osm-geo-br-sudeste --uf=SP,RJ --datasets=logradouro
 *
 * Por padrão processa logradouro + bairro + addr. Não toca OSM_ESTADO/MUNICIPIO.
 * Se a pasta de shards já existir, o dataset é pulado (use --force para refazer).
 */

var fs = require('fs');
var path = require('path');
var readline = require('readline');
var txtAt = require('../txt-at-writer');

var DEFAULT_DATASETS = ['logradouro', 'bairro', 'addr'];

function parseCli(argv) {
	var o = {
		dir: null,
		out: null,
		shardLines: 20000,
		datasets: DEFAULT_DATASETS.slice(),
		uf: null,
		removeFlat: false,
		force: false,
		quiet: false
	};
	for (var i = 0; i < argv.length; i++) {
		var a = argv[i];
		if (a.indexOf('--dir=') === 0) o.dir = a.slice(6);
		else if (a.indexOf('--out=') === 0) o.out = a.slice(6);
		else if (a.indexOf('--shard-lines=') === 0) o.shardLines = Number(a.slice(14)) | 0;
		else if (a.indexOf('--datasets=') === 0) {
			o.datasets = a.slice(11).split(',').map(function (s) {
				return s.trim().toLowerCase();
			}).filter(Boolean);
		} else if (a.indexOf('--uf=') === 0) {
			o.uf = a.slice(5).split(/[,+\s]+/).map(function (s) {
				return s.trim().toUpperCase();
			}).filter(Boolean);
		} else if (a === '--remove-flat') o.removeFlat = true;
		else if (a === '--force') o.force = true;
		else if (a === '--quiet') o.quiet = true;
	}
	return o;
}

function listFlatFiles(dir) {
	var out = [];
	if (!fs.existsSync(dir)) return out;
	var names = fs.readdirSync(dir);
	for (var i = 0; i < names.length; i++) {
		var n = names[i];
		if (!/^OSM_.+\.TXT$/i.test(n)) continue;
		var full = path.join(dir, n);
		if (!fs.statSync(full).isFile()) continue;
		out.push({ name: n, full: full, base: n.replace(/\.TXT$/i, '') });
	}
	out.sort(function (a, b) {
		return a.base < b.base ? -1 : a.base > b.base ? 1 : 0;
	});
	return out;
}

function wantBase(base, datasets, ufFilter) {
	var upper = base.toUpperCase();
	if (upper === 'OSM_ESTADO' || upper === 'OSM_MUNICIPIO') return false;

	var kind = null;
	var uf = null;
	if (upper === 'OSM_BAIRRO') {
		kind = 'bairro';
	} else if (upper.indexOf('OSM_LOGRADOURO_') === 0) {
		kind = 'logradouro';
		uf = upper.slice('OSM_LOGRADOURO_'.length);
	} else if (upper.indexOf('OSM_ADDR_POINT_') === 0) {
		kind = 'addr';
		uf = upper.slice('OSM_ADDR_POINT_'.length);
	} else {
		return false;
	}

	if (datasets.indexOf(kind) < 0) return false;
	if (ufFilter && ufFilter.length && uf) {
		if (ufFilter.indexOf(uf) < 0) return false;
	}
	return true;
}

function readLines(file, onLine) {
	return new Promise(function (resolve, reject) {
		var rl = readline.createInterface({
			input: fs.createReadStream(file, { encoding: 'utf8' }),
			crlfDelay: Infinity
		});
		rl.on('line', function (l) {
			if (l) onLine(l);
		});
		rl.on('close', resolve);
		rl.on('error', reject);
	});
}

/**
 * @param {object} opts
 * @returns {Promise<{ processed: number, skipped: number, lines: number, files: string[] }>}
 */
async function run(opts) {
	var dir = opts.dir;
	var outDir = opts.out || dir;
	var shardLines = opts.shardLines > 0 ? opts.shardLines : 20000;
	var log = opts.quiet
		? function () {}
		: function (m) { process.stderr.write(m + '\n'); };

	if (!dir || !fs.existsSync(dir)) {
		throw new Error('diretório inválido: ' + dir);
	}
	if (outDir !== dir) fs.mkdirSync(outDir, { recursive: true });

	var flats = listFlatFiles(dir);
	var processed = 0;
	var skipped = 0;
	var totalLines = 0;
	var done = [];

	for (var i = 0; i < flats.length; i++) {
		var f = flats[i];
		if (!wantBase(f.base, opts.datasets, opts.uf)) continue;

		var shardRoot = path.join(outDir, f.base);
		if (fs.existsSync(shardRoot) && !opts.force) {
			log('skip (já existe pasta de shards): ' + f.base + '  (use --force)');
			skipped++;
			continue;
		}
		if (fs.existsSync(shardRoot) && opts.force) {
			fs.rmSync(shardRoot, { recursive: true, force: true });
		}

		log('shard ' + f.base + ' → ' + shardLines + '-linhas/ …');
		var writer = txtAt.createTxtAtWriter(outDir, {
			shardLines: shardLines,
			shardOnly: [f.base],
			sourcePbf: 'shard-osm-txt:' + f.full
		});
		var n = 0;
		await readLines(f.full, function (line) {
			// write espera fields; grava a linha já formatada re-splitando em @
			// para reutilizar formatRow/sanitize — mas a linha já é válida.
			// Escreve via stream interno: fields = split (pode ter campos vazios).
			writer.write(f.base, line.split('@'));
			n++;
			if (!opts.quiet && n % 100000 === 0) {
				process.stderr.write('\r  ' + f.base + ' ' + n + ' linhas');
			}
		});
		await writer.flush();
		if (!opts.quiet && n >= 100000) process.stderr.write('\n');
		log('  OK ' + n + ' linhas → ' + path.join(f.base, shardLines + '-linhas'));

		if (opts.removeFlat && fs.existsSync(f.full)) {
			fs.unlinkSync(f.full);
			log('  removido flat ' + f.name);
		}

		processed++;
		totalLines += n;
		done.push(f.base);
	}

	log('feito: processed=' + processed + ' skipped=' + skipped +
		' lines=' + totalLines);
	return { processed: processed, skipped: skipped, lines: totalLines, files: done };
}

module.exports = {
	run: run,
	parseCli: parseCli,
	listFlatFiles: listFlatFiles,
	wantBase: wantBase
};

if (require.main === module) {
	var opts = parseCli(process.argv.slice(2));
	if (!opts.dir || !opts.shardLines) {
		process.stderr.write(
			'uso: node scripts/shard-osm-txt.js --dir=DIR [--shard-lines=20000]\n' +
			'     [--out=DIR] [--datasets=logradouro,bairro,addr] [--uf=SP,RJ]\n' +
			'     [--remove-flat] [--force] [--quiet]\n'
		);
		process.exit(1);
	}
	run(opts).catch(function (e) {
		process.stderr.write(String(e && e.stack || e) + '\n');
		process.exit(2);
	});
}
