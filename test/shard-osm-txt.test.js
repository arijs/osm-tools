'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var fs = require('fs');
var os = require('os');
var path = require('path');
var shard = require('../scripts/shard-osm-txt');
var txtAt = require('../txt-at-writer');

test('shard-osm-txt: flat → shards 3 linhas + MANIFEST', async function () {
	var dir = fs.mkdtempSync(path.join(os.tmpdir(), 'shard-osm-'));
	try {
		var lines = [];
		for (var i = 0; i < 7; i++) {
			lines.push(['id' + i, 'Rua', 'rua', 'residential', 'SP', '', '', '', '', '',
				'-23.5', '-46.6', '-23.5', '-23.5', '-46.6', '-46.6', '4', '', '', 'way'].join('@'));
		}
		fs.writeFileSync(path.join(dir, 'OSM_LOGRADOURO_SP.TXT'), lines.join('\n') + '\n');
		fs.writeFileSync(path.join(dir, 'OSM_ESTADO.TXT'), 'node@1@SP@Sao Paulo\n');

		var r = await shard.run({
			dir: dir,
			shardLines: 3,
			datasets: ['logradouro'],
			quiet: true
		});
		assert.equal(r.processed, 1);
		assert.equal(r.lines, 7);

		var man = JSON.parse(
			fs.readFileSync(path.join(dir, 'OSM_LOGRADOURO_SP', 'MANIFEST.json'), 'utf8')
		);
		assert.equal(man.complete, true);
		assert.equal(man.total_lines, 7);
		assert.equal(man.shard_count, 3);
		assert.ok(fs.existsSync(path.join(dir, 'OSM_LOGRADOURO_SP', '3-linhas', '000001.txt')));
		assert.ok(fs.existsSync(path.join(dir, 'OSM_LOGRADOURO_SP', '3-linhas', '000003.txt')));
		// estado não fatiado
		assert.ok(fs.existsSync(path.join(dir, 'OSM_ESTADO.TXT')));

		var resolved = txtAt.resolveDatasetPaths(dir, 'OSM_LOGRADOURO_SP');
		assert.equal(resolved.mode, 'shard');
		assert.equal(resolved.paths.length, 3);

		// skip se já existe
		var r2 = await shard.run({
			dir: dir, shardLines: 3, datasets: ['logradouro'], quiet: true
		});
		assert.equal(r2.skipped, 1);
		assert.equal(r2.processed, 0);
	} finally {
		fs.rmSync(dir, { recursive: true, force: true });
	}
});

test('shard-osm-txt: --remove-flat e --uf', async function () {
	var dir = fs.mkdtempSync(path.join(os.tmpdir(), 'shard-osm-rm-'));
	try {
		fs.writeFileSync(path.join(dir, 'OSM_LOGRADOURO_SP.TXT'), 'a@b\nc@d\n');
		fs.writeFileSync(path.join(dir, 'OSM_LOGRADOURO_RJ.TXT'), 'e@f\n');
		await shard.run({
			dir: dir,
			shardLines: 10,
			datasets: ['logradouro'],
			uf: ['SP'],
			removeFlat: true,
			quiet: true
		});
		assert.equal(fs.existsSync(path.join(dir, 'OSM_LOGRADOURO_SP.TXT')), false);
		assert.ok(fs.existsSync(path.join(dir, 'OSM_LOGRADOURO_SP', 'MANIFEST.json')));
		assert.ok(fs.existsSync(path.join(dir, 'OSM_LOGRADOURO_RJ.TXT')), 'RJ não tocado');
	} finally {
		fs.rmSync(dir, { recursive: true, force: true });
	}
});
