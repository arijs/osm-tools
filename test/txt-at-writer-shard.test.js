'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var fs = require('fs');
var path = require('path');
var os = require('os');
var txtAt = require('../txt-at-writer');

test('shard-lines=3 gera 3 arquivos e MANIFEST', async function () {
	var dir = fs.mkdtempSync(path.join(os.tmpdir(), 'osm-shard-'));
	try {
		var w = txtAt.createTxtAtWriter(dir, {
			shardLines: 3,
			shardOnly: ['OSM_LOGRADOURO'],
			sourcePbf: 'test.pbf'
		});
		for (var i = 0; i < 7; i++) {
			w.write('OSM_LOGRADOURO_SP', ['id' + i, 'Rua', 'rua', 'residential', 'SP']);
		}
		// flat dataset não fatiado
		w.write('OSM_ESTADO', ['node', '1', 'SP', 'Sao Paulo']);
		await w.flush();

		var shardDir = path.join(dir, 'OSM_LOGRADOURO_SP', '3-linhas');
		assert.ok(fs.existsSync(shardDir));
		assert.ok(fs.existsSync(path.join(shardDir, '000001.txt')));
		assert.ok(fs.existsSync(path.join(shardDir, '000002.txt')));
		assert.ok(fs.existsSync(path.join(shardDir, '000003.txt')));
		assert.equal(fs.existsSync(path.join(shardDir, '000004.txt')), false);

		var lines1 = fs.readFileSync(path.join(shardDir, '000001.txt'), 'utf8').trim().split('\n');
		var lines3 = fs.readFileSync(path.join(shardDir, '000003.txt'), 'utf8').trim().split('\n');
		assert.equal(lines1.length, 3);
		assert.equal(lines3.length, 1);

		var man = JSON.parse(
			fs.readFileSync(path.join(dir, 'OSM_LOGRADOURO_SP', 'MANIFEST.json'), 'utf8')
		);
		assert.equal(man.complete, true);
		assert.equal(man.total_lines, 7);
		assert.equal(man.shard_count, 3);
		assert.equal(man.shard_lines, 3);
		assert.equal(man.shards[0].lines, 3);
		assert.equal(man.shards[2].lines, 1);

		// flat
		assert.ok(fs.existsSync(path.join(dir, 'OSM_ESTADO.TXT')));
		assert.equal(w.counts.OSM_LOGRADOURO_SP, 7);
		assert.equal(w.counts.OSM_ESTADO, 1);
	} finally {
		fs.rmSync(dir, { recursive: true, force: true });
	}
});

test('wipeOsmOutputs remove TXT e pastas OSM_', function () {
	var dir = fs.mkdtempSync(path.join(os.tmpdir(), 'osm-wipe-'));
	try {
		fs.writeFileSync(path.join(dir, 'OSM_BAIRRO.TXT'), 'x\n');
		fs.mkdirSync(path.join(dir, 'OSM_LOGRADOURO_SP', '10-linhas'), { recursive: true });
		fs.writeFileSync(path.join(dir, 'OSM_LOGRADOURO_SP', '10-linhas', '000001.txt'), 'y\n');
		fs.writeFileSync(path.join(dir, 'README-colunas.md'), 'keep\n');
		txtAt.wipeOsmOutputs(dir);
		assert.equal(fs.existsSync(path.join(dir, 'OSM_BAIRRO.TXT')), false);
		assert.equal(fs.existsSync(path.join(dir, 'OSM_LOGRADOURO_SP')), false);
		assert.ok(fs.existsSync(path.join(dir, 'README-colunas.md')));
	} finally {
		fs.rmSync(dir, { recursive: true, force: true });
	}
});

test('resolveDatasetPaths: flat e shards (prefere pasta)', function () {
	var dir = fs.mkdtempSync(path.join(os.tmpdir(), 'osm-resolve-'));
	try {
		// flat only
		fs.writeFileSync(path.join(dir, 'OSM_LOGRADOURO_ES.TXT'), 'a\nb\n');
		var flat = txtAt.resolveDatasetPaths(dir, 'OSM_LOGRADOURO_ES');
		assert.equal(flat.mode, 'flat');
		assert.equal(flat.paths.length, 1);

		// shards with MANIFEST
		var root = path.join(dir, 'OSM_LOGRADOURO_SP');
		var shardDir = path.join(root, '3-linhas');
		fs.mkdirSync(shardDir, { recursive: true });
		fs.writeFileSync(path.join(shardDir, '000001.txt'), '1\n2\n3\n');
		fs.writeFileSync(path.join(shardDir, '000002.txt'), '4\n');
		fs.writeFileSync(path.join(root, 'MANIFEST.json'), JSON.stringify({
			dataset_key: 'OSM_LOGRADOURO_SP',
			shard_lines: 3,
			shard_dir: '3-linhas',
			total_lines: 4,
			shard_count: 2,
			shards: [
				{ file: '000001.txt', lines: 3 },
				{ file: '000002.txt', lines: 1 }
			]
		}));
		var sh = txtAt.resolveDatasetPaths(dir, 'OSM_LOGRADOURO_SP');
		assert.equal(sh.mode, 'shard');
		assert.equal(sh.paths.length, 2);
		assert.ok(sh.paths[0].endsWith('000001.txt'));
		assert.ok(sh.paths[1].endsWith('000002.txt'));
		assert.equal(sh.totalLines, 4);

		// se flat e shard existem, prefere shard
		fs.writeFileSync(path.join(dir, 'OSM_LOGRADOURO_SP.TXT'), 'old\n');
		var prefer = txtAt.resolveDatasetPaths(dir, 'OSM_LOGRADOURO_SP');
		assert.equal(prefer.mode, 'shard');

		assert.equal(txtAt.resolveDatasetPaths(dir, 'OSM_LOGRADOURO_XX').mode, 'missing');
		assert.equal(txtAt.datasetExists(dir, 'OSM_LOGRADOURO_ES'), true);
		assert.equal(txtAt.datasetExists(dir, 'OSM_LOGRADOURO_XX'), false);
	} finally {
		fs.rmSync(dir, { recursive: true, force: true });
	}
});
