'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var fs = require('fs');
var os = require('os');
var path = require('path');
var jb = require('../scripts/join-brasil');

test('join-brasil: 27 UFs e --only por UF/região', function () {
	assert.equal(jb.allUfs().length, 27);
	assert.deepEqual(jb.selectUfs('sp,rj'), ['RJ', 'SP']);
	assert.deepEqual(jb.selectUfs('sul'), ['PR', 'RS', 'SC']);
	assert.deepEqual(jb.selectUfs(null), jb.allUfs());
});

test('join-brasil: parseCli defaults e extras para o join', function () {
	var o = jb.parseCli([
		'--dne=D:\\dne', '--osm=G:\\osm', '--out=G:\\out',
		'--only=sp', '--sem-fuzzy', '--quiet'
	]);
	assert.equal(o.dneDir, 'D:\\dne');
	assert.equal(o.osmDir, 'G:\\osm');
	assert.equal(o.outDir, 'G:\\out');
	assert.equal(o.only, 'sp');
	assert.deepEqual(o.extra, ['--sem-fuzzy', '--quiet']);
	var args = jb.buildJoinArgs(o, 'SP');
	assert.ok(args[0].indexOf('dne-geo-join.js') >= 0);
	assert.ok(args.indexOf('--uf=SP') >= 0);
	assert.ok(args.indexOf('--sem-fuzzy') >= 0);
});

test('join-brasil: jobDone só com relatório JSON válido', function () {
	var dir = fs.mkdtempSync(path.join(os.tmpdir(), 'join-br-'));
	try {
		assert.equal(jb.jobDone(dir, 'SP'), false);
		fs.writeFileSync(path.join(dir, 'DNE_GEO_RELATORIO_SP.json'), '{nao json');
		assert.equal(jb.jobDone(dir, 'SP'), false);
		fs.writeFileSync(
			jb.reportPath(dir, 'SP'),
			JSON.stringify({ uf: 'SP', geo_status: { ok: 10 }, linhas_dne: 12 })
		);
		assert.equal(jb.jobDone(dir, 'SP'), true);
	} finally {
		fs.rmSync(dir, { recursive: true, force: true });
	}
});
