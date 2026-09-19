'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var fs = require('fs');
var os = require('os');
var path = require('path');
var vb = require('../scripts/via-brasil');

test('via-brasil: parseCli defaults e extras para o cruzamentos', function () {
	var o = vb.parseCli(['--out=G:\\via', '--only=sul', '--cell=0.003']);
	assert.equal(o.dneGeo, 'G:\\dne-geo-br-join5');
	assert.equal(o.geom, 'G:\\osm-geo-br-uf');
	assert.equal(o.outDir, 'G:\\via');
	assert.equal(o.only, 'sul');
	var args = vb.buildArgs(o, 'RS');
	assert.ok(args[0].indexOf('dne-via-cruzamentos.js') >= 0);
	assert.ok(args.indexOf('--uf=RS') >= 0);
	assert.ok(args.indexOf('--cell=0.003') >= 0);
	assert.ok(args.indexOf('--bbox') < 0, 'sem bbox: a UF inteira');
});

test('via-brasil: jobDone só com relatório terminado', function () {
	var dir = fs.mkdtempSync(path.join(os.tmpdir(), 'via-br-'));
	try {
		assert.equal(vb.jobDone(dir, 'SP'), false);
		fs.writeFileSync(vb.reportPath(dir, 'SP'), '{"uf":"SP"}');
		assert.equal(vb.jobDone(dir, 'SP'), false, 'sem finished_at = morreu no meio');
		fs.writeFileSync(vb.reportPath(dir, 'SP'), JSON.stringify({ uf: 'SP', finished_at: 'x' }));
		assert.equal(vb.jobDone(dir, 'SP'), true);
	} finally {
		fs.rmSync(dir, { recursive: true, force: true });
	}
});
