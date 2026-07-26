'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var fs = require('fs');
var path = require('path');
var { execFileSync } = require('child_process');
var {
	runExtractGeocode,
	parseDatasets,
	nameNorm
} = require('../extract-geocode-pbf');
var txtAt = require('../txt-at-writer');

var fixtures = path.join(__dirname, 'fixtures');
var geoPbf = path.join(fixtures, 'geocode-mini.osm.pbf');
var outDir = path.join(fixtures, 'geocode-mini-out');

function ensureGeocodePbf() {
	execFileSync(
		process.execPath,
		[path.join(__dirname, '..', 'scripts', 'write-geocode-pbf.js'), geoPbf],
		{ stdio: 'pipe' }
	);
}

function rmrf(dir) {
	if (!fs.existsSync(dir)) return;
	fs.rmSync(dir, { recursive: true, force: true });
}

function readTxt(name) {
	var p = path.join(outDir, name);
	if (!fs.existsSync(p)) return [];
	return fs
		.readFileSync(p, 'utf8')
		.split(/\r?\n/)
		.filter(Boolean)
		.map(function (line) {
			return line.split('@');
		});
}

test('parseDatasets defaults e subset', function () {
	var all = parseDatasets('all');
	assert.equal(all.municipio, true);
	assert.equal(all.addr, false);
	var sub = parseDatasets('municipio,estado');
	assert.equal(sub.municipio, true);
	assert.equal(sub.estado, true);
	assert.equal(sub.logradouro, false);
});

test('formatRow usa delimitador @', function () {
	assert.equal(txtAt.formatRow(['a', 'b', 'c']), 'a@b@c\n');
	assert.equal(txtAt.formatRow(['x@y', 'z']), 'x y@z\n');
});

test('runExtractGeocode emite TXT por nível e logradouro por UF', async function () {
	ensureGeocodePbf();
	rmrf(outDir);

	var result = await runExtractGeocode({
		inputPath: geoPbf,
		outDir: outDir,
		quiet: true,
		resume: false,
		datasets: parseDatasets('estado,municipio,bairro,logradouro,addr')
	});

	assert.equal(result.error, null);
	assert.equal(result.stoppedEarly, false);
	assert.ok(result.counts.municipio >= 1);
	assert.ok(result.counts.estado >= 1);
	assert.ok(result.counts.bairro >= 1);
	assert.ok(result.counts.logradouro >= 1);
	assert.ok(result.counts.addr >= 1);

	var munis = readTxt('OSM_MUNICIPIO.TXT');
	assert.ok(munis.length >= 1);
	// osm_type@osm_id@ibge@uf@name@name_norm@...
	var m = munis.find(function (row) {
		return row[2] === '3550308';
	});
	assert.ok(m, 'município com IBGE 3550308');
	assert.equal(m[3], 'SP');
	assert.equal(m[5], nameNorm('São Paulo'));
	assert.ok(Number(m[6]) < -23 && Number(m[6]) > -24);

	var estados = readTxt('OSM_ESTADO.TXT');
	assert.ok(estados.length >= 1);
	assert.ok(
		estados.some(function (row) {
			return row[2] === 'SP';
		})
	);

	var bairros = readTxt('OSM_BAIRRO.TXT');
	assert.ok(bairros.length >= 1);

	var logs = readTxt('OSM_LOGRADOURO_SP.TXT');
	assert.ok(logs.length >= 1, 'logradouro segmentado em SP (two-pass)');
	assert.equal(logs[0][1], 'Rua Augusta');
	assert.equal(logs[0][3], 'residential');
	// centroid should resolve both nodes via pass 2
	assert.ok(logs[0][10] !== '', 'lat do logradouro');
	assert.equal(logs[0][16], '2');
	assert.ok(result.counts.logradouroNoGeom === 0, 'two-pass deve resolver geom');
	// não deve cair em XX se o ponto está em SP
	assert.ok(!fs.existsSync(path.join(outDir, 'OSM_LOGRADOURO_XX.TXT')) ||
		readTxt('OSM_LOGRADOURO_XX.TXT').length === 0);

	var addrs = readTxt('OSM_ADDR_POINT_SP.TXT');
	assert.ok(addrs.length >= 1);

	assert.ok(fs.existsSync(path.join(outDir, 'README-colunas.md')));
	assert.ok(fs.existsSync(path.join(outDir, 'extract-checkpoint.json')));

	rmrf(outDir);
});
