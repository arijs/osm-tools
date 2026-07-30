'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var fs = require('fs');
var path = require('path');
var { execFileSync } = require('child_process');
var {
	runExtractGeocode,
	parseDatasets,
	logradouroKind,
	altNames,
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

test('logradouroKind aceita área além de highway', function () {
	assert.equal(logradouroKind({ highway: 'residential' }), 'residential');
	assert.equal(logradouroKind({ place: 'square' }), 'square');
	assert.equal(logradouroKind({ leisure: 'park' }), 'park');
	assert.equal(logradouroKind({ leisure: 'garden' }), 'park');
	assert.equal(logradouroKind({ landuse: 'village_green' }), 'park');
	// highway ganha quando os dois existem (praça com via de pedestre desenhada)
	assert.equal(logradouroKind({ highway: 'pedestrian', place: 'square' }), 'pedestrian');
	// nada disso não é logradouro
	assert.equal(logradouroKind({ landuse: 'residential', name: 'Jardim X' }), '');
	assert.equal(logradouroKind({ building: 'yes' }), '');
	assert.equal(logradouroKind(null), '');
});

test('altNames coleta variantes sem repetir o nome principal', function () {
	assert.deepEqual(
		altNames({ name: 'Rua Augusta', alt_name: 'Rua Augusta Velha' }, 'Rua Augusta'),
		['Rua Augusta Velha']
	);
	// multi-valor do OSM separado por ; e sem duplicata entre tags
	assert.deepEqual(
		altNames(
			{ alt_name: 'A;B', old_name: 'B', short_name: 'C' },
			'Rua X'
		),
		['A', 'B', 'C']
	);
	assert.deepEqual(altNames({ name: 'Só o nome' }, 'Só o nome'), []);
	assert.deepEqual(altNames(null, ''), []);
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
	var byName = {};
	logs.forEach(function (row) {
		byName[row[1]] = row;
	});

	var augusta = byName['Rua Augusta'];
	assert.ok(augusta, 'Rua Augusta');
	assert.equal(augusta[3], 'residential');
	// centroid should resolve both nodes via pass 2
	assert.ok(augusta[10] !== '', 'lat do logradouro');
	assert.equal(augusta[16], '2');
	assert.equal(augusta[17], 'Rua Augusta Velha', 'name_alt');
	assert.equal(augusta[18], nameNorm('Rua Augusta Velha'), 'name_alt_norm');
	assert.equal(augusta[19], 'way', 'osm_type');

	// praça como way fechada: kind=square, bbox real (não degenerada)
	var praca = byName['Praça da Sé'];
	assert.ok(praca, 'praça extraída (place=square)');
	assert.equal(praca[3], 'square');
	assert.equal(praca[19], 'way');
	assert.ok(Number(praca[13]) > Number(praca[12]), 'bbox de área tem altura');

	// parque como área
	var parque = byName['Parque do Ibirapuera'];
	assert.ok(parque, 'parque extraído (leisure=park)');
	assert.equal(parque[3], 'park');

	// praça como nó: ponto exato, bbox degenerada, osm_type=node
	var pracaNo = byName['Praça do Correio'];
	assert.ok(pracaNo, 'praça extraída (node place=square)');
	assert.equal(pracaNo[3], 'square');
	assert.equal(pracaNo[19], 'node');
	assert.equal(pracaNo[12], pracaNo[13], 'nó não tem extensão');
	assert.equal(pracaNo[16], '1');

	// highway ganha de place quando os dois existem
	assert.equal(byName['Largo Teste'][3], 'pedestrian');

	// landuse nomeado NÃO é logradouro
	assert.equal(byName['Jardim Teste'], undefined, 'landuse=residential fora');
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
