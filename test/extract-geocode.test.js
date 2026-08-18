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
var polyline = require('../geo-polyline');

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

test('formatExtractSummary inclui bairro e logradouro', function () {
	var { formatExtractSummary } = require('../extract-geocode-pbf');
	var line = formatExtractSummary({
		municipio: 0,
		bairro: 1994,
		logradouro: 179239,
		logradouroNoGeom: 0,
		streetWaves: 18
	});
	assert.match(line, /Municípios: 0/);
	assert.match(line, /Bairros: 1994/);
	assert.match(line, /Logradouros: 179239 \(sem geom: 0\)/);
	assert.match(line, /waves=18/);
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

test('filtro --uf=SP mantém logradouros SP e descarta fatia alheia', async function () {
	ensureGeocodePbf();
	var outSp = path.join(fixtures, 'geocode-mini-out-uf-sp');
	var outRj = path.join(fixtures, 'geocode-mini-out-uf-rj');
	rmrf(outSp);
	rmrf(outRj);

	function rows(dir, name) {
		var p = path.join(dir, name);
		if (!fs.existsSync(p)) return [];
		return fs.readFileSync(p, 'utf8').split(/\r?\n/).filter(Boolean);
	}

	var result = await runExtractGeocode({
		inputPath: geoPbf,
		outDir: outSp,
		quiet: true,
		resume: false,
		datasets: parseDatasets('estado,municipio,bairro,logradouro'),
		uf: 'SP'
	});
	assert.equal(result.error, null);
	assert.ok(result.counts.logradouro >= 1);
	assert.ok(rows(outSp, 'OSM_LOGRADOURO_SP.TXT').length >= 1);

	// filtro RJ no mesmo fixture (tudo em SP) → zero logradouro gravado
	var r2 = await runExtractGeocode({
		inputPath: geoPbf,
		outDir: outRj,
		quiet: true,
		resume: false,
		datasets: parseDatasets('logradouro'),
		uf: 'RJ'
	});
	assert.equal(r2.error, null);
	assert.equal(r2.counts.logradouro, 0);
	rmrf(outSp);
	rmrf(outRj);
});

test('wave-streets força flush mid-pass e ainda resolve geom', async function () {
	ensureGeocodePbf();
	var outWave = path.join(fixtures, 'geocode-mini-out-wave');
	rmrf(outWave);

	var result = await runExtractGeocode({
		inputPath: geoPbf,
		outDir: outWave,
		quiet: true,
		resume: false,
		datasets: parseDatasets('logradouro'),
		// 1 street por onda → pelo menos uma wave se houver ≥1 way
		waveStreets: 1,
		waveNodes: 2
	});
	assert.equal(result.error, null);
	assert.ok(result.counts.logradouro >= 1);
	assert.ok(result.counts.streetWaves >= 1, 'deve ter feito ao menos 1 wave');
	assert.ok(result.counts.logradouroNoGeom === 0 || result.counts.logradouroNoGeom == null);
	rmrf(outWave);
});

test('parseCli aceita --uf --region --wave-nodes', function () {
	var { parseCli } = require('../extract-geocode-pbf');
	var opts = parseCli([
		'node',
		'extract-geocode-pbf.js',
		'x.pbf',
		'--out=y',
		'--uf=SP,MG',
		'--region=sul',
		'--wave-nodes=1000',
		'--wave-streets=50'
	]);
	assert.ok(opts.ufAllow.SP && opts.ufAllow.MG && opts.ufAllow.RS);
	assert.equal(opts.waveNodes, 1000);
	assert.equal(opts.waveStreets, 50);
});

test('soft-stop via onControl para no blob atual (não completa o extract)', async function () {
	ensureGeocodePbf();
	var outStop = path.join(fixtures, 'geocode-mini-out-stop');
	rmrf(outStop);
	var t0 = Date.now();
	var result = await runExtractGeocode({
		inputPath: geoPbf,
		outDir: outStop,
		quiet: true,
		resume: false,
		datasets: parseDatasets('logradouro'),
		// forçaria várias waves se rodasse até o fim
		waveStreets: 1,
		waveNodes: 2,
		onControl: function (c) {
			// cancela assim que o runner expõe o controle (antes/durante o 1º blob)
			c.softStop('test');
		}
	});
	var elapsed = Date.now() - t0;
	assert.equal(result.stoppedEarly, true);
	assert.equal(result.stopReason, 'soft-stop');
	assert.ok(elapsed < 5000, 'soft-stop deve ser rápido, levou ' + elapsed + 'ms');
	rmrf(outStop);
});
test('--way-geom emite OSM_LOGRADOURO_GEOM_{UF} com o traçado da way', async function () {
	ensureGeocodePbf();
	var outGeom = path.join(fixtures, 'geocode-mini-out-geom');
	rmrf(outGeom);

	var result = await runExtractGeocode({
		inputPath: geoPbf,
		outDir: outGeom,
		quiet: true,
		resume: false,
		datasets: parseDatasets('logradouro,geom')
	});
	assert.equal(result.error, null);

	function rowsGeom(name) {
		var p = path.join(outGeom, name);
		if (!fs.existsSync(p)) return [];
		return fs.readFileSync(p, 'utf8').split(/\r?\n/).filter(Boolean)
			.map(function (l) { return l.split('@'); });
	}

	var logs = rowsGeom('OSM_LOGRADOURO_SP.TXT');
	var geoms = rowsGeom('OSM_LOGRADOURO_GEOM_SP.TXT');
	assert.ok(geoms.length >= 1, 'arquivo de geometria emitido');

	// Todo id do GEOM existe no logradouro (o inverso não vale: nó e way sem
	// traçado ficam de fora).
	var idsLog = {};
	logs.forEach(function (r) { idsLog[r[0]] = r; });
	geoms.forEach(function (r) {
		assert.ok(idsLog[r[0]], 'osm_id ' + r[0] + ' também está em OSM_LOGRADOURO_SP');
		assert.ok(r[1].indexOf(';') > 0, 'polyline com 2+ pontos');
		assert.equal(r.length, 3, 'osm_id@polyline@oneway');
		assert.ok(/^[0123]$/.test(r[2]), 'oneway compacto 0–3, got ' + r[2]);
	});

	// Rua Augusta: way de 2 nós, resolvida na pass 2 → traçado com 2 pontos que
	// batem com o centroide da linha principal.
	var augustaLog = logs.find(function (r) { return r[1] === 'Rua Augusta'; });
	assert.ok(augustaLog, 'Rua Augusta no extract');
	var augustaGeom = geoms.find(function (r) { return r[0] === augustaLog[0]; });
	assert.ok(augustaGeom, 'Rua Augusta tem traçado');
	var pts = polyline.decodePolyline(augustaGeom[1]);
	assert.equal(pts.length, 2);
	var mLat = (pts[0][0] + pts[1][0]) / 2;
	assert.ok(Math.abs(mLat - Number(augustaLog[10])) < 1e-6, 'centroide bate com a linha');

	// Praça mapeada como NÓ não tem traçado; a praça como way fechada tem, e o
	// anel fecha (primeiro ponto == último).
	var pracaNode = logs.find(function (r) { return r[19] === 'node'; });
	if (pracaNode) {
		assert.ok(!geoms.some(function (r) { return r[0] === pracaNode[0]; }),
			'nó não vira linha');
	}
	var pracaWay = logs.find(function (r) { return r[3] === 'square' && r[19] === 'way'; });
	if (pracaWay) {
		var anel = geoms.find(function (r) { return r[0] === pracaWay[0]; });
		assert.ok(anel, 'praça como way tem traçado');
		var pa = polyline.decodePolyline(anel[1]);
		assert.deepEqual(pa[0], pa[pa.length - 1], 'anel fechado');
	}

	// Contadores no resumo.
	assert.ok(result.counts.logradouroGeom >= 1);
	assert.ok(result.counts.logradouroGeomPontos >= 2 * result.counts.logradouroGeom);

	// README-colunas documenta o arquivo novo.
	var readme = fs.readFileSync(path.join(outGeom, 'README-colunas.md'), 'utf8');
	assert.ok(readme.indexOf('OSM_LOGRADOURO_GEOM_{UF}') >= 0);
	assert.ok(readme.indexOf('osm_id@polyline@oneway') >= 0);

	rmrf(outGeom);
});

test('sem --way-geom nenhum arquivo de geometria é criado', async function () {
	ensureGeocodePbf();
	var outSem = path.join(fixtures, 'geocode-mini-out-sem-geom');
	rmrf(outSem);
	var result = await runExtractGeocode({
		inputPath: geoPbf,
		outDir: outSem,
		quiet: true,
		resume: false,
		datasets: parseDatasets('logradouro')
	});
	assert.equal(result.error, null);
	assert.ok(result.counts.logradouro >= 1);
	var achou = fs.readdirSync(outSem).some(function (n) {
		return n.indexOf('OSM_LOGRADOURO_GEOM') === 0;
	});
	assert.equal(achou, false, 'geometria é opt-in');
	assert.equal(result.counts.logradouroGeom, 0);
	rmrf(outSem);
});

test('geometria acompanha o fatiamento do logradouro, com MANIFEST próprio', async function () {
	// `--shard-datasets=logradouro` casa o prefixo `OSM_LOGRADOURO`, então o
	// arquivo de geometria fatia junto sem ninguém pedir. Vale fixar: o
	// carregador do ddsoft resolve os dois pelo mesmo `resolveDatasetPaths`.
	ensureGeocodePbf();
	var outSh = path.join(fixtures, 'geocode-mini-out-geom-shard');
	rmrf(outSh);
	var result = await runExtractGeocode({
		inputPath: geoPbf,
		outDir: outSh,
		quiet: true,
		resume: false,
		datasets: parseDatasets('logradouro,geom'),
		shardLines: 2,
		shardDatasets: ['logradouro']
	});
	assert.equal(result.error, null);

	var res = txtAt.resolveDatasetPaths(outSh, 'OSM_LOGRADOURO_GEOM_SP');
	assert.equal(res.mode, 'shard');
	assert.ok(res.paths.length >= 1);
	assert.equal(res.totalLines, result.counts.logradouroGeom);
	rmrf(outSh);
});
