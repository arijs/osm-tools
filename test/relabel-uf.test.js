'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var fs = require('fs');
var os = require('os');
var path = require('path');
var txtAt = require('../txt-at-writer');
var relabelUf = require('../scripts/relabel-uf');

function linhaLogradouro(id, uf, lat, lng, tipo) {
	var c = new Array(20).fill('');
	c[0] = String(id);
	c[1] = 'Rua ' + id;
	c[2] = 'rua ' + id;
	c[3] = 'residential';
	c[4] = uf;
	c[10] = String(lat);
	c[11] = String(lng);
	c[19] = tipo || 'way';
	return c;
}

function linhaBairro(id, uf, lat, lng) {
	var c = new Array(15).fill('');
	c[0] = 'node';
	c[1] = String(id);
	c[2] = 'Bairro ' + id;
	c[3] = 'bairro ' + id;
	c[4] = uf;
	c[8] = String(lat);
	c[9] = String(lng);
	return c;
}

function linhaAddr(id, lat, lng) {
	var c = new Array(10).fill('');
	c[0] = String(id);
	c[1] = String(lat);
	c[2] = String(lng);
	c[3] = 'Rua ' + id;
	return c;
}

/** polyline do GEOM: primeiro ponto absoluto em 1e-6 de grau, resto em deltas */
function polyline(lat, lng) {
	return Math.round(lat * 1e6) + ',' + Math.round(lng * 1e6) + ';1000,1000';
}

function montarFatia(base, nome, logradouro, geom, bairro, addr) {
	var dir = path.join(base, nome);
	var w = txtAt.createTxtAtWriter(dir, { shardLines: 2 });
	logradouro.forEach(function (l) {
		w.write('OSM_LOGRADOURO_' + l[4], l);
	});
	geom.forEach(function (g) {
		w.write('OSM_LOGRADOURO_GEOM_' + g.uf, [String(g.id), g.polyline, '0']);
	});
	(bairro || []).forEach(function (b) {
		w.write('OSM_BAIRRO', b);
	});
	(addr || []).forEach(function (a) {
		w.write('OSM_ADDR_POINT_' + a.uf, a.campos);
	});
	return w.flush();
}

function lerDataset(out, chave) {
	var dir = path.join(out, chave);
	if (!fs.existsSync(dir)) return null;
	var man = JSON.parse(fs.readFileSync(path.join(dir, 'MANIFEST.json'), 'utf8'));
	var linhas = [];
	man.shards.forEach(function (s) {
		fs.readFileSync(path.join(dir, man.shard_dir, s.file), 'utf8')
			.split('\n')
			.forEach(function (l) {
				if (l) linhas.push(l.split('@'));
			});
	});
	assert.equal(man.total_lines, linhas.length, chave + ': MANIFEST bate com o arquivo');
	return linhas;
}

// Patrocínio saía GO e a praça do Rio saía MG: é isso que o re-rótulo desfaz,
// sem re-extrair o PBF. Coordenadas reais, as mesmas de test/uf-br.test.js.
var PATROCINIO = [-18.9375, -46.9856];
var CATALAO = [-18.1658, -47.9469];
var RIO = [-22.9231, -43.6918];
var MAR = [-23.2, -41.5];

test('relabel-uf — polígono manda, GEOM segue o irmão, duplicata entra uma vez', async function (t) {
	var base = fs.mkdtempSync(path.join(os.tmpdir(), 'relabel-base-'));
	var out = fs.mkdtempSync(path.join(os.tmpdir(), 'relabel-out-'));
	t.after(function () {
		fs.rmSync(base, { recursive: true, force: true });
		fs.rmSync(out, { recursive: true, force: true });
	});

	// fatia mg, como o paliativo de 18/08 gravou: tudo rotulado MG
	await montarFatia(
		base,
		'mg',
		[
			linhaLogradouro(100, 'MG', PATROCINIO[0], PATROCINIO[1]),
			linhaLogradouro(200, 'MG', RIO[0], RIO[1]),
			linhaLogradouro(300, 'MG', CATALAO[0], CATALAO[1]),
			linhaLogradouro(400, 'MG', PATROCINIO[0], PATROCINIO[1], 'node')
		],
		[
			// a via 100 começa do outro lado da divisa: sozinho, este ponto daria GO
			{ id: 100, uf: 'MG', polyline: polyline(CATALAO[0], CATALAO[1]) },
			{ id: 200, uf: 'MG', polyline: polyline(RIO[0], RIO[1]) }
		],
		[linhaBairro(700, 'MG', RIO[0], RIO[1]), linhaBairro(701, 'MG', PATROCINIO[0], PATROCINIO[1])],
		[{ uf: 'MG', campos: linhaAddr(800, CATALAO[0], CATALAO[1]) }]
	);

	// fatia rj: viu a mesma praça do Rio (o retângulo do RJ também a continha) e
	// uma via no mar, que polígono nenhum reivindica
	await montarFatia(
		base,
		'rj',
		[
			linhaLogradouro(200, 'RJ', RIO[0], RIO[1]),
			linhaLogradouro(500, 'RJ', MAR[0], MAR[1])
		],
		[{ id: 200, uf: 'RJ', polyline: polyline(RIO[0], RIO[1]) }]
	);

	var r = await relabelUf.relabel({ base: base, out: out, shardLines: 2 });
	var lgr = r.por_familia.logradouro;

	assert.equal(lgr.lidas, 6);
	assert.equal(lgr.duplicadas, 1, 'a praça do Rio foi vista por duas fatias');
	assert.equal(lgr.gravadas, 5);

	var mg = lerDataset(out, 'OSM_LOGRADOURO_MG');
	assert.deepEqual(mg.map(function (l) { return l[0]; }).sort(), ['100', '400']);
	assert.ok(mg.every(function (l) { return l[4] === 'MG'; }), 'coluna uf reescrita');

	var rj = lerDataset(out, 'OSM_LOGRADOURO_RJ');
	assert.deepEqual(rj.map(function (l) { return l[0]; }).sort(), ['200', '500']);

	// Catalão estava dentro do retângulo de MG, mas é GO
	var go = lerDataset(out, 'OSM_LOGRADOURO_GO');
	assert.deepEqual(go.map(function (l) { return l[0]; }), ['300']);

	// no mar o polígono não opina: o rótulo antigo é o que existe
	assert.equal(rj.filter(function (l) { return l[0] === '500'; })[0][4], 'RJ');

	// GEOM fica irmão do logradouro, não do próprio primeiro ponto
	assert.deepEqual(lerDataset(out, 'OSM_LOGRADOURO_GEOM_MG').map(function (l) { return l[0]; }), ['100']);
	assert.equal(lerDataset(out, 'OSM_LOGRADOURO_GEOM_GO'), null);
	assert.deepEqual(lerDataset(out, 'OSM_LOGRADOURO_GEOM_RJ').map(function (l) { return l[0]; }), ['200']);
	assert.equal(r.por_familia.geom.duplicadas, 1);
	assert.equal(r.por_familia.geom.sem_irmao, 0);

	// OSM_BAIRRO é dataset único: o que muda é a coluna uf, não o arquivo
	var bairro = lerDataset(out, 'OSM_BAIRRO');
	assert.deepEqual(
		bairro.map(function (l) { return l[1] + ':' + l[4]; }).sort(),
		['700:RJ', '701:MG']
	);

	// addr point não tem coluna uf: o rótulo vinha do nome do dataset
	assert.deepEqual(lerDataset(out, 'OSM_ADDR_POINT_GO').map(function (l) { return l[0]; }), ['800']);
	assert.equal(lerDataset(out, 'OSM_ADDR_POINT_MG'), null);

	var doc = JSON.parse(fs.readFileSync(path.join(out, 'RELABEL-SUMMARY.json'), 'utf8'));
	var resumo = doc.runs[doc.runs.length - 1];
	assert.equal(resumo.por_familia.logradouro.de_para['MG→RJ'], 1);
	assert.equal(resumo.por_familia.logradouro.de_para['MG→GO'], 1);
	assert.equal(resumo.por_familia.addr.de_para['MG→GO'], 1);
	assert.ok(String(resumo.malha.fonte).indexOf('ibge.gov.br') >= 0);
});

test('relabel-uf — --datasets acrescenta à mesma pasta sem tocar no resto', async function (t) {
	var base = fs.mkdtempSync(path.join(os.tmpdir(), 'relabel-inc-'));
	var out = fs.mkdtempSync(path.join(os.tmpdir(), 'relabel-inc-out-'));
	t.after(function () {
		fs.rmSync(base, { recursive: true, force: true });
		fs.rmSync(out, { recursive: true, force: true });
	});

	await montarFatia(
		base,
		'sudeste',
		[linhaLogradouro(1, 'MG', RIO[0], RIO[1])],
		[],
		[linhaBairro(2, 'MG', RIO[0], RIO[1])],
		[]
	);

	await relabelUf.relabel({ base: base, out: out, shardLines: 2, datasets: ['logradouro'] });
	assert.equal(lerDataset(out, 'OSM_BAIRRO'), null);

	await relabelUf.relabel({ base: base, out: out, shardLines: 2, datasets: ['bairro'] });
	assert.deepEqual(lerDataset(out, 'OSM_LOGRADOURO_RJ').map(function (l) { return l[0]; }), ['1']);
	assert.deepEqual(lerDataset(out, 'OSM_BAIRRO').map(function (l) { return l[4]; }), ['RJ']);

	var doc = JSON.parse(fs.readFileSync(path.join(out, 'RELABEL-SUMMARY.json'), 'utf8'));
	assert.equal(doc.runs.length, 2, 'cada corrida deixa seu resumo');

	// regravar a mesma família reabriria o shard 000001 do zero: tem de recusar
	assert.throws(function () {
		relabelUf.relabel({ base: base, out: out, shardLines: 2, datasets: ['bairro'] });
	}, /já tem/);
});

test('relabel-uf — --dirs aponta as fatias uma a uma', async function (t) {
	var base = fs.mkdtempSync(path.join(os.tmpdir(), 'relabel-dirs-'));
	var out = fs.mkdtempSync(path.join(os.tmpdir(), 'relabel-dirs-out-'));
	t.after(function () {
		fs.rmSync(base, { recursive: true, force: true });
		fs.rmSync(out, { recursive: true, force: true });
	});

	await montarFatia(base, 'mg', [linhaLogradouro(9, 'MG', PATROCINIO[0], PATROCINIO[1])], []);
	var r = await relabelUf.relabel({
		dirs: [path.join(base, 'mg')], out: out, shardLines: 2, datasets: ['logradouro']
	});
	assert.deepEqual(r.fatias, ['mg']);
	assert.deepEqual(lerDataset(out, 'OSM_LOGRADOURO_MG').map(function (l) { return l[0]; }), ['9']);
});

test('relabel-uf — dry-run não escreve nada', async function (t) {
	var base = fs.mkdtempSync(path.join(os.tmpdir(), 'relabel-dry-'));
	var out = fs.mkdtempSync(path.join(os.tmpdir(), 'relabel-dry-out-'));
	t.after(function () {
		fs.rmSync(base, { recursive: true, force: true });
		fs.rmSync(out, { recursive: true, force: true });
	});

	await montarFatia(base, 'mg', [linhaLogradouro(1, 'MG', RIO[0], RIO[1])], []);
	var r = await relabelUf.relabel({ base: base, out: out, shardLines: 2, dryRun: true });

	assert.equal(r.por_familia.logradouro.gravadas, 1);
	assert.equal(r.por_familia.logradouro.por_uf.RJ, 1);
	assert.deepEqual(fs.readdirSync(out), []);
});

test('relabel-uf — primeiroPonto decodifica a polyline do GEOM', function () {
	assert.deepEqual(relabelUf.primeiroPonto('-23552000,-46632000;-1000,-1000'), {
		lat: -23.552,
		lng: -46.632
	});
	assert.equal(relabelUf.primeiroPonto(''), null);
	assert.equal(relabelUf.primeiroPonto('lixo'), null);
});
