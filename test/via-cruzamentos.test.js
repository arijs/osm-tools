'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var vg = require('../via-geom');
var cruz = require('../scripts/dne-via-cruzamentos');

test('segmentIntersection: cruzamento próprio no meio', function () {
	// + cruzando em (0,0) — lat/lng locais
	var a = [-0.001, -0.001];
	var b = [0.001, 0.001];
	var c = [-0.001, 0.001];
	var d = [0.001, -0.001];
	var hit = vg.segmentIntersection(a, b, c, d);
	assert.ok(hit);
	assert.ok(Math.abs(hit.lat) < 1e-9);
	assert.ok(Math.abs(hit.lng) < 1e-9);
	assert.ok(hit.t > 0 && hit.t < 1);
	assert.ok(hit.u > 0 && hit.u < 1);
});

test('segmentIntersection: só toque em extremo → null', function () {
	var a = [0, 0];
	var b = [0, 0.001];
	var c = [0, 0.001];
	var d = [0.001, 0.001];
	assert.equal(vg.segmentIntersection(a, b, c, d), null);
});

test('segmentIntersection: paralelo → null', function () {
	assert.equal(
		vg.segmentIntersection([0, 0], [0, 0.001], [0.001, 0], [0.001, 0.001]),
		null
	);
});

test('projectPointOnSegment: T no meio do segmento', function () {
	var a = [0, 0];
	var b = [0, 0.002];
	var p = [0.00001, 0.001]; // quase sobre o segmento
	var pr = vg.projectPointOnSegment(p, a, b);
	assert.ok(pr.t > 0.4 && pr.t < 0.6);
	assert.ok(pr.distKm < 0.002);
});

test('densifyBetween: ceil(d/max)-1 pontos, seções ≤ max', function () {
	// ~222 m a leste no equador ≈ 0.002° lng
	var a = [0, 0];
	var b = [0, 0.002];
	var d = vg.distKm(a[0], a[1], b[0], b[1]);
	assert.ok(d > 0.2 && d < 0.25, 'dist≈222m, got ' + d);
	var mid = vg.densifyBetween(a, b, 0.111);
	assert.equal(mid.length, Math.ceil(d / 0.111) - 1);
	assert.ok(mid.length >= 1);
	// cada seção ≤ 0.111
	var chain = [a].concat(mid).concat([b]);
	for (var i = 0; i < chain.length - 1; i++) {
		assert.ok(
			vg.distKm(chain[i][0], chain[i][1], chain[i + 1][0], chain[i + 1][1]) <= 0.111 + 1e-9
		);
	}
});

test('densifyBetween: trecho curto → vazio', function () {
	assert.deepEqual(vg.densifyBetween([0, 0], [0, 0.0005], 0.111), []);
});

test('emitDensifiedPoints: âncora de cruzamento + amostras', function () {
	var pts = [
		[0, 0],
		[0, 0.003] // ~333 m
	];
	var extras = [{ lat: 0, lng: 0.0015, origem: 'cruzamento' }];
	var out = vg.emitDensifiedPoints(pts, extras, 0.111);
	var origens = out.map(function (p) {
		return p.origem;
	});
	assert.ok(origens.indexOf('vertice') >= 0);
	assert.ok(origens.indexOf('cruzamento') >= 0);
	assert.ok(origens.indexOf('amostra') >= 0);
	assert.ok(out.length >= 4); // 2 vértices + 1 cruz + ≥1 amostra
});

test('fixture: cruzamento + T + agregação log_nu', function () {
	// Way A horizontal (lng): log 100
	// Way B vertical cruzando no meio: log 200
	// Way C T: extremo toca o interior de A (não o vértice)
	var ways = new Map();
	ways.set('1', [
		[0, -0.002],
		[0, 0.002]
	]);
	ways.set('2', [
		[-0.001, 0],
		[0.001, 0]
	]);
	ways.set('3', [
		[0.0005, 0.001],
		[0, 0.001]
	]);

	var osmToLogs = new Map();
	osmToLogs.set('1', new Set(['100']));
	osmToLogs.set('2', new Set(['200']));
	osmToLogs.set('3', new Set(['300']));

	var built = cruz.buildSegmentGrid(ways, 0.002);
	var conn = cruz.findConnectionsFull(ways, built.segs, built.grid, 0.002, 0.002);
	var tipos = {};
	conn.hits.forEach(function (h) {
		tipos[h.tipo] = (tipos[h.tipo] || 0) + 1;
	});
	assert.ok(tipos.cruzamento >= 1, 'espera cruzamento A×B: ' + JSON.stringify(conn.hits));
	assert.ok(tipos.conexao >= 1, 'espera T A↔C: ' + JSON.stringify(conn.hits));

	var art = cruz.emitArtifacts(ways, osmToLogs, conn.hits, conn.extrasByOsm, 0.111);
	assert.ok(art.pontoLines.length > 0);
	assert.ok(art.ligLines.length >= 2, 'ligações 100↔200 e 100↔300');

	var pairs = art.ligLines.map(function (l) {
		var p = l.split('@');
		return p[0] + '-' + p[1] + ':' + p[4];
	});
	assert.ok(pairs.some(function (s) {
		return s.indexOf('100-200:cruzamento') === 0;
	}));
	assert.ok(pairs.some(function (s) {
		return s.indexOf('100-300:conexao') === 0;
	}));

	// pontos herdam log_nu
	var logsPontos = new Set(
		art.pontoLines.map(function (l) {
			return l.split('@')[0];
		})
	);
	assert.ok(logsPontos.has('100'));
	assert.ok(logsPontos.has('200'));
	assert.ok(logsPontos.has('300'));
});

test('vértice intermediário compartilhado (caso OSM / Estoril)', function () {
	// Cruzamento tipico: nó no MEIO das duas ways — não extremo↔extremo nem T interior
	var cross = [0, 0];
	var ways = new Map();
	ways.set('a', [
		[0, -0.001],
		cross,
		[0, 0.001]
	]);
	ways.set('b', [
		[-0.001, 0],
		cross,
		[0.001, 0]
	]);
	var osmToLogs = new Map();
	osmToLogs.set('a', new Set(['591259']));
	osmToLogs.set('b', new Set(['618173']));
	var built = cruz.buildSegmentGrid(ways, 0.002);
	var conn = cruz.findConnectionsFull(ways, built.segs, built.grid, 0.002, 0.0015);
	assert.ok(
		conn.hits.some(function (h) {
			return h.tipo === 'conexao' && h.lat === 0 && h.lng === 0;
		}),
		'espera conexao no nó compartilhado: ' + JSON.stringify(conn.hits)
	);
	// não deve depender de cruzamento geométrico (vértices excluídos do intersect)
	var art = cruz.emitArtifacts(ways, osmToLogs, conn.hits, conn.extrasByOsm, 0.111);
	assert.ok(
		art.ligLines.some(function (l) {
			var p = l.split('@');
			return p[0] === '591259' && p[1] === '618173' && p[4] === 'conexao';
		}),
		art.ligLines.join('\n')
	);
});

test('parseBbox + filterWaysByBbox', function () {
	var bbox = cruz.parseBbox('-47.20,-24.05,-46.30,-23.20');
	assert.equal(bbox.minLng, -47.2);
	assert.equal(bbox.minLat, -24.05);
	assert.equal(bbox.maxLng, -46.3);
	assert.equal(bbox.maxLat, -23.2);
	assert.ok(cruz.wayIntersectsBbox([[-23.5, -46.6], [-23.51, -46.61]], bbox));
	assert.equal(cruz.wayIntersectsBbox([[-22, -46.6], [-22.1, -46.61]], bbox), false);
	var ways = new Map();
	ways.set('in', [[-23.5, -46.6], [-23.51, -46.61]]);
	ways.set('out', [[-22, -46.6], [-22.1, -46.61]]);
	assert.equal(cruz.filterWaysByBbox(ways, bbox), 1);
	assert.ok(ways.has('in'));
	assert.equal(ways.has('out'), false);
	assert.throws(function () {
		cruz.parseBbox('1,2,3');
	}, /4 números/);
});

test('mesmo log_nu em duas ways: ligação inter-log omitida (self)', function () {
	var ways = new Map();
	ways.set('10', [
		[0, 0],
		[0, 0.001]
	]);
	ways.set('11', [
		[0, 0.001],
		[0, 0.002]
	]);
	var osmToLogs = new Map();
	osmToLogs.set('10', new Set(['999']));
	osmToLogs.set('11', new Set(['999']));
	var built = cruz.buildSegmentGrid(ways, 0.002);
	var conn = cruz.findConnectionsFull(ways, built.segs, built.grid, 0.002, 0.0015);
	assert.ok(conn.hits.length >= 1, 'ways se tocam');
	var art = cruz.emitArtifacts(ways, osmToLogs, conn.hits, conn.extrasByOsm, 0.111);
	assert.equal(art.ligLines.length, 0, 'self-loop log_nu omitido');
	assert.ok(art.pontoLines.some(function (l) {
		return l.split('@')[3] === 'conexao';
	}));
});
