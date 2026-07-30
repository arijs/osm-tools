'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var geo = require('../geo-cluster');

function f(lat, lng, n, kind) {
	return {
		lat: lat, lng: lng, latMin: lat, latMax: lat, lngMin: lng, lngMax: lng,
		n: n || 1, kind: kind || 'residential'
	};
}

test('clusterFeatures separa vias homônimas distantes', function () {
	// dois grupos a ~50 km: Consolação e um bairro na zona leste
	var feats = [
		f(-23.5550, -46.6600), f(-23.5560, -46.6610), f(-23.5570, -46.6590),
		f(-23.5550, -46.1600), f(-23.5560, -46.1610)
	];
	var g = geo.clusterFeatures(feats);
	assert.equal(g.length, 2);
	var tamanhos = g.map(function (x) { return x.length; }).sort();
	assert.deepEqual(tamanhos, [2, 3]);
});

test('clusterFeatures junta segmentos contíguos de uma via longa', function () {
	// Avenida Paulista: 2,8 km em segmentos de ~300 m, tudo um cluster só
	var feats = [];
	for (var i = 0; i < 10; i++) feats.push(f(-23.5620 + i * 0.0028, -46.6560 + i * 0.0028));
	assert.equal(geo.clusterFeatures(feats).length, 1);
});

test('clusterFeatures aguenta 0 e 1 feature', function () {
	assert.deepEqual(geo.clusterFeatures([]), []);
	assert.equal(geo.clusterFeatures([f(-23.5, -46.6)]).length, 1);
});

test('aggregate: bbox união e centroide ponderado por way_node_count', function () {
	var a = geo.aggregate([
		{ lat: -23.50, lng: -46.60, latMin: -23.51, latMax: -23.49, lngMin: -46.61, lngMax: -46.59, n: 1, kind: 'residential' },
		{ lat: -23.60, lng: -46.70, latMin: -23.61, latMax: -23.59, lngMin: -46.71, lngMax: -46.69, n: 3, kind: 'primary' }
	]);
	assert.equal(a.ways, 2);
	assert.equal(a.weight, 4);
	// centroide puxado para o segmento de peso 3
	assert.ok(Math.abs(a.lat - (-23.575)) < 1e-9);
	assert.ok(Math.abs(a.lng - (-46.675)) < 1e-9);
	assert.equal(a.latMin, -23.61);
	assert.equal(a.latMax, -23.49);
	assert.equal(a.lngMin, -46.71);
	assert.equal(a.lngMax, -46.59);
	assert.deepEqual(a.kinds, ['primary', 'residential']); // ordenado = saída determinística
});

test('aggregate trata n=0 como peso 1 (não divide por zero)', function () {
	var a = geo.aggregate([f(-23.5, -46.6, 0), f(-23.7, -46.8, 0)]);
	assert.equal(a.weight, 2);
	assert.ok(Math.abs(a.lat - (-23.6)) < 1e-9);
});

test('buildFootprint dilata a borda em 1 célula', function () {
	var semDilatar = geo.buildFootprint([f(-23.55, -46.63)], 0.01, 0);
	assert.equal(semDilatar.cells.size, 1);
	var comDilatar = geo.buildFootprint([f(-23.55, -46.63)], 0.01, 1);
	assert.equal(comDilatar.cells.size, 9);   // 3x3
	assert.equal(comDilatar.size, 1);          // `size` = células reais, sem dilatação
});

// Atenção com coordenada negativa: a célula de -23.55 (cell 0.01) é
// floor(-2355) = -2355, que cobre [-23.55, -23.54). Um ponto em -23.5505 cai na
// célula SEGUINTE (-2356), não na mesma — erra fácil ao escrever teste.
test('inFootprint aceita vizinho dentro da dilatação e rejeita longe', function () {
	var fp = geo.buildFootprint([f(-23.55, -46.63)], 0.01, 1);
	assert.equal(geo.inFootprint(fp, -23.55, -46.63), true);
	assert.equal(geo.inFootprint(fp, -23.5450, -46.6250), true);   // mesma célula
	assert.equal(geo.inFootprint(fp, -23.5550, -46.6350), true);   // célula diagonal, coberta pela dilatação
	assert.equal(geo.inFootprint(fp, -23.20, -46.63), false);
	assert.equal(geo.inFootprint(null, -23.55, -46.63), false);
});

test('sem dilatação, a célula vizinha fica de fora', function () {
	var fp = geo.buildFootprint([f(-23.55, -46.63)], 0.01, 0);
	assert.equal(geo.inFootprint(fp, -23.5450, -46.6250), true);
	assert.equal(geo.inFootprint(fp, -23.5550, -46.6350), false);
});

test('footprintOverlap mede a fração dentro da pegada', function () {
	var fp = geo.buildFootprint([f(-23.55, -46.63)], 0.01, 0);
	var dentro = f(-23.5450, -46.6250);
	var fora = f(-23.20, -46.63);
	assert.equal(geo.footprintOverlap(fp, [dentro, dentro]), 1);
	assert.equal(geo.footprintOverlap(fp, [dentro, fora]), 0.5);
	assert.equal(geo.footprintOverlap(fp, []), 0);
});

test('distKm bate com a escala real', function () {
	// 1 grau de latitude ~111 km
	assert.ok(Math.abs(geo.distKm(-23.0, -46.0, -24.0, -46.0) - 111) < 1);
	// Sé -> Paulista ~3 km
	var d = geo.distKm(-23.5505, -46.6333, -23.5614, -46.6559);
	assert.ok(d > 2 && d < 4, 'esperado ~3 km, veio ' + d);
});
