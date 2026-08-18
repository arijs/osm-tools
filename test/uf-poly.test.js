'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var poly = require('../uf-poly');
var DATA = require('../uf-poly.json');

/** Gerador de semente fixa — o pipeline é comparado entre execuções. */
function rng(seed) {
	return function () {
		seed = (seed * 1103515245 + 12345) % 2147483648;
		return seed / 2147483648;
	};
}

test('grade coberta e barata', function () {
	var s = poly.gridStats();
	assert.equal(s.ufs, 27);
	assert.ok(s.celulas > 10000, 'grade cobre o Brasil: ' + s.celulas);
	// só a célula cortada por divisa/costa paga point-in-polygon; se esta conta
	// passar de ~20% a consulta deixou de ser O(1) amortizada
	assert.ok(s.ambiguas / s.celulas < 0.2, 'ambíguas: ' + s.ambiguas + '/' + s.celulas);
	assert.ok(String(DATA.fonte).indexOf('ibge.gov.br') >= 0, 'fonte citada');
});

test('ponto exatamente sobre a divisa — vence a primeira em ordem alfabética', function () {
	// um vértice que MG e GO compartilham: está dentro dos dois (a divisa é dos
	// dois), e a regra do desempate é alfabética — GO. Arbitrário de propósito;
	// o que não pode é variar entre execuções.
	var emGo = {};
	DATA.ufs.GO.forEach(function (r) {
		for (var i = 0; i < r.length; i += 2) emGo[r[i] + ',' + r[i + 1]] = true;
	});
	var achado = null;
	DATA.ufs.MG.forEach(function (r) {
		for (var i = 0; !achado && i < r.length; i += 2) {
			if (emGo[r[i] + ',' + r[i + 1]]) achado = [r[i + 1], r[i]];
		}
	});
	assert.ok(achado, 'MG e GO compartilham vértices na malha');
	assert.equal(poly.ufFromPointPoly(achado[0], achado[1]), 'GO');
	assert.equal(poly.ufFromPointPoly(achado[0], achado[1]), 'GO');
});

test('fora de qualquer UF responde vazio', function () {
	assert.equal(poly.ufFromPointPoly(-23.2, -41.5), ''); // mar, ao largo de Cabo Frio
	assert.equal(poly.ufFromPointPoly(-6, -30), ''); // Atlântico
	assert.equal(poly.ufFromPointPoly(-34.6, -58.4), ''); // Buenos Aires
	assert.equal(poly.ufFromPointPoly(null, null), '');
	assert.equal(poly.ufFromPointPoly(NaN, -46), '');
});

test('determinístico: mesma entrada, mesma saída', function () {
	function rodada() {
		var r = rng(20260818);
		var out = [];
		for (var i = 0; i < 2000; i++) {
			out.push(poly.ufFromPointPoly(-33.75 + r() * 39.3, -73.99 + r() * 39.3));
		}
		return out.join('|');
	}
	assert.equal(rodada(), rodada());
});

// Teto grosseiro de desempenho: o extract chama isto por feature e o job de MG
// levava ~1h30. Não é benchmark, é rede de segurança — só cai se a consulta
// deixar de ser indexada (varredura linear dos 27 polígonos seria ~50x isto).
test('desempenho: 200 mil pontos aleatórios sob o teto', function () {
	var r = rng(7);
	var N = 200000;
	var lat = new Float64Array(N);
	var lng = new Float64Array(N);
	for (var i = 0; i < N; i++) {
		lat[i] = -33.75 + r() * 39.3;
		lng[i] = -73.99 + r() * 39.3;
	}
	var t0 = Date.now();
	var dentro = 0;
	for (var j = 0; j < N; j++) if (poly.ufFromPointPoly(lat[j], lng[j])) dentro++;
	var ms = Date.now() - t0;
	assert.ok(dentro > N / 4, 'boa parte dos pontos cai no Brasil: ' + dentro);
	assert.ok(ms < 3000, N + ' consultas em ' + ms + ' ms (teto 3000)');
});
