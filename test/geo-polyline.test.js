'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var poly = require('../geo-polyline');
var txtAt = require('../txt-at-writer');

// Codec da geometria da way dentro do TXT `@` (docs/geo/geometria-via-destaque.md).

test('ida e volta preserva as coordenadas na precisão do formato', function () {
	var pontos = [
		[-23.552, -46.632],
		[-23.553, -46.633],
		[-23.5535, -46.6335]
	];
	var enc = poly.encodePolyline(pontos);
	var dec = poly.decodePolyline(enc);
	assert.equal(dec.length, 3);
	for (var i = 0; i < pontos.length; i++) {
		assert.ok(Math.abs(dec[i][0] - pontos[i][0]) < 1e-6, 'lat ' + i);
		assert.ok(Math.abs(dec[i][1] - pontos[i][1]) < 1e-6, 'lng ' + i);
	}
});

test('primeiro ponto absoluto, demais em delta', function () {
	var enc = poly.encodePolyline([[-23.552, -46.632], [-23.553, -46.633]]);
	assert.equal(enc, '-23552000,-46632000;-1000,-1000');
});

test('sobrevive ao sanitizador do writer — nada de `@` no alfabeto', function () {
	// A razão de não usar o polyline codificado do Google: o alfabeto dele inclui
	// `@` (ASCII 64), que o writer troca por espaço em silêncio.
	var enc = poly.encodePolyline([
		[-23.552, -46.632], [-23.553, -46.633], [0, 0], [12.3456789, 98.7654321]
	]);
	assert.equal(txtAt.sanitizeField(enc), enc);
	assert.ok(!/[@\r\n]/.test(enc));
	assert.ok(/^[-0-9,;]+$/.test(enc), 'só dígito, sinal, vírgula e ponto e vírgula');
});

test('ponto repetido CONSECUTIVO some; repetido no fim (anel) fica', function () {
	var reto = poly.encodePolyline([[-23.55, -46.63], [-23.55, -46.63], [-23.56, -46.64]]);
	assert.equal(poly.countPolyline(reto), 2, 'duplicata consecutiva descartada');

	// Way fechada (praça, parque): o primeiro ponto volta no fim e o anel fecha.
	var anel = poly.encodePolyline([
		[-23.55, -46.63], [-23.55, -46.64], [-23.56, -46.64], [-23.55, -46.63]
	]);
	var dec = poly.decodePolyline(anel);
	assert.equal(dec.length, 4);
	assert.deepEqual(dec[0], dec[3]);
});

test('lista vazia e um ponto só não viram linha', function () {
	assert.equal(poly.encodePolyline([]), '');
	assert.equal(poly.encodePolyline(null), '');
	var um = poly.encodePolyline([[-23.55, -46.63]]);
	assert.equal(poly.countPolyline(um), 1);
	assert.ok(um.indexOf(poly.SEP_PONTO) < 0, 'sem separador = não é linha');
});

test('countPolyline não precisa decodificar', function () {
	var enc = poly.encodePolyline([[-1, -1], [-1.001, -1.001], [-1.002, -1.002]]);
	assert.equal(poly.countPolyline(enc), 3);
	assert.equal(poly.countPolyline(''), 0);
	assert.equal(poly.countPolyline(null), 0);
});

test('entrada malformada lança em vez de devolver geometria pela metade', function () {
	// Meia via desenhada é pior que via nenhuma: o erro tem de aparecer.
	assert.throws(function () { poly.decodePolyline('1,2;3'); }, /malformada/);
	assert.throws(function () { poly.decodePolyline('1,2;a,3'); }, /não numérica/);
	assert.throws(function () { poly.decodePolyline('1,2;,3'); }, /não numérica/);
	assert.deepEqual(poly.decodePolyline(''), []);
	assert.deepEqual(poly.decodePolyline(null), []);
});

test('erro de arredondamento fica abaixo de 0,11 m', function () {
	// 1e-6 grau ≈ 0,11 m. O limite existe porque 1e-5 (≈1,1 m) daria escadinha
	// visível no zoom 18 do mapa.
	var p = [[-23.5512345678, -46.6323456789]];
	var dec = poly.decodePolyline(poly.encodePolyline(p));
	assert.ok(Math.abs(dec[0][0] - p[0][0]) <= 5e-7);
	assert.ok(Math.abs(dec[0][1] - p[0][1]) <= 5e-7);
});
