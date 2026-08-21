'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var fs = require('fs');
var path = require('path');
var mun = require('../mun-poly');

var DATA = require('../mun-poly.json');

var RIO = '3304557';
var NITEROI = '3303302';
var MACAE = '3302403';

test.beforeEach(function () { mun.usarArquivo(null); });

test('mun-poly: malha carregada, com fonte e recorte declarados', function () {
	var m = mun.meta();
	assert.ok(mun.disponivel());
	assert.ok(String(m.fonte).indexOf('ibge.gov.br') >= 0, 'fonte citada');
	assert.ok(String(m.fonte).indexOf('intrarregiao=municipio') >= 0, 'malha municipal');
	assert.ok(m.municipios >= 600, 'municípios: ' + m.municipios);
	assert.ok(String(m.simplificacao).indexOf('Douglas-Peucker') === 0);
	assert.ok(/^\d{4}-\d{2}-\d{2}$/.test(m.baixado_em), 'data da baixa: ' + m.baixado_em);
	Object.keys(DATA.municipios).forEach(function (cod) {
		assert.ok(/^\d{7}$/.test(cod), 'codarea de 7 dígitos: ' + cod);
	});
});

test('mun-poly: ponto dentro e ponto fora', function () {
	// Cinelândia, centro do Rio
	assert.equal(mun.dentroDoMunicipio(RIO, -22.9099, -43.1759), true);
	// o mesmo ponto não é de Niterói, do outro lado da baía
	assert.equal(mun.dentroDoMunicipio(NITEROI, -22.9099, -43.1759), false);
	// centro de Niterói é de Niterói, e não do Rio
	assert.equal(mun.dentroDoMunicipio(NITEROI, -22.8930, -43.1215), true);
	assert.equal(mun.dentroDoMunicipio(RIO, -22.8930, -43.1215), false);
});

test('mun-poly: ponto exatamente sobre a divisa é dentro dos dois municípios', function () {
	// mesma convenção do uf-poly.js: a divisa é dos dois que a compartilham.
	// O vértice é achado no dado, não fixado — a malha muda de versão em versão.
	var vizinhos = {};
	DATA.municipios[RIO].forEach(function (r) {
		for (var i = 0; i < r.length; i += 2) vizinhos[r[i] + ',' + r[i + 1]] = true;
	});
	var achado = null, outro = null;
	Object.keys(DATA.municipios).forEach(function (cod) {
		if (achado || cod === RIO) return;
		DATA.municipios[cod].forEach(function (r) {
			for (var i = 0; !achado && i < r.length; i += 2) {
				if (vizinhos[r[i] + ',' + r[i + 1]]) {
					achado = [r[i + 1], r[i]];
					outro = cod;
				}
			}
		});
	});
	assert.ok(achado, 'o Rio compartilha vértice com algum vizinho na malha');
	assert.equal(mun.dentroDoMunicipio(RIO, achado[0], achado[1]), true);
	assert.equal(mun.dentroDoMunicipio(outro, achado[0], achado[1]), true);
	// e a distância da borda ali é zero
	assert.ok(mun.distanciaDaBordaKm(RIO, achado[0], achado[1]) < 0.001);
});

test('mun-poly: ponto sobre o segmento (não sobre o vértice) também é dentro', function () {
	var r = DATA.municipios[RIO][0];
	var meioX = (r[0] + r[2]) / 2, meioY = (r[1] + r[3]) / 2;
	assert.equal(mun.dentroDoMunicipio(RIO, meioY, meioX), true);
});

test('mun-poly: buraco no MultiPolygon fica de fora (paridade par-ímpar)', function () {
	// anel externo 0..10, buraco 4..6 — sem saber quem é buraco de quem
	var externo = [0, 0, 10, 0, 10, 10, 0, 10, 0, 0];
	var buraco = [4, 4, 6, 4, 6, 6, 4, 6, 4, 4];
	var shape = mun.montarShape([externo, buraco]);
	assert.equal(mun.contemPonto(shape, 2, 2), true, 'na coroa, dentro');
	assert.equal(mun.contemPonto(shape, 5, 5), false, 'no buraco, fora');
	assert.equal(mun.contemPonto(shape, 12, 5), false, 'fora do externo');
	assert.equal(mun.contemPonto(shape, 0, 5), true, 'sobre a borda externa, dentro');
	assert.equal(mun.contemPonto(shape, 4, 5), true, 'sobre a borda do buraco, dentro');
});

test('mun-poly: distanciaKm mede até o segmento, não até o vértice', function () {
	var quadrado = [0, 0, 1, 0, 1, 1, 0, 1, 0, 0];
	var shape = mun.montarShape([quadrado]);
	// 1° de latitude ≈ 111,19 km; o ponto está 0,5° ao sul do lado y=0
	var d = mun.distanciaKm(shape, 0.5, -0.5);
	assert.ok(Math.abs(d - 55.6) < 0.5, 'meio grau ao sul do lado: ' + d);
	// e dentro a distância é até a borda mais próxima, não zero
	var dentro = mun.distanciaKm(shape, 0.5, 0.1);
	assert.ok(Math.abs(dentro - 11.1) < 0.5, 'dentro, 0,1° da borda: ' + dentro);
});

test('mun-poly: "não sei" é null, e nunca false', function () {
	// município fora do recorte (só entram os que têm logradouro no DNE)
	assert.equal(mun.dentroDoMunicipio('9999999', -22.9, -43.2), null);
	assert.equal(mun.distanciaDaBordaKm('9999999', -22.9, -43.2), null);
	// coordenada que não existe
	assert.equal(mun.dentroDoMunicipio(RIO, null, null), null);
	assert.equal(mun.dentroDoMunicipio(RIO, NaN, -43.2), null);
	assert.equal(mun.distanciaDaBordaKm(RIO, undefined, undefined), null);
});

test('mun-poly: malha ausente degrada, não quebra', function () {
	assert.equal(mun.usarArquivo(path.join(__dirname, 'nao-existe-mun-poly.json')), false);
	assert.equal(mun.disponivel(), false);
	assert.equal(mun.meta(), null);
	assert.equal(mun.dentroDoMunicipio(RIO, -22.9099, -43.1759), null);
	assert.equal(mun.distanciaDaBordaKm(RIO, -22.9099, -43.1759), null);
	// e volta ao normal quando a malha reaparece
	assert.equal(mun.usarArquivo(null), true);
	assert.equal(mun.dentroDoMunicipio(RIO, -22.9099, -43.1759), true);
});

// Teto grosseiro: o join pergunta uma vez por linha `ok` (70.797 em RJ). Não é
// benchmark, é rede de segurança — o polígono do município já vem dado, então
// isto só cai se a consulta virar varredura de todos os 636.
test('mun-poly: 200 mil consultas sob o teto', function () {
	var t0 = Date.now();
	var dentro = 0;
	for (var i = 0; i < 200000; i++) {
		if (mun.dentroDoMunicipio(RIO, -22.9 + (i % 100) * 0.001, -43.2 + (i % 97) * 0.001)) dentro++;
	}
	var ms = Date.now() - t0;
	assert.ok(dentro > 0);
	assert.ok(ms < 3000, '200000 consultas em ' + ms + ' ms (teto 3000)');
});

// ------------------------------------------------ aceitação sobre o join de RJ

var RJ_JOIN = 'G:\\dne-geo-rj-join3\\DNE_GEO_LOGRADOURO_RJ.TXT';

test('mun-poly: o resíduo de RJ bate com a medição de 21/08/2026', function (t) {
	if (!fs.existsSync(RJ_JOIN)) {
		return t.skip(RJ_JOIN + ' ausente — é o join de RJ com --ancora-raio-km=60');
	}
	var faixas = { ate_1km: 0, de_1_a_5km: 0, de_5_a_25km: 0, mais_de_25km: 0 };
	var ok = 0, fora = 0, semPoligono = 0, colinas = null;
	fs.readFileSync(RJ_JOIN, 'utf8').split(/\r?\n/).forEach(function (l) {
		if (!l) return;
		var p = l.split('@');
		if (p[20] !== 'ok') return; // coluna 21, 1-based
		ok++;
		var ibge = p[13], lat = Number(p[14]), lng = Number(p[15]);
		var dentro = mun.dentroDoMunicipio(ibge, lat, lng);
		if (dentro === null) { semPoligono++; return; }
		if (dentro) return;
		fora++;
		var km = mun.distanciaDaBordaKm(ibge, lat, lng);
		faixas[km < 1 ? 'ate_1km' : km < 5 ? 'de_1_a_5km' : km <= 25 ? 'de_5_a_25km' : 'mais_de_25km']++;
		if (p[7] === '27960271') colinas = { cidade: p[11], km: km, faixa: km > 25 };
	});

	assert.equal(ok, 70797, 'linhas ok no join de referência');
	assert.equal(semPoligono, 0, 'todo município de RJ com logradouro está na malha');
	// referência medida sobre a malha crua: 6.837. A simplificação (eps=0,003°)
	// engorda só a faixa de ruído; as duas faixas que decidem não se mexem.
	assert.ok(Math.abs(fora - 6837) < 200, 'fora do município: ' + fora + ' (referência 6837)');
	assert.ok(Math.abs(faixas.de_5_a_25km - 3030) < 60, '5–25 km: ' + faixas.de_5_a_25km);
	assert.ok(Math.abs(faixas.mais_de_25km - 1085) < 60, '>25 km: ' + faixas.mais_de_25km);
	assert.ok(Math.abs(faixas.de_1_a_5km - 1539) < 120, '1–5 km: ' + faixas.de_1_a_5km);
	assert.ok(faixas.ate_1km > 1000 && faixas.ate_1km < 1500, '<1 km: ' + faixas.ate_1km);

	// o caso que fez a tarefa existir
	assert.ok(colinas, 'Rua das Colinas / CEP 27960271 está no arquivo e saiu fora');
	assert.equal(colinas.cidade, 'Macaé');
	assert.ok(Math.abs(colinas.km - 45) < 2, 'a ~45 km da divisa de Macaé: ' + colinas.km);
	assert.ok(colinas.faixa, 'na faixa >25 km, a que não tem desculpa');
});

test('mun-poly: 3302403 (Macaé) não contém a coordenada gravada da Rua das Colinas', function () {
	assert.equal(mun.dentroDoMunicipio(MACAE, -22.8399636, -42.0608335), false);
	var km = mun.distanciaDaBordaKm(MACAE, -22.8399636, -42.0608335);
	assert.ok(Math.abs(km - 45) < 2, 'km fora: ' + km);
});
