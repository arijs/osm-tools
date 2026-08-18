'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var uf = require('../uf-br');

test('ufFromIbge por prefixo', function () {
	assert.equal(uf.ufFromIbge('3550308'), 'SP');
	assert.equal(uf.ufFromIbge('3304557'), 'RJ');
	assert.equal(uf.ufFromIbge('3106200'), 'MG');
	assert.equal(uf.ufFromIbge('3205309'), 'ES');
	assert.equal(uf.ufFromIbge('1302603'), 'AM');
	assert.equal(uf.ufFromIbge('4314902'), 'RS');
});

test('ufFromTags ISO e addr:state', function () {
	assert.equal(uf.ufFromTags({ 'ISO3166-2': 'BR-SP' }), 'SP');
	assert.equal(uf.ufFromTags({ 'addr:state': 'RJ' }), 'RJ');
	assert.equal(uf.ufFromTags({ place: 'state', name: 'Minas Gerais' }), 'MG');
});

test('ufFromPoint Sudeste e outras regiões', function () {
	assert.equal(uf.ufFromPoint(-23.55, -46.63), 'SP');
	assert.equal(uf.ufFromPoint(-22.9, -43.2), 'RJ');
	assert.equal(uf.ufFromPoint(-30.03, -51.23), 'RS');
	assert.equal(uf.ufFromPoint(-3.12, -60.02), 'AM');
});

test('parseUfFilter região e UF', function () {
	var se = uf.parseUfFilter('', 'sudeste');
	assert.ok(se.SP && se.RJ && se.MG && se.ES);
	assert.equal(se.PR, undefined);

	var sul = uf.parseUfFilter('', 'sul');
	assert.ok(sul.PR && sul.SC && sul.RS);
	assert.equal(sul.SP, undefined);

	var co = uf.parseUfFilter('', 'centro-oeste');
	assert.ok(co.DF && co.GO && co.MT && co.MS);

	var mixed = uf.parseUfFilter('SP,ES', 'norte');
	assert.ok(mixed.SP && mixed.ES && mixed.AM && mixed.PA);
});

test('passesUfFilter e tagsDisallowedByFilter', function () {
	var onlySp = uf.parseUfFilter('SP', '');
	assert.equal(uf.passesUfFilter(onlySp, 'SP', -23.5, -46.6), true);
	assert.equal(uf.passesUfFilter(onlySp, 'RJ', -22.9, -43.2), false);
	// ponto em SP mesmo com UF XX
	assert.equal(uf.passesUfFilter(onlySp, 'XX', -23.55, -46.63), true);
	assert.equal(uf.tagsDisallowedByFilter(onlySp, { 'addr:state': 'RJ' }, ''), true);
	assert.equal(uf.tagsDisallowedByFilter(onlySp, { 'addr:state': 'SP' }, ''), false);
	assert.equal(uf.tagsDisallowedByFilter(null, { 'addr:state': 'RJ' }, ''), false);
});

// Regressão do rótulo que discordava do filtro (DDSOFT, 18/08/2026): rodando
// `--uf=MG`, uma via em Patrocínio caía na caixa de MG *e* na de GO, e a de GO —
// menor — vencia o desempate. A via era MANTIDA no run (passesUfFilter aceita
// ponto em caixa permitida) e escrita em OSM_LOGRADOURO_GEOM_GO, onde o
// consumidor de MG nunca ia procurar: 29.505 ways de MG sem traçado.
test('resolveUfFiltered — o filtro do run nomeia o que só a geometria resolve', function () {
	var mg = uf.parseUfFilter('MG', '');
	var patrocinio = { lat: -18.9375, lng: -46.9856 };
	var uberlandia = { lat: -18.9186, lng: -48.2772 };
	var montesClaros = { lat: -16.735, lng: -43.8617 };

	// o defeito, preservado como documentação do porquê
	assert.equal(uf.resolveUf(patrocinio), 'GO');
	assert.equal(uf.resolveUf(uberlandia), 'GO');
	assert.equal(uf.resolveUf(montesClaros), 'BA');

	// e a correção
	assert.equal(uf.resolveUfFiltered(patrocinio, mg), 'MG');
	assert.equal(uf.resolveUfFiltered(uberlandia, mg), 'MG');
	assert.equal(uf.resolveUfFiltered(montesClaros, mg), 'MG');
});

test('resolveUfFiltered — tag e IBGE continuam mandando', function () {
	var mg = uf.parseUfFilter('MG', '');
	var patrocinio = { lat: -18.9375, lng: -46.9856 };

	// dado explícito da feature vence a geometria; quem descarta é o filtro,
	// não o rótulo — senão o run de MG renomearia Goiás inteiro.
	assert.equal(
		uf.resolveUfFiltered({ tags: { 'addr:state': 'GO' }, lat: patrocinio.lat, lng: patrocinio.lng }, mg),
		'GO'
	);
	assert.equal(
		uf.resolveUfFiltered({ ibge: '5208707', lat: patrocinio.lat, lng: patrocinio.lng }, mg),
		'GO'
	);
});

test('resolveUfFiltered — sem filtro, e fora das caixas permitidas', function () {
	var mg = uf.parseUfFilter('MG', '');
	var patrocinio = { lat: -18.9375, lng: -46.9856 };
	var manaus = { lat: -3.12, lng: -60.02 };

	// sem filtro é o comportamento de sempre
	assert.equal(uf.resolveUfFiltered(patrocinio, null), 'GO');
	// ponto fora de qualquer caixa permitida cai no comportamento antigo —
	// o descarte é do passesUfFilter, e mentir o rótulo não ajudaria ninguém
	assert.equal(uf.resolveUfFiltered(manaus, mg), 'AM');
	assert.equal(uf.passesUfFilter(mg, 'AM', manaus.lat, manaus.lng), false);
});

test('resolveUfFiltered — região não achata vizinho legítimo', function () {
	var se = uf.parseUfFilter('', 'sudeste');
	// Franca e Rio Claro estão dentro do retângulo de MG; com SP no conjunto
	// permitido, o desempate por área continua acertando.
	assert.equal(uf.resolveUfFiltered({ lat: -20.5386, lng: -47.4008 }, se), 'SP');
	assert.equal(uf.resolveUfFiltered({ lat: -22.4149, lng: -47.5651 }, se), 'SP');
	assert.equal(uf.resolveUfFiltered({ lat: -18.9375, lng: -46.9856 }, se), 'MG');
});

test('extractIbge', function () {
	assert.deepEqual(uf.extractIbge({ 'IBGE:GEOCODIGO': '3550308' }), {
		ibge: '3550308',
		source_tag: 'IBGE:GEOCODIGO'
	});
	assert.deepEqual(uf.extractIbge({ 'ref:IBGE': '3304557' }), {
		ibge: '3304557',
		source_tag: 'ref:IBGE'
	});
});
