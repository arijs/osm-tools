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
