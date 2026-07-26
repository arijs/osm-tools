'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var uf = require('../uf-br');

test('ufFromIbge por prefixo', function () {
	assert.equal(uf.ufFromIbge('3550308'), 'SP');
	assert.equal(uf.ufFromIbge('3304557'), 'RJ');
	assert.equal(uf.ufFromIbge('3106200'), 'MG');
	assert.equal(uf.ufFromIbge('3205309'), 'ES');
});

test('ufFromTags ISO e addr:state', function () {
	assert.equal(uf.ufFromTags({ 'ISO3166-2': 'BR-SP' }), 'SP');
	assert.equal(uf.ufFromTags({ 'addr:state': 'RJ' }), 'RJ');
	assert.equal(uf.ufFromTags({ place: 'state', name: 'Minas Gerais' }), 'MG');
});

test('ufFromPoint Sudeste', function () {
	assert.equal(uf.ufFromPoint(-23.55, -46.63), 'SP');
	assert.equal(uf.ufFromPoint(-22.9, -43.2), 'RJ');
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
