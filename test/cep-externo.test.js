'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var fs = require('fs');
var os = require('os');
var path = require('path');
var cepExt = require('../cep-externo');

test('digitsCep normaliza 8 dígitos', function () {
	assert.equal(cepExt.digitsCep('01310-100'), '01310100');
	assert.equal(cepExt.digitsCep('1310100'), '01310100');
	assert.equal(cepExt.digitsCep(''), '');
	assert.equal(cepExt.digitsCep('abc'), '');
});

test('formatRow/parseRow round-trip', function () {
	var line = cepExt.formatRow({
		cep: '01001000',
		http_status: 200,
		status: 'ok',
		lat: -23.5502784,
		lng: -46.6342179,
		api_city: 'São Paulo',
		api_state: 'SP',
		api_district: 'Sé',
		api_ibge: '3550308',
		api_address: 'Praça da Sé',
		api_address_type: 'Praça',
		consultado_em: '2026-07-30T22:00:00.000Z',
		fonte: 'awesomeapi',
	});
	assert.equal(line.split('@').length, 13);
	assert.ok(!line.includes('\n'));
	var row = cepExt.parseRow(line);
	assert.equal(row.cep, '01001000');
	assert.equal(row.status, 'ok');
	assert.equal(row.http_status, 200);
	assert.ok(Math.abs(row.lat - (-23.5502784)) < 1e-6);
	assert.equal(row.api_city, 'São Paulo');
	assert.equal(row.api_ibge, '3550308');
});

test('formatRow sanitiza @ no endereço', function () {
	var line = cepExt.formatRow({
		cep: '01001000',
		http_status: 200,
		status: 'ok',
		lat: -23.5,
		lng: -46.6,
		api_address: 'Rua A@B',
		consultado_em: '2026-07-30T22:00:00.000Z',
	});
	assert.equal(line.split('@').length, 13);
	assert.ok(line.includes('Rua A B'));
});

test('fromAwesomeResponse classifica 200/404/empty', function () {
	var ok = cepExt.fromAwesomeResponse('01001000', 200, {
		city: 'São Paulo', state: 'SP', district: 'Sé', city_ibge: '3550308',
		address: 'Praça da Sé', address_type: 'Praça',
		lat: '-23.55', lng: '-46.63',
	});
	assert.equal(ok.status, 'ok');
	assert.equal(ok.api_city, 'São Paulo');

	var nf = cepExt.fromAwesomeResponse('00000000', 404, { message: 'nao encontrado' });
	assert.equal(nf.status, 'not_found');
	assert.equal(nf.lat, null);

	var empty = cepExt.fromAwesomeResponse('01001000', 200, {
		city: 'X', state: 'SP', lat: '', lng: '',
	});
	assert.equal(empty.status, 'empty_coords');
});

test('cache multi-UF: split + loadCacheMulti + mergeAndSaveByUf', async function () {
	var dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cep-multi-'));
	try {
		var mono = path.join(dir, 'CEP_EXTERNO.TXT');
		var map0 = new Map();
		cepExt.mergeAndSave(mono, map0, [
			cepExt.fromAwesomeResponse('01001000', 200, {
				city: 'São Paulo', state: 'SP', lat: '-23.55', lng: '-46.63', city_ibge: '3550308',
			}),
			cepExt.fromAwesomeResponse('20010070', 200, {
				city: 'Rio de Janeiro', state: 'RJ', lat: '-22.9', lng: '-43.2',
			}),
			cepExt.fromAwesomeResponse('00000000', 404, null),
		]);
		// 404 sem state → XX
		var split = await cepExt.splitCacheByUf(mono, dir, { removeSource: true });
		assert.equal(split.total, 3);
		assert.ok(split.byUf.SP >= 1);
		assert.ok(split.byUf.RJ >= 1);
		assert.ok(fs.existsSync(cepExt.cachePathForUf(dir, 'SP')));
		assert.ok(fs.existsSync(cepExt.cachePathForUf(dir, 'RJ')));
		assert.equal(fs.existsSync(mono), false);

		var loaded = await cepExt.loadCacheMulti(dir, { ufs: ['SP', 'RJ'] });
		assert.equal(loaded.size, 3); // SP+RJ+XX (XX sempre entra com ufs filter)
		assert.equal(loaded.get('01001000').api_state, 'SP');

		// append novo CEP em MG
		var r = cepExt.mergeAndSaveByUf(dir, loaded, [
			cepExt.fromAwesomeResponse('30130000', 200, {
				city: 'Belo Horizonte', state: 'MG', lat: '-19.9', lng: '-43.9',
			}),
		]);
		assert.ok(r.ufs.indexOf('MG') >= 0);
		assert.ok(fs.existsSync(cepExt.cachePathForUf(dir, 'MG')));
		var again = await cepExt.loadCacheMulti(dir);
		assert.equal(again.size, 4);
		assert.equal(again.has('01001000'), true);
	} finally {
		fs.rmSync(dir, { recursive: true, force: true });
	}
});

test('loadCache + mergeAndSave: não perde CEP e permite skip', async function () {
	var dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cep-ext-'));
	var file = path.join(dir, 'CEP_EXTERNO.TXT');
	try {
		var map = new Map();
		cepExt.mergeAndSave(file, map, [
			cepExt.fromAwesomeResponse('01001000', 200, {
				city: 'São Paulo', state: 'SP', lat: '-23.55', lng: '-46.63', city_ibge: '3550308',
			}),
			cepExt.fromAwesomeResponse('20010070', 200, {
				city: 'Rio de Janeiro', state: 'RJ', lat: '-14.235004', lng: '-51.92528',
			}),
		]);
		assert.ok(fs.existsSync(file));

		var loaded = await cepExt.loadCache(file);
		assert.equal(loaded.size, 2);
		assert.equal(loaded.get('01001000').status, 'ok');
		assert.equal(loaded.get('20010070').status, 'ok');

		// simula política de skip
		function shouldFetch(cep, force) {
			if (force) return true;
			return !loaded.has(cep);
		}
		assert.equal(shouldFetch('01001000', false), false);
		assert.equal(shouldFetch('01310100', false), true);
		assert.equal(shouldFetch('01001000', true), true);

		// merge de um terceiro
		cepExt.mergeAndSave(file, loaded, [
			cepExt.fromAwesomeResponse('01310100', 404, null),
		]);
		var again = await cepExt.loadCache(file);
		assert.equal(again.size, 3);
		assert.equal(again.get('01310100').status, 'not_found');
		// ordenado por CEP
		var text = fs.readFileSync(file, 'utf8').trim().split(/\n/);
		assert.equal(text.length, 3);
		assert.ok(text[0].startsWith('01001000@'));
		assert.ok(text[1].startsWith('01310100@'));
		assert.ok(text[2].startsWith('20010070@'));
	} finally {
		fs.rmSync(dir, { recursive: true, force: true });
	}
});
