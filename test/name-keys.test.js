'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var { coreName, phoneticKey, isAreaTlo, isAreaKind } = require('../name-keys');

test('coreName tira tipo de logradouro e conector da frente', function () {
	assert.equal(coreName('travessa da moenda'), 'moenda');
	assert.equal(coreName('rua goias'), 'goias');
	assert.equal(coreName('via de pedestre audi'), 'audi');
	assert.equal(coreName('avenida do contorno'), 'contorno');
	// nome que é só o tipo volta inteiro — não pode virar string vazia
	assert.equal(coreName('travessa'), 'travessa');
	assert.equal(coreName(''), '');
	// não come token do meio
	assert.equal(coreName('rua barao de piracicaba'), 'barao de piracicaba');
});

test('coreName casa DNE Travessa com OSM Rua', function () {
	assert.equal(coreName('travessa santo antonio do monte'), coreName('rua santo antonio do monte'));
});

test('phoneticKey colapsa as variações reais DNE x OSM', function () {
	var pares = [
		['luiz gushiken', 'luis gushiken'],
		['dario de souza', 'dario de sousa'],
		['ayrton senna', 'airton senna'],
		['affonso de azevedo evora', 'afonso de azevedo evora'],
		['ernest renam', 'ernest renan'],
		['bras cubas', 'braz cubas'],
		['gabriel garcia marquez', 'gabriel garcia marques'],
		['valdomiro maximo', 'waldomiro maximo'],
		['lajeado amarelo', 'lageado amarelo'],
		['xavier', 'chavier']
	];
	pares.forEach(function (p) {
		assert.equal(phoneticKey(p[0]), phoneticKey(p[1]), p[0] + ' != ' + p[1]);
	});
});

test('phoneticKey preserva lh/nh e não colapsa nomes distintos', function () {
	assert.equal(phoneticKey('filho'), 'filho');
	assert.equal(phoneticKey('junho'), 'junho');
	assert.notEqual(phoneticKey('silva'), phoneticKey('salva'));
	assert.notEqual(phoneticKey('rocha'), phoneticKey('roca'));
	assert.equal(phoneticKey(''), '');
});

test('guarda kind-aware: tipos de área dos dois lados', function () {
	assert.equal(isAreaTlo('praca'), true);
	assert.equal(isAreaTlo('largo'), true);
	assert.equal(isAreaTlo('parque'), true);
	assert.equal(isAreaTlo('rua'), false);
	assert.equal(isAreaTlo('avenida'), false);
	assert.equal(isAreaKind('square'), true);
	assert.equal(isAreaKind('park'), true);
	assert.equal(isAreaKind('residential'), false);
	assert.equal(isAreaKind('pedestrian'), false);
});
