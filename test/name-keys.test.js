'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var {
	coreName, stripTitulos, coreBare, phoneticKey, isAreaTlo, isAreaKind
} = require('../name-keys');

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

test('coreName tira qualificadores de TLO composto (Estrada Municipal)', function () {
	assert.equal(
		coreName('estrada municipal professora therezinha de lima belloto'),
		'professora therezinha de lima belloto'
	);
	assert.equal(
		coreName('estrada municipal marcelo eugenio tofanin'),
		'marcelo eugenio tofanin'
	);
	assert.equal(coreName('rodovia estadual joao monlevade'), 'joao monlevade');
	assert.equal(coreName('estrada vicinal do engordo'), 'engordo');
	// sem tipo antes, "municipal" não é stripado (não é TLO sozinho)
	assert.equal(coreName('municipal xyz'), 'municipal xyz');
	// casa DNE TLO composto com OSM que omite "Municipal"
	assert.equal(
		coreName('estrada municipal professora therezinha de lima belloto'),
		coreName('estrada professora therezinha de lima belloto')
	);
});

test('stripTitulos remove Doutor/Prof no início do núcleo', function () {
	var a = stripTitulos('doutor olimpio carr ribeiro');
	assert.equal(a.bare, 'olimpio carr ribeiro');
	assert.deepEqual(a.removed, ['doutor']);

	var b = stripTitulos('dr jose de alencar');
	assert.equal(b.bare, 'jose de alencar');
	assert.deepEqual(b.removed, ['dr']);

	var c = stripTitulos('professor luis de camoes');
	assert.equal(c.bare, 'luis de camoes');

	// sem título: bare idêntico, removed vazio
	var d = stripTitulos('olimpio carr ribeiro');
	assert.equal(d.bare, 'olimpio carr ribeiro');
	assert.deepEqual(d.removed, []);

	// só título não some
	assert.equal(stripTitulos('doutor').bare, 'doutor');
	assert.equal(stripTitulos('').bare, '');
});

test('coreBare casa DNE sem título com OSM com Doutor', function () {
	assert.equal(
		coreBare('rua olimpio carr ribeiro'),
		coreBare('rua doutor olimpio carr ribeiro')
	);
	assert.equal(
		coreBare('rua eng joao silva'),
		coreBare('rua engenheiro joao silva')
	);
	// título no meio do nome (sobrenome) não é stripado
	assert.equal(coreBare('rua mario doutor silva'), 'mario doutor silva');
	// nobreza não é stripada (identidade do logradouro)
	assert.equal(coreBare('rua barao de piracicaba'), 'barao de piracicaba');
	// TLO "Estrada Municipal" + Professora: tira tipo composto e o título
	assert.equal(
		coreBare('estrada municipal professora therezinha de lima belloto'),
		'therezinha de lima belloto'
	);
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
