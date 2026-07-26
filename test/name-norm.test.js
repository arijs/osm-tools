'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var { nameNorm } = require('../name-norm');

test('nameNorm remove acentos e normaliza', function () {
	assert.equal(nameNorm('São Paulo'), 'sao paulo');
	assert.equal(nameNorm('  Rua  Augusta  '), 'rua augusta');
	assert.equal(nameNorm('Avenida Paulista'), 'avenida paulista');
	assert.equal(nameNorm(''), '');
	assert.equal(nameNorm(null), '');
});
