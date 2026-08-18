'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var ow = require('../osm-oneway');

test('onewayCode: ausente / frente / reverso / duplo', function () {
	assert.equal(ow.onewayCode(null), 0);
	assert.equal(ow.onewayCode({}), 0);
	assert.equal(ow.onewayCode({ oneway: '' }), 0);
	assert.equal(ow.onewayCode({ oneway: 'yes' }), 1);
	assert.equal(ow.onewayCode({ oneway: 'True' }), 1);
	assert.equal(ow.onewayCode({ oneway: '1' }), 1);
	assert.equal(ow.onewayCode({ oneway: '-1' }), 2);
	assert.equal(ow.onewayCode({ oneway: 'reverse' }), 2);
	assert.equal(ow.onewayCode({ oneway: 'no' }), 3);
	assert.equal(ow.onewayCode({ oneway: 'false' }), 3);
	assert.equal(ow.onewayCode({ oneway: '0' }), 3);
	assert.equal(ow.onewayCode({ oneway: 'alternating' }), 0);
});

test('splitGeomPayload: legado 2 cols e novo 3 cols', function () {
	var leg = ow.splitGeomPayload('-23552000,-46632000;-1000,-1000');
	assert.equal(leg.oneway, 0);
	assert.ok(leg.polyline.indexOf(';') > 0);
	var neu = ow.splitGeomPayload('-23552000,-46632000;-1000,-1000@1');
	assert.equal(neu.oneway, 1);
	assert.equal(neu.polyline, '-23552000,-46632000;-1000,-1000');
	assert.equal(ow.splitGeomPayload('x@2').oneway, 2);
	assert.equal(ow.splitGeomPayload('x@9').oneway, 0);
});
