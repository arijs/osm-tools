'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var CoordLayout = require('../coord-layout');

test('snapshotCoordLayout maxBlocks=0 omite blocks', function () {
	var layout = CoordLayout.createCoordLayout();
	for (var i = 0; i < 5; i++) {
		CoordLayout.noteNodeCoord(
			layout,
			{ lat: -23 - i * 0.001, lon: -46, id: i },
			{ bzFile: 0, bzBlock: i, fileOffset: i * 10, byteOffsetStart: 0, byteOffsetEnd: 1, chunkPosIn: 0, chunkPosOut: 1 }
		);
		CoordLayout.finalizeBzBlockLayout(layout, {
			bzFile: 0,
			bzBlock: i,
			fileOffset: i * 10,
			byteOffsetEnd: 1,
			chunkPosIn: 0,
			chunkPosOut: 1
		});
	}
	CoordLayout.flushCoordLayout(layout);
	assert.ok(layout.blocks.length >= 1);

	var full = CoordLayout.snapshotCoordLayout(layout, { maxBlocks: null });
	assert.ok(full.blocks.length >= 1);

	var slim = CoordLayout.snapshotCoordLayout(layout, { maxBlocks: 0 });
	assert.equal(slim.blocks.length, 0);
	assert.ok(slim.blocksTotal >= 1);
	assert.ok(slim.sequence.jumpCount >= 1);
});
