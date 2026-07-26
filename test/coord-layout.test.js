'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var fs = require('fs');
var os = require('os');
var path = require('path');
var { execFileSync } = require('child_process');
var CL = require('../coord-layout');
var { runProcess } = require('../index0');

function makeBz2(tmp, name, xml) {
	var osm = path.join(tmp, name + '.osm');
	var bz2 = path.join(tmp, name + '.osm.bz2');
	fs.writeFileSync(osm, xml, 'utf8');
	execFileSync(
		'python',
		[
			'-c',
			'import bz2,sys; open(sys.argv[2],"wb").write(bz2.compress(open(sys.argv[1],"rb").read()))',
			osm,
			bz2
		],
		{ stdio: 'pipe' }
	);
	return bz2;
}

test('jumpDeg is hypot of degree deltas with lon wrap', function () {
	assert.ok(Math.abs(CL.jumpDeg(0, 0, 0, 3) - 3) < 1e-9);
	assert.ok(Math.abs(CL.jumpDeg(0, 179, 0, -179) - 2) < 1e-6);
	assert.ok(CL.jumpDeg(0, 0, 3, 4) - 5 < 1e-9);
});

test('sequential nodes yield high pctSmallJumps', function () {
	var layout = CL.createCoordLayout({ jumpSmallDeg: 0.05, sampleSize: 64 });
	var pos = { bzFile: 0, bzBlock: 0, fileOffset: 0, byteOffset: 0, chunkPos: [0, 0, 0] };
	for (var i = 0; i < 50; i++) {
		CL.noteNodeCoord(
			layout,
			{ lat: -23.5 + i * 0.001, lon: -46.6 + i * 0.001, id: String(i) },
			pos
		);
	}
	CL.finalizeBzBlockLayout(layout, {
		bzFile: 0,
		bzBlock: 0,
		fileOffset: 0,
		byteOffsetEnd: 100,
		chunkPosIn: 10,
		chunkPosOut: 1000
	});
	var snap = CL.snapshotCoordLayout(layout);
	assert.ok(snap.sequence.jumpCount >= 49);
	assert.ok(
		snap.sequence.pctSmallJumps > 0.9,
		'seq ' + snap.sequence.pctSmallJumps
	);
	assert.ok(snap.sequence.meanJumpDeg < 0.01);
	assert.equal(snap.blocks.length, 1);
	assert.equal(snap.blocks[0].nodes, 50);
	assert.ok(snap.samples.length > 0);
	assert.ok(/lay~seq|lay seq=/.test(CL.formatCoordLayout(layout)));
});

test('scattered nodes yield low pctSmallJumps', function () {
	var layout = CL.createCoordLayout({ jumpSmallDeg: 0.05, sampleSize: 64 });
	var pts = [
		[0, 0],
		[40, 100],
		[-30, -70],
		[50, -120],
		[-10, 150],
		[20, -40]
	];
	pts.forEach(function (p, i) {
		CL.noteNodeCoord(
			layout,
			{ lat: p[0], lon: p[1], id: String(i) },
			{ bzFile: 0, bzBlock: 0, chunkPos: [i, i, i] }
		);
	});
	var seq = CL.sequenceSnapshot(layout);
	assert.ok(seq.meanJumpDeg > 10, 'mean ' + seq.meanJumpDeg);
	assert.ok(seq.pctSmallJumps < 0.2, 'pct ' + seq.pctSmallJumps);
});

test('addr-tagged nodes and addressMapping flag', function () {
	var layout = CL.createCoordLayout();
	var g = { addrAny: 1, nodeWithLatLon: 1, name: 0, place: 0 };
	CL.maybeEnableAddressMapping(layout, g, { bzFile: 1, bzBlock: 2, chunkPos: [9, 9, 9] });
	assert.equal(layout.addressMappingEnabled, true);
	assert.equal(layout.addressMappingEnabledAt.bzFile, 1);

	CL.noteNodeCoord(
		layout,
		{ lat: 1, lon: 2, id: '9', hasAddr: true },
		{ bzFile: 1, bzBlock: 2, chunkPos: [9, 9, 9] }
	);
	assert.ok(layout.addrSamples.length >= 1);
	assert.equal(layout.addrSamples[0].kind, 'addr');
});

test('runProcess sequential fixture: layout in results', async function () {
	var lines = ['<?xml version="1.0"?>', '<osm version="0.6">'];
	for (var i = 0; i < 30; i++) {
		var lat = (-23.5 + i * 0.002).toFixed(6);
		var lon = (-46.6 + i * 0.002).toFixed(6);
		lines.push(
			'  <node id="' +
				(i + 1) +
				'" lat="' +
				lat +
				'" lon="' +
				lon +
				'">' +
				'<tag k="addr:street" v="Rua ' +
				i +
				'"/>' +
				'<tag k="name" v="N' +
				i +
				'"/>' +
				'</node>'
		);
	}
	lines.push('</osm>');
	var tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'osm-lay-'));
	var bz2 = makeBz2(tmp, 'seq', lines.join('\n'));
	var result = await runProcess({
		inputPath: bz2,
		statsPath: path.join(tmp, 's.json'),
		resultsPath: path.join(tmp, 'r.json'),
		quiet: true,
		resume: false,
		saveStats: true,
		checkpointEveryFiles: null,
		syncSchedule: true
	});

	assert.ok(result.coordLayout);
	assert.ok(result.coordLayout.sequence.jumpCount >= 20);
	assert.ok(
		result.coordLayout.sequence.pctSmallJumps > 0.8,
		JSON.stringify(result.coordLayout.sequence)
	);
	assert.ok(result.coordLayout.addressMappingEnabled);
	assert.ok(result.geocodeSignals.hints.likelyHasGeocodeMaterial);

	var stats = JSON.parse(fs.readFileSync(path.join(tmp, 's.json'), 'utf8'));
	assert.ok(stats.coordLayout);
	assert.ok(stats.coordLayout.blocks.length >= 1 || stats.coordLayout.samples.length > 0);
});

test('runProcess shuffled fixture: lower sequential score', async function () {
	var coords = [];
	for (var i = 0; i < 20; i++) {
		coords.push([i * 5 - 40, i * 8 - 80]); // large steps
	}
	// reverse order of second half to mess jumps further
	var order = [0, 10, 1, 11, 2, 12, 3, 13, 4, 14, 5, 15, 6, 16, 7, 17, 8, 18, 9, 19];
	var lines = ['<?xml version="1.0"?>', '<osm version="0.6">'];
	order.forEach(function (i, n) {
		lines.push(
			'<node id="' +
				n +
				'" lat="' +
				coords[i][0] +
				'" lon="' +
				coords[i][1] +
				'"/>'
		);
	});
	lines.push('</osm>');
	var tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'osm-rand-'));
	var bz2 = makeBz2(tmp, 'rand', lines.join('\n'));
	var result = await runProcess({
		inputPath: bz2,
		statsPath: path.join(tmp, 's.json'),
		resultsPath: path.join(tmp, 'r.json'),
		quiet: true,
		resume: false,
		saveStats: false,
		checkpointEveryFiles: null,
		syncSchedule: true
	});
	assert.ok(result.coordLayout.sequence.meanJumpDeg > 1);
	assert.ok(result.coordLayout.sequence.pctSmallJumps < 0.3);
});
