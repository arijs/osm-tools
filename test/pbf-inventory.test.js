'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var fs = require('fs');
var path = require('path');
var { execFileSync } = require('child_process');
var { runPbfProcess } = require('../index-pbf');
var pbfReader = require('../pbf-reader');

var fixtures = path.join(__dirname, 'fixtures');
var tinyPbf = path.join(fixtures, 'tiny.osm.pbf');

function ensureTinyPbf() {
	if (!fs.existsSync(tinyPbf)) {
		execFileSync(process.execPath, [path.join(__dirname, '..', 'scripts', 'write-tiny-pbf.js'), tinyPbf], {
			stdio: 'pipe'
		});
	}
}

test('tiny.osm.pbf has OSMHeader and OSMData blobs', function () {
	ensureTinyPbf();
	var types = [];
	pbfReader.forEachBlob(tinyPbf, function (b) {
		types.push(b.type);
	});
	assert.deepEqual(types, ['OSMHeader', 'OSMData']);
});

test('runPbfProcess inventoria tiny PBF com GEO e layout', async function () {
	ensureTinyPbf();
	var statsPath = path.join(fixtures, 'tiny-test-pbf-stats.json');
	var resultsPath = path.join(fixtures, 'tiny-test-pbf-results.json');
	try {
		fs.unlinkSync(statsPath);
	} catch (_) {}
	try {
		fs.unlinkSync(resultsPath);
	} catch (_) {}

	var result = await runPbfProcess({
		inputPath: tinyPbf,
		statsPath: statsPath,
		resultsPath: resultsPath,
		quiet: true,
		resume: false,
		saveStats: true
	});

	assert.equal(result.error, null);
	assert.equal(result.stoppedEarly, false);
	assert.ok(result.header);
	assert.ok(result.header.bbox);
	assert.ok(result.header.bbox.left < result.header.bbox.right);

	assert.equal(result.geocodeSignals.node, 3);
	assert.equal(result.geocodeSignals.nodeWithLatLon, 3);
	assert.equal(result.geocodeSignals.addrStreet, 1);
	assert.equal(result.geocodeSignals.way, 1);
	assert.equal(result.geocodeSignals.hints.likelyHasGeocodeMaterial, true);

	assert.ok(result.coordLayout);
	assert.ok(result.coordLayout.sequence.jumpCount >= 2);
	assert.ok(result.coordLayout.sequence.pctSmallJumps > 0.9);
	assert.ok(result.coordLayout.samples.length >= 1);

	var disk = JSON.parse(fs.readFileSync(resultsPath, 'utf8'));
	assert.ok(disk.geocodeSignals.hints.likelyHasGeocodeMaterial);
	assert.ok(disk.coordLayout);

	try {
		fs.unlinkSync(statsPath);
		fs.unlinkSync(resultsPath);
	} catch (_) {}
});

test('runPbfProcess resume continua do offset', async function () {
	ensureTinyPbf();
	var statsPath = path.join(fixtures, 'tiny-resume-pbf-stats.json');
	var resultsPath = path.join(fixtures, 'tiny-resume-pbf-results.json');
	try {
		fs.unlinkSync(statsPath);
	} catch (_) {}

	// First run: process only header by soft-stop immediately after start is hard;
	// instead write a fake cursor after header blob
	var headerEnd = null;
	pbfReader.forEachBlob(tinyPbf, function (b) {
		if (b.type === 'OSMHeader') {
			headerEnd = b.nextOffset;
			return 'stop';
		}
	});
	assert.ok(headerEnd > 0);

	fs.writeFileSync(
		statsPath,
		JSON.stringify({
			cursor: { fileOffset: headerEnd, blobIndex: 1 },
			geocodeSignals: { node: 0, way: 0, nodeWithLatLon: 0, addrAny: 0 },
			runs: [],
			current: { time: 0, bytesRead: headerEnd }
		})
	);

	var result = await runPbfProcess({
		inputPath: tinyPbf,
		statsPath: statsPath,
		resultsPath: resultsPath,
		quiet: true,
		resume: true,
		saveStats: true
	});

	// Should process OSMData only from resume → still get 3 nodes
	assert.equal(result.geocodeSignals.node, 3);
	assert.equal(result.geocodeSignals.hints.likelyHasGeocodeMaterial, true);

	try {
		fs.unlinkSync(statsPath);
		fs.unlinkSync(resultsPath);
	} catch (_) {}
});
