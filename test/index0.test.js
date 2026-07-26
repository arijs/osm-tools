'use strict';

/**
 * Tests for index0.js runProcess.
 *
 * Examples (single fixture / subset):
 *   node --test --test-name-pattern="index0 tiny" test/index0.test.js
 *   node --test --test-name-pattern="index0 large20" test/index0.test.js
 *   npm run test:index0
 *   npm run test:fixture -- index0 tiny
 */

var test = require('node:test');
var assert = require('node:assert/strict');
var fs = require('fs');
var path = require('path');
var os = require('os');
var { runProcess } = require('../index0');
var { fixturePath, fixturesDir } = require('./helpers');

var manifest = JSON.parse(
	fs.readFileSync(path.join(fixturesDir, 'manifest.json'), 'utf8')
);

function hasBz2(name) {
	return fs.existsSync(fixturePath(name + '.osm.bz2'));
}

function tempStatsPath(label) {
	return path.join(
		os.tmpdir(),
		'osm-tools-index0-' + label + '-' + process.pid + '-' + Date.now() + '.json'
	);
}

function cleanup(p) {
	try {
		fs.unlinkSync(p);
	} catch (_) {}
}

test('index0 exports runProcess', function () {
	assert.equal(typeof runProcess, 'function');
});

test('index0 rejects without inputPath', async function () {
	await assert.rejects(
		function () {
			return runProcess({});
		},
		/inputPath/
	);
});

test('index0 tiny: full process counts match manifest', async function () {
	var name = 'tiny';
	assert.ok(hasBz2(name), 'run npm run fixtures');
	var expected = manifest[name];
	var statsPath = tempStatsPath(name);
	cleanup(statsPath);

	var result = await runProcess({
		inputPath: fixturePath(name + '.osm.bz2'),
		statsPath: statsPath,
		quiet: true,
		resume: false,
		saveStats: true,
		checkpointEveryFiles: null,
		syncSchedule: true
	});

	assert.equal(result.error, null);
	assert.equal(result.stoppedEarly, false);
	assert.equal(result.xml.nodeCount, expected.nodes);
	assert.equal(result.xml.wayCount, expected.ways);
	assert.ok(result.bzStats.length >= 1, 'at least one bzip member');
	assert.ok(result.chunkPos[0] > 0, 'read compressed input');
	assert.ok(result.chunkPos[1] > 0, 'produced decompressed output');
	assert.ok(fs.existsSync(statsPath), 'stats file written');
	var bzPath = statsPath.replace(/\.json$/i, '') + '-bzip.json';
	assert.equal(
		fs.existsSync(bzPath),
		false,
		'stats-bzip sidecar must not be written without flag'
	);

	var saved = JSON.parse(fs.readFileSync(statsPath, 'utf8'));
	assert.ok(saved.xml && saved.xml.root);
	assert.equal(
		Object.prototype.hasOwnProperty.call(saved, 'bzip'),
		false,
		'bzip must not be embedded in stats'
	);
	assert.ok(saved.current);
	// in-memory result still has bzStats for callers
	assert.ok(result.bzStats && result.bzStats.length >= 1);
	cleanup(statsPath);
});

test('index0 small: full process', async function () {
	var name = 'small';
	assert.ok(hasBz2(name));
	var expected = manifest[name];
	var statsPath = tempStatsPath(name);
	cleanup(statsPath);

	var result = await runProcess({
		inputPath: fixturePath(name + '.osm.bz2'),
		statsPath: statsPath,
		quiet: true,
		resume: false,
		saveStats: false,
		checkpointEveryFiles: null,
		syncSchedule: true
	});

	assert.equal(result.xml.nodeCount, expected.nodes);
	assert.equal(result.xml.wayCount, expected.ways);
	assert.equal(result.stoppedEarly, false);
	cleanup(statsPath);
});

test('index0 medium: multi-block process', async function () {
	var name = 'medium';
	assert.ok(hasBz2(name));
	var expected = manifest[name];
	var statsPath = tempStatsPath(name);

	var result = await runProcess({
		inputPath: fixturePath(name + '.osm.bz2'),
		statsPath: statsPath,
		quiet: true,
		resume: false,
		saveStats: false,
		checkpointEveryFiles: null,
		syncSchedule: true
	});

	assert.equal(result.xml.nodeCount, expected.nodes);
	assert.equal(result.xml.wayCount, expected.ways);
	// medium is built with compresslevel 1 → multiple blocks in one member
	var blocks = 0;
	for (var i = 0; i < result.bzStats.length; i++) {
		blocks += result.bzStats[i].blocks.length;
	}
	assert.ok(blocks >= 2, 'medium should produce multiple bzip blocks, got ' + blocks);
	cleanup(statsPath);
});

test('index0 large20: optional full process', async function (t) {
	var name = 'large20';
	if (!hasBz2(name)) {
		t.skip('large20.osm.bz2 missing — npm run fixtures:large');
		return;
	}
	var expected = manifest[name];
	var statsPath = tempStatsPath(name);

	var result = await runProcess({
		inputPath: fixturePath(name + '.osm.bz2'),
		statsPath: statsPath,
		quiet: true,
		resume: false,
		saveStats: false,
		checkpointEveryFiles: null,
		syncSchedule: true
	});

	assert.equal(result.xml.nodeCount, expected.nodes);
	assert.equal(result.xml.wayCount, expected.ways);
	assert.ok(result.elapsedMs > 0);
	cleanup(statsPath);
});

test('index0 large200: optional full process (slow)', async function (t) {
	var name = 'large200';
	if (!hasBz2(name)) {
		t.skip('large200.osm.bz2 missing — npm run fixtures:large');
		return;
	}
	// Node test default timeout may be short; increase if runner supports it
	if (typeof t.setTimeout === 'function') t.setTimeout(30 * 60 * 1000);

	var expected = manifest[name];
	var statsPath = tempStatsPath(name);

	var result = await runProcess({
		inputPath: fixturePath(name + '.osm.bz2'),
		statsPath: statsPath,
		quiet: true,
		resume: false,
		saveStats: false,
		checkpointEveryFiles: null,
		syncSchedule: true
	});

	assert.equal(result.xml.nodeCount, expected.nodes);
	assert.equal(result.xml.wayCount, expected.ways);
	cleanup(statsPath);
});

test('index0 saveStats false does not write stats file', async function () {
	var statsPath = tempStatsPath('nostats');
	cleanup(statsPath);
	await runProcess({
		inputPath: fixturePath('tiny.osm.bz2'),
		statsPath: statsPath,
		quiet: true,
		resume: false,
		saveStats: false,
		checkpointEveryFiles: null,
		syncSchedule: true
	});
	assert.equal(fs.existsSync(statsPath), false);
});

test('index0 resume works with legacy stats missing bzCurrentFile', async function () {
	var statsPath = tempStatsPath('legacy');
	cleanup(statsPath);

	// First run to produce a valid stats skeleton, then strip new fields
	await runProcess({
		inputPath: fixturePath('tiny.osm.bz2'),
		statsPath: statsPath,
		quiet: true,
		resume: false,
		saveStats: true,
		checkpointEveryFiles: null,
		syncSchedule: true
	});

	var stats = JSON.parse(fs.readFileSync(statsPath, 'utf8'));
	// Mimic H:\osm\planet-latest.osm-stats.json (2018 schema)
	delete stats.current.bzCurrentFile;
	delete stats.current.bzNextFileOffset;
	// Pretend we stopped mid-way: rewind fopt/chunks to start so resume re-reads
	stats.fopt = {
		fileCount: 0,
		fileOffset: 0,
		byteOffset: 0,
		bytesInput: 0,
		bytesOutput: 0,
		bytesInputPos: 0,
		bytesOutputPos: 0,
		bitOffset: 0,
		bitOffsetEnd: 0,
		blockCount: 0,
		blockCRC: null,
		streamPartialCRC: '00000000',
		streamCRC: null,
		bzLevelBlock: 0,
		bzLevelFile: 0
	};
	stats.current.chunkCount = [0, 0, 0];
	stats.current.chunkPos = [0, 0, 0];
	stats.current.bzFile = 0;
	stats.current.bzBlock = 0;
	stats.bzip = [];
	stats.xml = {
		tags: [],
		index: [],
		current: 0,
		root: { tags: {} },
		first: [],
		last: [],
		remain: null
	};
	fs.writeFileSync(statsPath, JSON.stringify(stats));

	var expected = manifest.tiny;
	var result = await runProcess({
		inputPath: fixturePath('tiny.osm.bz2'),
		statsPath: statsPath,
		quiet: true,
		resume: true,
		saveStats: false,
		checkpointEveryFiles: null,
		syncSchedule: true
	});

	assert.equal(result.xml.nodeCount, expected.nodes);
	assert.equal(result.xml.wayCount, expected.ways);
	cleanup(statsPath);
});

test('index0 default discards bzip/bzStats on disk (no embed, no sidecar)', async function () {
	var statsPath = tempStatsPath('bz-default');
	var resultsPath = tempStatsPath('bz-default-results');
	var bzPath = statsPath.replace(/\.json$/i, '') + '-bzip.json';
	var resultsBzPath = resultsPath.replace(/\.json$/i, '') + '-bzip.json';
	cleanup(statsPath);
	cleanup(bzPath);
	cleanup(resultsPath);
	cleanup(resultsBzPath);

	var result = await runProcess({
		inputPath: fixturePath('tiny.osm.bz2'),
		statsPath: statsPath,
		resultsPath: resultsPath,
		quiet: true,
		resume: false,
		saveStats: true,
		saveBzStatsSeparate: false,
		checkpointEveryFiles: null,
		syncSchedule: true
	});

	assert.ok(fs.existsSync(statsPath));
	assert.ok(fs.existsSync(resultsPath));
	assert.equal(fs.existsSync(bzPath), false);
	assert.equal(fs.existsSync(resultsBzPath), false);

	var stats = JSON.parse(fs.readFileSync(statsPath, 'utf8'));
	assert.equal(Object.prototype.hasOwnProperty.call(stats, 'bzip'), false);
	assert.equal(stats.bzipPath, undefined);

	var results = JSON.parse(fs.readFileSync(resultsPath, 'utf8'));
	assert.equal(Object.prototype.hasOwnProperty.call(results, 'bzStats'), false);
	assert.equal(results.bzStatsPath, undefined);

	// still available in-memory on the returned result
	assert.ok(result.bzStats && result.bzStats.length >= 1);

	cleanup(statsPath);
	cleanup(resultsPath);
});

test('index0 --save-bz-stats writes sidecars and omits bzip from main JSON', async function () {
	var statsPath = tempStatsPath('bz-sep');
	var resultsPath = tempStatsPath('bz-sep-results');
	var bzPath = statsPath.replace(/\.json$/i, '') + '-bzip.json';
	var resultsBzPath = resultsPath.replace(/\.json$/i, '') + '-bzip.json';
	cleanup(statsPath);
	cleanup(bzPath);
	cleanup(resultsPath);
	cleanup(resultsBzPath);

	await runProcess({
		inputPath: fixturePath('tiny.osm.bz2'),
		statsPath: statsPath,
		resultsPath: resultsPath,
		bzStatsPath: bzPath,
		resultsBzStatsPath: resultsBzPath,
		quiet: true,
		resume: false,
		saveStats: true,
		saveBzStatsSeparate: true,
		checkpointEveryFiles: null,
		syncSchedule: true
	});

	assert.ok(fs.existsSync(bzPath), 'stats-bzip sidecar');
	assert.ok(fs.existsSync(resultsBzPath), 'results-bzip sidecar');

	var stats = JSON.parse(fs.readFileSync(statsPath, 'utf8'));
	assert.equal(Object.prototype.hasOwnProperty.call(stats, 'bzip'), false);
	assert.equal(stats.bzipPath, bzPath);

	var results = JSON.parse(fs.readFileSync(resultsPath, 'utf8'));
	assert.equal(Object.prototype.hasOwnProperty.call(results, 'bzStats'), false);
	assert.equal(results.bzStatsPath, resultsBzPath);

	var bz = JSON.parse(fs.readFileSync(bzPath, 'utf8'));
	assert.ok(Array.isArray(bz) && bz.length >= 1);

	// Resume with only sidecar + bzipPath
	stats.fopt = {
		fileCount: 0,
		fileOffset: 0,
		byteOffset: 0,
		bytesInput: 0,
		bytesOutput: 0,
		bytesInputPos: 0,
		bytesOutputPos: 0,
		bitOffset: 0,
		bitOffsetEnd: 0,
		blockCount: 0,
		blockCRC: null,
		streamPartialCRC: '00000000',
		streamCRC: null,
		bzLevelBlock: 0,
		bzLevelFile: 0
	};
	stats.current.chunkCount = [0, 0, 0];
	stats.current.chunkPos = [0, 0, 0];
	stats.current.bzFile = 0;
	stats.current.bzBlock = 0;
	stats.current.bzNextFileOffset = 0;
	stats.current.bzCurrentFile = null;
	stats.xml = {
		tags: [],
		index: [],
		current: 0,
		root: { tags: {} },
		first: [],
		last: [],
		remain: null
	};
	fs.writeFileSync(statsPath, JSON.stringify(stats));

	var result = await runProcess({
		inputPath: fixturePath('tiny.osm.bz2'),
		statsPath: statsPath,
		resultsPath: resultsPath,
		bzStatsPath: bzPath,
		resultsBzStatsPath: resultsBzPath,
		quiet: true,
		resume: true,
		saveStats: false,
		saveBzStatsSeparate: true,
		checkpointEveryFiles: null,
		syncSchedule: true
	});
	assert.equal(result.xml.nodeCount, manifest.tiny.nodes);
	assert.ok(result.bzStats.length >= 1);
	cleanup(statsPath);
	cleanup(bzPath);
	cleanup(resultsPath);
	cleanup(resultsBzPath);
});

test('index0 saveBzStatsPerMember writes one json per bzip member', async function () {
	var statsPath = tempStatsPath('bz-members');
	var bzPath = statsPath.replace(/\.json$/i, '') + '-bzip.json';
	var membersDir = bzPath.replace(/\.json$/i, '');
	cleanup(statsPath);
	cleanup(bzPath);
	try {
		fs.rmSync(membersDir, { recursive: true, force: true });
	} catch (_) {}

	await runProcess({
		inputPath: fixturePath('tiny.osm.bz2'),
		statsPath: statsPath,
		bzStatsPath: bzPath,
		quiet: true,
		resume: false,
		saveStats: true,
		saveBzStatsPerMember: true,
		checkpointEveryFiles: null,
		syncSchedule: true
	});

	assert.ok(fs.existsSync(bzPath));
	assert.ok(fs.existsSync(path.join(membersDir, '0000.json')));
	var member = JSON.parse(
		fs.readFileSync(path.join(membersDir, '0000.json'), 'utf8')
	);
	assert.ok(member.blocks);
	cleanup(statsPath);
	cleanup(bzPath);
	try {
		fs.rmSync(membersDir, { recursive: true, force: true });
	} catch (_) {}
});

test('index0 soft-stop finishes quickly via onControl', async function () {
	var name = hasBz2('medium') ? 'medium' : 'small';
	var statsPath = tempStatsPath('softstop');
	cleanup(statsPath);
	var control = null;
	var started = Date.now();

	var promise = runProcess({
		inputPath: fixturePath(name + '.osm.bz2'),
		statsPath: statsPath,
		quiet: true,
		resume: false,
		saveStats: false,
		checkpointEveryFiles: null,
		syncSchedule: true,
		softStopMaxMs: 5000,
		onControl: function (ctl) {
			control = ctl;
		}
	});

	// Request soft-stop almost immediately
	await new Promise(function (r) {
		setImmediate(function () {
			assert.ok(control, 'onControl should register immediately');
			control.softStop('test');
			r();
		});
	});

	var result = await promise;
	var elapsed = Date.now() - started;
	assert.ok(
		elapsed < 15000,
		'soft-stop should finish well under 15s, took ' + elapsed + 'ms'
	);
	// medium may stop early; tiny/small may finish entirely before soft-stop applies
	assert.ok(result.xml.nodeCount >= 0);
	cleanup(statsPath);
});
