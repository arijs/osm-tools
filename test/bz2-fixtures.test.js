'use strict';

/**
 * Fixture decode / parse tests.
 *
 * Run only one fixture by name pattern, e.g.:
 *   node --test --test-name-pattern="tiny" test/bz2-fixtures.test.js
 *   npm run test:fixture -- tiny
 *   npm run test:fixture -- large20
 *
 * Large fixtures (large20, large200) are gitignored. Generate with:
 *   npm run fixtures:large
 * Tests skip automatically when the .bz2 is missing.
 */

var test = require('node:test');
var assert = require('node:assert/strict');
var fs = require('fs');
var path = require('path');
var {
	fixturePath,
	fixturesDir,
	decodeBz2File,
	decodeBz2ByBlocks,
	parseOsmBz2,
	parseXmlString,
	countOpen
} = require('./helpers');

var manifestPath = path.join(fixturesDir, 'manifest.json');
var manifest = fs.existsSync(manifestPath)
	? JSON.parse(fs.readFileSync(manifestPath, 'utf8'))
	: {};

/** Committed fixtures — always expected to exist after `npm run fixtures`. */
var DEFAULT_FIXTURES = ['tiny', 'small', 'medium'];

/** Optional large fixtures (gitignored). */
var LARGE_FIXTURES = ['large20', 'large200'];

function hasBz2(name) {
	return fs.existsSync(fixturePath(name + '.osm.bz2'));
}

function expectedOf(name) {
	var m = manifest[name];
	assert.ok(m, 'manifest.json missing entry for ' + name + ' — run npm run fixtures');
	return m;
}

test('default fixture files exist with distinct sizes', function () {
	var sizes = {};
	for (var i = 0; i < DEFAULT_FIXTURES.length; i++) {
		var name = DEFAULT_FIXTURES[i];
		assert.ok(hasBz2(name), 'missing ' + name + '.osm.bz2 — run npm run fixtures');
		var st = fs.statSync(fixturePath(name + '.osm.bz2'));
		assert.ok(st.size > 0);
		sizes[name] = st.size;
	}
	assert.ok(sizes.tiny < sizes.small, 'tiny < small');
	assert.ok(sizes.small < sizes.medium, 'small < medium');
});

function registerFixtureTests(name, opts) {
	opts = opts || {};
	var optional = !!opts.optional;

	test(name + ': decode whole-file matches raw .osm (when present)', function (t) {
		if (!hasBz2(name)) {
			if (optional) {
				t.skip(name + '.osm.bz2 not found — run: npm run fixtures:large');
				return;
			}
			assert.fail('missing ' + name + '.osm.bz2');
		}
		var expected = expectedOf(name);
		if (!expected.hasRawOsm) {
			t.skip(name + ' has no raw .osm on disk (bz2-only large fixture)');
			return;
		}
		var decoded = decodeBz2File(fixturePath(name + '.osm.bz2'));
		var raw = fs.readFileSync(fixturePath(name + '.osm'));
		assert.equal(decoded.toString('utf8'), raw.toString('utf8'));
	});

	test(name + ': decode block-by-block', function (t) {
		if (!hasBz2(name)) {
			if (optional) {
				t.skip(name + '.osm.bz2 not found — run: npm run fixtures:large');
				return;
			}
			assert.fail('missing ' + name + '.osm.bz2');
		}
		var expected = expectedOf(name);
		var result = decodeBz2ByBlocks(fixturePath(name + '.osm.bz2'));
		assert.ok(result.blocks.length >= 1, 'expected at least one bzip2 block');
		assert.ok(result.text.length > 0);
		if (expected.hasRawOsm) {
			var raw = fs.readFileSync(fixturePath(name + '.osm'), 'utf8');
			assert.equal(result.text, raw);
		} else {
			assert.ok(result.text.indexOf('<osm') !== -1);
			assert.ok(result.text.indexOf('</osm>') !== -1);
			// uncompressed size should be in the ballpark of target
			if (expected.targetMb) {
				var mb = result.text.length / (1024 * 1024);
				assert.ok(
					mb > expected.targetMb * 0.7 && mb < expected.targetMb * 1.4,
					name + ' uncompressed ~' + mb.toFixed(1) + ' MB, target ' + expected.targetMb
				);
			}
		}
		if (name === 'medium' || name === 'large20' || name === 'large200') {
			assert.ok(
				result.blocks.length >= 2,
				name + ' should span multiple bzip2 blocks, got ' + result.blocks.length
			);
		}
	});

	test(name + ': parse XML element counts', async function (t) {
		if (!hasBz2(name)) {
			if (optional) {
				t.skip(name + '.osm.bz2 not found — run: npm run fixtures:large');
				return;
			}
			assert.fail('missing ' + name + '.osm.bz2');
		}
		var expected = expectedOf(name);
		var text;
		if (expected.hasRawOsm) {
			text = fs.readFileSync(fixturePath(name + '.osm'), 'utf8');
		} else {
			// large fixtures: decode only (no raw file)
			text = decodeBz2ByBlocks(fixturePath(name + '.osm.bz2')).text;
		}
		var events = await parseXmlString(text);
		assert.equal(countOpen(events, 'node'), expected.nodes);
		assert.equal(countOpen(events, 'way'), expected.ways);
		assert.equal(countOpen(events, 'osm'), 1);
	});

	test(name + ': pipeline bz2 → seek-bzip → XMLParser', async function (t) {
		if (!hasBz2(name)) {
			if (optional) {
				t.skip(name + '.osm.bz2 not found — run: npm run fixtures:large');
				return;
			}
			assert.fail('missing ' + name + '.osm.bz2');
		}
		var expected = expectedOf(name);
		var result = await parseOsmBz2(fixturePath(name + '.osm.bz2'));
		assert.equal(result.nodeCount, expected.nodes);
		assert.equal(result.wayCount, expected.ways);
		assert.equal(result.openTags.osm, 1);
		assert.ok(result.firstOpen);
		assert.equal(result.firstOpen.name, 'osm');
		assert.equal(result.firstOpen.attrs.version, '0.6');
	});
}

for (var di = 0; di < DEFAULT_FIXTURES.length; di++) {
	registerFixtureTests(DEFAULT_FIXTURES[di], { optional: false });
}
for (var li = 0; li < LARGE_FIXTURES.length; li++) {
	registerFixtureTests(LARGE_FIXTURES[li], { optional: true });
}

test('medium fixture is substantially larger uncompressed than small', function () {
	assert.ok(hasBz2('medium') && hasBz2('small'));
	var small = fs.statSync(fixturePath('small.osm')).size;
	var medium = fs.statSync(fixturePath('medium.osm')).size;
	assert.ok(medium > small * 5);
});
