'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var fs = require('fs');
var os = require('os');
var path = require('path');
var { execFileSync } = require('child_process');
var {
	runProcess,
	createGeocodeSignals,
	noteGeocodeOpen,
	geocodeHints,
	formatGeocodeSignals,
	snapshotGeocodeSignals,
	recomputeGeocodeSignalsFromTree
} = require('../index0');

test('noteGeocodeOpen counts nodes with lat/lon and addr tags', function () {
	var g = createGeocodeSignals();
	noteGeocodeOpen(g, 'node', { id: '1', lat: '-23.5', lon: '-46.6' });
	noteGeocodeOpen(g, 'tag', { k: 'addr:street', v: 'X' });
	noteGeocodeOpen(g, 'tag', { k: 'addr:housenumber', v: '10' });
	noteGeocodeOpen(g, 'tag', { k: 'name', v: 'Y' });
	noteGeocodeOpen(g, 'way', { id: '2' });
	noteGeocodeOpen(g, 'tag', { k: 'highway', v: 'residential' });

	assert.equal(g.node, 1);
	assert.equal(g.nodeWithLatLon, 1);
	assert.equal(g.addrStreet, 1);
	assert.equal(g.addrHousenumber, 1);
	assert.equal(g.addrAny, 2);
	assert.equal(g.name, 1);
	assert.equal(g.way, 1);
	assert.equal(g.highway, 1);

	var hints = geocodeHints(g);
	assert.equal(hints.hasPointGeometry, true);
	assert.equal(hints.hasAddressTags, true);
	assert.equal(hints.likelyHasGeocodeMaterial, true);

	var s = formatGeocodeSignals(g);
	assert.ok(s.indexOf('GEO') === 0, s);
	assert.ok(s.indexOf('ll=') !== -1, s);
	assert.ok(s.indexOf('addr=') !== -1, s);
});

test('geocodeHints false when only changesets (no nodes)', function () {
	var g = createGeocodeSignals();
	noteGeocodeOpen(g, 'changeset', { id: '1', open: 'false' });
	noteGeocodeOpen(g, 'tag', { k: 'comment', v: 'edit' });
	var hints = geocodeHints(g);
	assert.equal(hints.likelyHasGeocodeMaterial, false);
	assert.ok(formatGeocodeSignals(g).indexOf('geo ') === 0);
});

test('recomputeGeocodeSignalsFromTree matches inventory shape', function () {
	var root = {
		tags: {
			osm: {
				count: 1,
				tags: {
					node: {
						count: 10,
						attrs: {
							lat: { count: 10 },
							lon: { count: 10 }
						},
						tag_k_map: {
							'addr:street': { count: 3 },
							name: { count: 5 }
						}
					},
					way: {
						count: 2,
						tag_k_map: {
							highway: { count: 2 }
						}
					}
				}
			}
		}
	};
	var g = recomputeGeocodeSignalsFromTree(root);
	assert.equal(g.node, 10);
	assert.equal(g.nodeWithLatLon, 10);
	assert.equal(g.addrStreet, 3);
	assert.equal(g.addrAny, 3);
	assert.equal(g.name, 5);
	assert.equal(g.way, 2);
	assert.equal(g.highway, 2);
	assert.equal(geocodeHints(g).likelyHasGeocodeMaterial, true);
});

test('runProcess puts geocodeSignals on results for tiny fixture', async function () {
	var tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'osm-geo-'));
	var result = await runProcess({
		inputPath: path.join(__dirname, 'fixtures', 'tiny.osm.bz2'),
		statsPath: path.join(tmp, 's.json'),
		resultsPath: path.join(tmp, 'r.json'),
		quiet: true,
		resume: false,
		saveStats: true,
		checkpointEveryFiles: null,
		syncSchedule: true
	});

	assert.ok(result.geocodeSignals);
	assert.ok(result.geocodeSignals.node >= 3);
	assert.ok(result.geocodeSignals.nodeWithLatLon >= 3);
	assert.ok(result.geocodeSignals.name >= 1); // Node N tags
	assert.equal(
		result.geocodeSignals.hints.hasPointGeometry,
		true
	);
	assert.equal(
		result.geocodeSignals.hints.likelyHasGeocodeMaterial,
		true
	);

	var results = JSON.parse(fs.readFileSync(path.join(tmp, 'r.json'), 'utf8'));
	assert.ok(results.geocodeSignals);
	assert.ok(results.geocodeSignals.hints);

	var stats = JSON.parse(fs.readFileSync(path.join(tmp, 's.json'), 'utf8'));
	assert.ok(stats.geocodeSignals);
	assert.ok(stats.xml.geocodeSignals);
});

test('runProcess address-like XML sets GEO signals', async function () {
	var xml =
		'<?xml version="1.0"?>\n' +
		'<osm version="0.6">\n' +
		'  <node id="1" lat="-23.56" lon="-46.65">\n' +
		'    <tag k="addr:street" v="Av Paulista"/>\n' +
		'    <tag k="addr:housenumber" v="1000"/>\n' +
		'    <tag k="name" v="Edificio"/>\n' +
		'  </node>\n' +
		'</osm>\n';
	var tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'osm-geo2-'));
	var osmPath = path.join(tmp, 'a.osm');
	var bz2Path = path.join(tmp, 'a.osm.bz2');
	fs.writeFileSync(osmPath, xml);
	execFileSync(
		'python',
		[
			'-c',
			'import bz2,sys; open(sys.argv[2],"wb").write(bz2.compress(open(sys.argv[1],"rb").read()))',
			osmPath,
			bz2Path
		],
		{ stdio: 'pipe' }
	);

	var result = await runProcess({
		inputPath: bz2Path,
		statsPath: path.join(tmp, 's.json'),
		resultsPath: path.join(tmp, 'r.json'),
		quiet: true,
		resume: false,
		saveStats: false,
		checkpointEveryFiles: null,
		syncSchedule: true
	});

	assert.equal(result.geocodeSignals.node, 1);
	assert.equal(result.geocodeSignals.nodeWithLatLon, 1);
	assert.equal(result.geocodeSignals.addrStreet, 1);
	assert.equal(result.geocodeSignals.addrHousenumber, 1);
	assert.equal(result.geocodeSignals.addrAny, 2);
	assert.equal(result.geocodeSignals.name, 1);
	assert.equal(result.geocodeSignals.hints.likelyHasGeocodeMaterial, true);
	assert.ok(
		snapshotGeocodeSignals(result.geocodeSignals).hints.likelyHasGeocodeMaterial
	);
});
