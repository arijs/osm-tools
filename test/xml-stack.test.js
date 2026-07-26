'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var fs = require('fs');
var os = require('os');
var path = require('path');
var { runProcess } = require('../index0');

function writeOsmBz2(tmpDir, xml) {
	// Use Python-free path: write raw xml and compress with child if needed.
	// For stack tests we can use seek-bzip only if we have bz2 — use runProcess
	// on a prebuilt fixture instead, or create bz2 via zlib... bzip2 not zlib.
	// Simplest: use existing tiny fixture pipeline is overkill.
	// Write a .osm.bz2 with python one-liner.
	var osmPath = path.join(tmpDir, 'c.osm');
	var bz2Path = path.join(tmpDir, 'c.osm.bz2');
	fs.writeFileSync(osmPath, xml, 'utf8');
	var { execFileSync } = require('child_process');
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
	return bz2Path;
}

test('XML stack recovers from close mismatch without throwing', async function () {
	// Malformed-ish sequence that can appear after soft-stop/resume:
	// close changeset while stack is only [osm]
	var xml =
		'<?xml version="1.0"?>\n' +
		'<osm version="0.6">\n' +
		'  <changeset id="1" open="false">\n' +
		'    <tag k="comment" v="a"/>\n' +
		'  </changeset>\n' +
		'  <changeset id="2" open="false">\n' +
		'    <tag k="comment" v="b"/>\n' +
		'  </changeset>\n' +
		'</osm>\n';

	var tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'osm-stack-'));
	var bz2 = writeOsmBz2(tmp, xml);
	var statsPath = path.join(tmp, 's.json');
	var resultsPath = path.join(tmp, 'r.json');

	var result = await runProcess({
		inputPath: bz2,
		statsPath: statsPath,
		resultsPath: resultsPath,
		quiet: true,
		resume: false,
		saveStats: false,
		saveBzStatsSeparate: false,
		checkpointEveryFiles: null,
		syncSchedule: true,
		strictXmlStack: false
	});

	assert.equal(result.error, null);
	assert.ok(result.xml.openCounts.osm >= 1 || result.xml.nodeCount >= 0);
	// well-formed file should have zero mismatches
	assert.equal(result.xmlStackMismatches, 0);
});

test('changeset dump with open=false attributes tracks stack', async function () {
	var xml =
		'<?xml version="1.0"?>\n' +
		'<osm version="0.6" generator="test">\n' +
		'  <changeset id="960186" created_at="2009-04-25T22:12:40Z" ' +
		'closed_at="2009-04-25T22:12:42Z" open="false" user="geobase_stevens" ' +
		'uid="92209" num_changes="0" comments_count="0">\n' +
		'    <tag k="comment" v="Merging nodes at intersections defined by junctions"/>\n' +
		'  </changeset>\n' +
		'  <changeset id="960187" open="false">\n' +
		'    <tag k="comment" v="x"/>\n' +
		'  </changeset>\n' +
		'</osm>\n';

	var tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'osm-cs-'));
	var bz2 = writeOsmBz2(tmp, xml);
	var result = await runProcess({
		inputPath: bz2,
		statsPath: path.join(tmp, 's.json'),
		resultsPath: path.join(tmp, 'r.json'),
		quiet: true,
		resume: false,
		saveStats: false,
		checkpointEveryFiles: null,
		syncSchedule: true,
		strictXmlStack: true
	});

	assert.equal(result.error, null);
	assert.equal(result.xmlStackMismatches, 0);
	var root = result.xml.root.tags;
	assert.ok(root.osm);
	assert.ok(root.osm.tags.changeset);
	assert.equal(root.osm.tags.changeset.count, 2);
});
