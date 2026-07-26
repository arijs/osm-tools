'use strict';

/**
 * Write a minimal OSM PBF fixture for tests (Header + DenseNodes).
 * Usage: node scripts/write-tiny-pbf.js [out.pbf]
 */

var fs = require('fs');
var path = require('path');
var zlib = require('zlib');
var Pbf = require('pbf');
var fileformat = require('../fileformat.proto.js');
var osmformat = require('../osmformat.proto.js');

function writeBlob(type, payloadBuf) {
	var blobPbf = new Pbf();
	fileformat.Blob.write(
		{ raw_size: payloadBuf.length, zlib_data: zlib.deflateSync(payloadBuf) },
		blobPbf
	);
	var blobBuf = blobPbf.finish();

	var hdrPbf = new Pbf();
	fileformat.BlobHeader.write(
		{ type: type, datasize: blobBuf.length },
		hdrPbf
	);
	var hdrBuf = hdrPbf.finish();

	var size = Buffer.alloc(4);
	size.writeUInt32BE(hdrBuf.length, 0);
	return Buffer.concat([size, Buffer.from(hdrBuf), Buffer.from(blobBuf)]);
}

function encodeStrings(list) {
	// stringtable: first entry empty string by convention
	var s = [Buffer.from('')];
	for (var i = 0; i < list.length; i++) s.push(Buffer.from(list[i], 'utf8'));
	return s;
}

function buildTinyPbf() {
	// Header
	var hb = {
		bbox: {
			// Sudeste-ish nanodegrees
			left: Math.round(-50 * 1e9),
			right: Math.round(-40 * 1e9),
			top: Math.round(-14 * 1e9),
			bottom: Math.round(-26 * 1e9)
		},
		required_features: ['OsmSchema-V0.6', 'DenseNodes'],
		optional_features: [],
		writingprogram: 'osm-tools-test',
		source: 'test-fixture'
	};
	var hbPbf = new Pbf();
	osmformat.HeaderBlock.write(hb, hbPbf);
	var headerPart = writeBlob('OSMHeader', Buffer.from(hbPbf.finish()));

	// String table for tags
	var strings = [
		'',
		'addr:street',
		'Av Paulista',
		'addr:housenumber',
		'1000',
		'name',
		'Edificio',
		'highway',
		'residential'
	];
	// indices: 1=addr:street, 2=Av, 3=addr:housenumber, 4=1000, 5=name, 6=Edificio

	// Dense nodes: 3 sequential points near SP
	// delta-coded
	var baseLat = Math.round((-23.56 * 1e9) / 100); // with granularity 100
	var baseLon = Math.round((-46.65 * 1e9) / 100);
	// lat_int such that lat = 1e-9 * (0 + 100 * lat_int) = lat_int * 1e-7
	// so lat_int = lat / 1e-7 = lat * 1e7
	function toInt(deg) {
		return Math.round(deg / 1e-7);
	}
	var lats = [toInt(-23.56), toInt(-23.561) - toInt(-23.56), toInt(-23.562) - toInt(-23.561)];
	var lons = [toInt(-46.65), toInt(-46.651) - toInt(-46.65), toInt(-46.652) - toInt(-46.651)];
	var ids = [1001, 1, 1]; // absolute 1001, 1002, 1003

	// keys_vals: for each node pairs then 0
	// node0: street, housenumber, name
	// node1: name only
	// node2: none
	var keys_vals = [
		1,
		2,
		3,
		4,
		5,
		6,
		0,
		5,
		6,
		0,
		0
	];

	var pb = {
		stringtable: { s: encodeStrings(strings.slice(1)) }, // s[0] will be ''
		// fix stringtable: include empty first
		primitivegroup: [
			{
				nodes: [],
				dense: {
					id: ids,
					lat: lats,
					lon: lons,
					keys_vals: keys_vals,
					denseinfo: null
				},
				ways: [
					{
						id: 2001,
						keys: [7], // highway - wait string indices
						vals: [8],
						info: null,
						refs: [1001, 1] // delta: 1001, 1002
					}
				],
				relations: [],
				changesets: []
			}
		],
		granularity: 100,
		lat_offset: 0,
		lon_offset: 0,
		date_granularity: 1000
	};

	// Rebuild stringtable properly with empty first entry
	pb.stringtable = {
		s: [
			Buffer.from(''),
			Buffer.from('addr:street'),
			Buffer.from('Av Paulista'),
			Buffer.from('addr:housenumber'),
			Buffer.from('1000'),
			Buffer.from('name'),
			Buffer.from('Edificio'),
			Buffer.from('highway'),
			Buffer.from('residential')
		]
	};
	// keys_vals use 1-based as above (1=addr:street...)
	// way keys: highway=7, residential=8
	pb.primitivegroup[0].ways[0].keys = [7];
	pb.primitivegroup[0].ways[0].vals = [8];

	var pbPbf = new Pbf();
	osmformat.PrimitiveBlock.write(pb, pbPbf);
	var dataPart = writeBlob('OSMData', Buffer.from(pbPbf.finish()));

	return Buffer.concat([headerPart, dataPart]);
}

var out =
	process.argv[2] ||
	path.join(__dirname, '..', 'test', 'fixtures', 'tiny.osm.pbf');
fs.mkdirSync(path.dirname(out), { recursive: true });
fs.writeFileSync(out, buildTinyPbf());
console.log('Wrote', out, fs.statSync(out).size, 'bytes');
