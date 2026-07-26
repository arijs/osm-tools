'use strict';

/**
 * Minimal OSM PBF with geocode material for extract tests:
 * - place=city node with IBGE (São Paulo)
 * - place=suburb node
 * - highway way with name spanning cached nodes
 * - place=state node
 *
 * Usage: node scripts/write-geocode-pbf.js [out.pbf]
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
	fileformat.BlobHeader.write({ type: type, datasize: blobBuf.length }, hdrPbf);
	var hdrBuf = hdrPbf.finish();
	var size = Buffer.alloc(4);
	size.writeUInt32BE(hdrBuf.length, 0);
	return Buffer.concat([size, Buffer.from(hdrBuf), Buffer.from(blobBuf)]);
}

function toInt(deg) {
	return Math.round(deg / 1e-7);
}

function buildGeocodePbf() {
	var hb = {
		bbox: {
			left: Math.round(-50 * 1e9),
			right: Math.round(-40 * 1e9),
			top: Math.round(-14 * 1e9),
			bottom: Math.round(-26 * 1e9)
		},
		required_features: ['OsmSchema-V0.6', 'DenseNodes'],
		optional_features: [],
		writingprogram: 'osm-tools-geocode-fixture',
		source: 'test'
	};
	var hbPbf = new Pbf();
	osmformat.HeaderBlock.write(hb, hbPbf);
	var headerPart = writeBlob('OSMHeader', Buffer.from(hbPbf.finish()));

	// stringtable indices
	// 0 empty
	// 1 place 2 city 3 name 4 São Paulo 5 IBGE:GEOCODIGO 6 3550308
	// 7 suburb 8 Consolação
	// 9 highway 10 residential 11 Rua Augusta
	// 12 state 13 São Paulo (state name) 14 ISO3166-2 15 BR-SP
	// 16 addr:street 17 Av Paulista 18 addr:housenumber 19 1000
	var st = [
		Buffer.from(''),
		Buffer.from('place'),
		Buffer.from('city'),
		Buffer.from('name'),
		Buffer.from('São Paulo'),
		Buffer.from('IBGE:GEOCODIGO'),
		Buffer.from('3550308'),
		Buffer.from('suburb'),
		Buffer.from('Consolação'),
		Buffer.from('highway'),
		Buffer.from('residential'),
		Buffer.from('Rua Augusta'),
		Buffer.from('state'),
		Buffer.from('São Paulo'),
		Buffer.from('ISO3166-2'),
		Buffer.from('BR-SP'),
		Buffer.from('addr:street'),
		Buffer.from('Av Paulista'),
		Buffer.from('addr:housenumber'),
		Buffer.from('1000')
	];

	// Dense nodes (delta):
	// 1001 city SP, 1002 suburb, 1003 street node A, 1004 street node B, 1005 state, 1006 addr point
	var ids = [1001, 1, 1, 1, 1, 1];
	var lats = [
		toInt(-23.55),
		toInt(-23.551) - toInt(-23.55),
		toInt(-23.552) - toInt(-23.551),
		toInt(-23.553) - toInt(-23.552),
		toInt(-22.0) - toInt(-23.553),
		toInt(-23.561) - toInt(-22.0)
	];
	var lons = [
		toInt(-46.63),
		toInt(-46.631) - toInt(-46.63),
		toInt(-46.632) - toInt(-46.631),
		toInt(-46.633) - toInt(-46.632),
		toInt(-48.0) - toInt(-46.633),
		toInt(-46.65) - toInt(-48.0)
	];
	// keys_vals per node terminated by 0
	var keys_vals = [
		// 1001 city + name + ibge
		1, 2, 3, 4, 5, 6, 0,
		// 1002 suburb + name
		1, 7, 3, 8, 0,
		// 1003 bare
		0,
		// 1004 bare
		0,
		// 1005 state + name + ISO
		1, 12, 3, 13, 14, 15, 0,
		// 1006 addr
		16, 17, 18, 19, 0
	];

	var pb = {
		stringtable: { s: st },
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
						keys: [9, 3], // highway, name
						vals: [10, 11], // residential, Rua Augusta
						info: null,
						refs: [1003, 1] // 1003, 1004
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

	var pbPbf = new Pbf();
	osmformat.PrimitiveBlock.write(pb, pbPbf);
	var dataPart = writeBlob('OSMData', Buffer.from(pbPbf.finish()));
	return Buffer.concat([headerPart, dataPart]);
}

var out =
	process.argv[2] ||
	path.join(__dirname, '..', 'test', 'fixtures', 'geocode-mini.osm.pbf');
fs.mkdirSync(path.dirname(out), { recursive: true });
fs.writeFileSync(out, buildGeocodePbf());
console.log('Wrote', out, fs.statSync(out).size, 'bytes');
