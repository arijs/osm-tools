'use strict';

/**
 * Minimal OSM PBF with geocode material for extract tests:
 * - place=city node with IBGE (São Paulo)
 * - place=suburb node
 * - highway way with name spanning cached nodes (+ alt_name)
 * - place=state node
 * - place=square way (praça, área fechada) e place=square node
 * - leisure=park way; landuse=residential nomeado (negativo: não é logradouro)
 * - highway+place na mesma way (highway ganha)
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
		Buffer.from('1000'),
		// 20 square 21 Praça da Sé 22 leisure 23 park 24 Parque do Ibirapuera
		// 25 alt_name 26 Rua Augusta Velha 27 Praça do Correio
		// 28 landuse 29 Jardim Teste 30 pedestrian 31 Largo Teste
		Buffer.from('square'),
		Buffer.from('Praça da Sé'),
		Buffer.from('leisure'),
		Buffer.from('park'),
		Buffer.from('Parque do Ibirapuera'),
		Buffer.from('alt_name'),
		Buffer.from('Rua Augusta Velha'),
		Buffer.from('Praça do Correio'),
		Buffer.from('landuse'),
		Buffer.from('Jardim Teste'),
		Buffer.from('pedestrian'),
		Buffer.from('Largo Teste')
	];

	// Dense nodes (delta):
	// 1001 city SP, 1002 suburb, 1003 street node A, 1004 street node B, 1005 state, 1006 addr point
	// 1007/1008 cantos da praça (way fechada), 1009/1010 cantos do parque,
	// 1011 praça mapeada como nó
	var nodeLat = [
		-23.55, -23.551, -23.552, -23.553, -22.0, -23.561,
		-23.5505, -23.551, -23.5875, -23.588, -23.545
	];
	var nodeLon = [
		-46.63, -46.631, -46.632, -46.633, -48.0, -46.65,
		-46.634, -46.6335, -46.658, -46.657, -46.636
	];
	var ids = [1001, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1];
	var lats = nodeLat.map(function (v, i) {
		return i === 0 ? toInt(v) : toInt(v) - toInt(nodeLat[i - 1]);
	});
	var lons = nodeLon.map(function (v, i) {
		return i === 0 ? toInt(v) : toInt(v) - toInt(nodeLon[i - 1]);
	});
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
		16, 17, 18, 19, 0,
		// 1007..1010 bare (geometria de área)
		0,
		0,
		0,
		0,
		// 1011 place=square + name
		1, 20, 3, 27, 0
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
						keys: [9, 3, 25], // highway, name, alt_name
						vals: [10, 11, 26], // residential, Rua Augusta, Rua Augusta Velha
						info: null,
						refs: [1003, 1] // 1003, 1004
					},
					{
						id: 2002,
						keys: [1, 3], // place, name
						vals: [20, 21], // square, Praça da Sé
						info: null,
						refs: [1007, 1, -1] // 1007, 1008, 1007 (fechada)
					},
					{
						id: 2003,
						keys: [22, 3], // leisure, name
						vals: [23, 24], // park, Parque do Ibirapuera
						info: null,
						refs: [1009, 1] // 1009, 1010
					},
					{
						id: 2004,
						keys: [28, 3], // landuse, name — NÃO é logradouro
						vals: [10, 29], // residential, Jardim Teste
						info: null,
						refs: [1009, 1]
					},
					{
						id: 2005,
						keys: [9, 1, 3], // highway + place + name → highway ganha
						vals: [30, 20, 31], // pedestrian, square, Largo Teste
						info: null,
						refs: [1007, 1]
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
