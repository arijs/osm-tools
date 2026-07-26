'use strict';

/**
 * Extract geocode features from OSM PBF → DNE-style @-delimited TXT files.
 *
 * CLI:
 *   node extract-geocode-pbf.js [file.osm.pbf] --out=DIR
 *   --datasets=estado,municipio,bairro,logradouro[,addr]
 *   --resume / --no-resume
 *   --node-cache=N   (default 500000)
 *
 * Soft-stop: Ctrl+C (finish current blob); 2nd hard-stop.
 */

var fs = require('fs');
var path = require('path');
var Pbf = require('pbf');
var osmformat = require('./osmformat.proto.js');
var pbfReader = require('./pbf-reader');
var dataSize = require('./datasize');
var nameNorm = require('./name-norm').nameNorm;
var ufBr = require('./uf-br');
var txtAt = require('./txt-at-writer');

var DEFAULT_SOFT_STOP_MS = 30000;
var DEFAULT_NODE_CACHE = 500000;

var PLACE_STATE = { state: 1 };
var PLACE_MUNI = { city: 1, municipality: 1, town: 1 };
var PLACE_BAIRRO = { suburb: 1, neighbourhood: 1, quarter: 1, district: 1 };

function printTime(s) {
	var m = s / 60;
	var h = m / 60;
	var d = h / 24;
	s = Math.floor(s % 60).toFixed(0);
	m = Math.floor(m % 60).toFixed(0);
	h = Math.floor(h % 24).toFixed(0);
	return h < 1
		? ('  ' + m).substr(-2) + ':' + ('00' + s).substr(-2)
		: d <= 1
			? ('  ' + h).substr(-2) + ':' + ('00' + m).substr(-2)
			: d.toFixed(1) + 'd';
}

function decodeCoord(n, granularity, offset) {
	return 1e-9 * (offset + granularity * n);
}

function stringTableToArray(st) {
	var out = [];
	if (!st || !st.s) return out;
	for (var i = 0; i < st.s.length; i++) {
		var b = st.s[i];
		out.push(
			Buffer.isBuffer(b) ? b.toString('utf8') : Buffer.from(b).toString('utf8')
		);
	}
	return out;
}

function tagsFromKeysVals(keys, vals, strings) {
	var tags = {};
	if (!keys || !vals) return tags;
	for (var i = 0; i < keys.length; i++) {
		var k = strings[keys[i]];
		var v = strings[vals[i]];
		if (k != null) tags[k] = v != null ? v : '';
	}
	return tags;
}

function forEachDenseNode(dense, strings, granularity, latOffset, lonOffset, onNode) {
	if (!dense || !dense.id || !dense.id.length) return;
	var id = 0;
	var lat = 0;
	var lon = 0;
	var kv = dense.keys_vals || [];
	var ki = 0;
	for (var i = 0; i < dense.id.length; i++) {
		id += dense.id[i];
		lat += dense.lat[i];
		lon += dense.lon[i];
		var tags = {};
		while (ki < kv.length) {
			var sid = kv[ki++];
			if (sid === 0) break;
			var vid = kv[ki++];
			var k = strings[sid];
			var v = strings[vid];
			if (k != null) tags[k] = v != null ? v : '';
		}
		onNode(
			id,
			decodeCoord(lat, granularity, latOffset),
			decodeCoord(lon, granularity, lonOffset),
			tags
		);
	}
}

function decodeWayRefs(refs) {
	var ids = [];
	if (!refs || !refs.length) return ids;
	var id = 0;
	for (var i = 0; i < refs.length; i++) {
		id += refs[i];
		ids.push(id);
	}
	return ids;
}

function emptyGeom() {
	return {
		lat: '',
		lng: '',
		lat_min: '',
		lat_max: '',
		lng_min: '',
		lng_max: '',
		way_node_count: 0,
		nodes_resolved: 0
	};
}

function geomFromPoint(lat, lon) {
	return {
		lat: lat,
		lng: lon,
		lat_min: lat,
		lat_max: lat,
		lng_min: lon,
		lng_max: lon,
		way_node_count: 1,
		nodes_resolved: 1
	};
}

function geomFromNodeIds(nodeIds, cache) {
	var g = emptyGeom();
	g.way_node_count = nodeIds.length;
	var sumLat = 0;
	var sumLon = 0;
	var n = 0;
	var latMin = Infinity;
	var latMax = -Infinity;
	var lonMin = Infinity;
	var lonMax = -Infinity;
	for (var i = 0; i < nodeIds.length; i++) {
		var c = cache.get(nodeIds[i]);
		if (!c) continue;
		var lat = c[0];
		var lon = c[1];
		sumLat += lat;
		sumLon += lon;
		n++;
		if (lat < latMin) latMin = lat;
		if (lat > latMax) latMax = lat;
		if (lon < lonMin) lonMin = lon;
		if (lon > lonMax) lonMax = lon;
	}
	g.nodes_resolved = n;
	if (n === 0) return g;
	g.lat = sumLat / n;
	g.lng = sumLon / n;
	g.lat_min = latMin;
	g.lat_max = latMax;
	g.lng_min = lonMin;
	g.lng_max = lonMax;
	return g;
}

function createNodeCache(maxSize) {
	var map = new Map();
	var max = maxSize > 0 ? maxSize : DEFAULT_NODE_CACHE;
	return {
		set: function (id, lat, lon) {
			if (map.size >= max) {
				var drop = Math.max(1, Math.floor(max * 0.1));
				var it = map.keys();
				for (var i = 0; i < drop; i++) {
					var k = it.next().value;
					if (k === undefined) break;
					map.delete(k);
				}
			}
			map.set(id, [lat, lon]);
		},
		get: function (id) {
			return map.get(id);
		},
		get size() {
			return map.size;
		}
	};
}

function displayName(tags) {
	if (!tags) return '';
	return tags['name:pt'] || tags.name || tags['official_name'] || '';
}

function wantDataset(set, name) {
	return set[name] === true;
}

/**
 * Process one primitive; write rows via ctx.writer
 */
function processFeatureNode(ctx, id, lat, lon, tags) {
	// Em two-pass, não enche o LRU com 141M nós sem tag.
	// Só cacheia nós “interessantes” (place/admin/addr) para relations no pass1.
	if (
		!ctx.twoPassGeometry ||
		tags.place ||
		tags.boundary ||
		tags.admin_level ||
		tags['addr:street'] ||
		tags.highway
	) {
		ctx.nodeCache.set(id, lat, lon);
	}
	var place = tags.place || '';
	var admin = tags.admin_level || '';
	var boundary = tags.boundary || '';
	var ibgeInfo = ufBr.extractIbge(tags);
	var g = geomFromPoint(lat, lon);
	var name = displayName(tags);
	var nn = nameNorm(name);

	if (wantDataset(ctx.datasets, 'estado')) {
		if (PLACE_STATE[place] || (boundary === 'administrative' && admin === '4')) {
			var uf = ufBr.resolveUf({
				tags: tags,
				lat: lat,
				lng: lon,
				ibge: ibgeInfo.ibge
			});
			if (uf === 'XX') uf = ufBr.normalizeUfToken(tags.ref) || uf;
			ctx.writer.write('OSM_ESTADO', [
				'node',
				id,
				uf === 'XX' ? '' : uf,
				name,
				nn,
				g.lat,
				g.lng,
				g.lat_min,
				g.lat_max,
				g.lng_min,
				g.lng_max,
				admin,
				place
			]);
			ctx.stats.estado++;
		}
	}

	if (wantDataset(ctx.datasets, 'municipio')) {
		// Só place city/town/municipality ou boundary admin_level=8.
		// NÃO emitir suburb/district com IBGE de 8–9 dígitos (polui match).
		var ibgeMun = municipioIbgeOnly(ibgeInfo.ibge);
		if (
			PLACE_MUNI[place] ||
			(boundary === 'administrative' && admin === '8')
		) {
			var ufM = ufBr.resolveUf({
				tags: tags,
				lat: lat,
				lng: lon,
				ibge: ibgeMun || ibgeInfo.ibge
			});
			ctx.writer.write('OSM_MUNICIPIO', [
				'node',
				id,
				ibgeMun,
				ufM === 'XX' ? '' : ufM,
				name,
				nn,
				g.lat,
				g.lng,
				g.lat_min,
				g.lat_max,
				g.lng_min,
				g.lng_max,
				admin,
				place,
				ibgeMun ? ibgeInfo.source_tag : ''
			]);
			ctx.stats.municipio++;
		}
	}

	if (wantDataset(ctx.datasets, 'bairro') && PLACE_BAIRRO[place]) {
		var city = tags['addr:city'] || tags['is_in:city'] || tags['is_in'] || '';
		var ufB = ufBr.resolveUf({ tags: tags, lat: lat, lng: lon });
		ctx.writer.write('OSM_BAIRRO', [
			'node',
			id,
			name,
			nn,
			ufB === 'XX' ? '' : ufB,
			city,
			nameNorm(city),
			ibgeInfo.ibge,
			g.lat,
			g.lng,
			g.lat_min,
			g.lat_max,
			g.lng_min,
			g.lng_max,
			place
		]);
		ctx.stats.bairro++;
	}

	if (wantDataset(ctx.datasets, 'addr') && tags['addr:street']) {
		var street = tags['addr:street'];
		var ufA = ufBr.resolveUf({ tags: tags, lat: lat, lng: lon });
		var base = 'OSM_ADDR_POINT_' + (ufA === 'XX' ? 'XX' : ufA);
		ctx.writer.write(base, [
			id,
			lat,
			lon,
			street,
			nameNorm(street),
			tags['addr:housenumber'] || '',
			tags['addr:city'] || '',
			tags['addr:suburb'] || '',
			tags['addr:postcode'] || '',
			name
		]);
		ctx.stats.addr++;
	}
}

function processFeatureWay(ctx, way) {
	var tags = way.tags;
	var name = displayName(tags);
	var admin = tags.admin_level || '';
	var boundary = tags.boundary || '';
	var place = tags.place || '';
	var nodeIds = decodeWayRefs(way.refs);
	var g = geomFromNodeIds(nodeIds, ctx.nodeCache);
	var ibgeInfo = ufBr.extractIbge(tags);
	var nn = nameNorm(name);

	if (wantDataset(ctx.datasets, 'logradouro') && tags.highway && name) {
		if (ctx.twoPassStreets) {
			// Pass 1: só agenda; coords resolvidas na pass 2
			for (var ri = 0; ri < nodeIds.length; ri++) {
				ctx.neededNodeIds.add(nodeIds[ri]);
			}
			ctx.pendingStreets.push({
				id: way.id,
				tags: tags,
				nodeIds: nodeIds,
				name: name,
				nn: nn
			});
			ctx.stats.logradouroPending++;
		} else {
			writeLogradouroRow(ctx, way.id, tags, name, nn, g, ibgeInfo);
		}
	}

	if (wantDataset(ctx.datasets, 'estado')) {
		if (PLACE_STATE[place] || (boundary === 'administrative' && admin === '4')) {
			var ufE = ufBr.resolveUf({
				tags: tags,
				lat: g.lat === '' ? null : g.lat,
				lng: g.lng === '' ? null : g.lng,
				ibge: ibgeInfo.ibge
			});
			ctx.writer.write('OSM_ESTADO', [
				'way',
				way.id,
				ufE === 'XX' ? '' : ufE,
				name,
				nn,
				g.lat,
				g.lng,
				g.lat_min,
				g.lat_max,
				g.lng_min,
				g.lng_max,
				admin,
				place
			]);
			ctx.stats.estado++;
		}
	}

	if (wantDataset(ctx.datasets, 'municipio')) {
		var ibgeMunW = municipioIbgeOnly(ibgeInfo.ibge);
		if (
			PLACE_MUNI[place] ||
			(boundary === 'administrative' && admin === '8')
		) {
			var ufM = ufBr.resolveUf({
				tags: tags,
				lat: g.lat === '' ? null : g.lat,
				lng: g.lng === '' ? null : g.lng,
				ibge: ibgeMunW || ibgeInfo.ibge
			});
			ctx.writer.write('OSM_MUNICIPIO', [
				'way',
				way.id,
				ibgeMunW,
				ufM === 'XX' ? '' : ufM,
				name,
				nn,
				g.lat,
				g.lng,
				g.lat_min,
				g.lat_max,
				g.lng_min,
				g.lng_max,
				admin,
				place,
				ibgeMunW ? ibgeInfo.source_tag : ''
			]);
			ctx.stats.municipio++;
		}
	}

	if (wantDataset(ctx.datasets, 'bairro') && PLACE_BAIRRO[place]) {
		var city = tags['addr:city'] || '';
		var ufB = ufBr.resolveUf({
			tags: tags,
			lat: g.lat === '' ? null : g.lat,
			lng: g.lng === '' ? null : g.lng
		});
		ctx.writer.write('OSM_BAIRRO', [
			'way',
			way.id,
			name,
			nn,
			ufB === 'XX' ? '' : ufB,
			city,
			nameNorm(city),
			ibgeInfo.ibge,
			g.lat,
			g.lng,
			g.lat_min,
			g.lat_max,
			g.lng_min,
			g.lng_max,
			place
		]);
		ctx.stats.bairro++;
	}
}

/** IBGE município = exatamente 7 dígitos; senão string vazia. */
function municipioIbgeOnly(raw) {
	if (raw == null || raw === '') return '';
	var d = String(raw).replace(/\D/g, '');
	return d.length === 7 ? d : '';
}

function writeLogradouroRow(ctx, wayId, tags, name, nn, g, ibgeInfo) {
	var uf = ufBr.resolveUf({
		tags: tags,
		lat: g.lat === '' ? null : g.lat,
		lng: g.lng === '' ? null : g.lng,
		lat_min: g.lat_min === '' ? null : g.lat_min,
		lat_max: g.lat_max === '' ? null : g.lat_max,
		lng_min: g.lng_min === '' ? null : g.lng_min,
		lng_max: g.lng_max === '' ? null : g.lng_max,
		ibge: ibgeInfo && ibgeInfo.ibge
	});
	var base = 'OSM_LOGRADOURO_' + (uf || 'XX');
	ctx.writer.write(base, [
		wayId,
		name,
		nn,
		tags.highway,
		uf === 'XX' ? '' : uf,
		tags['addr:city'] || '',
		nameNorm(tags['addr:city'] || ''),
		tags['addr:suburb'] || tags['is_in'] || '',
		nameNorm(tags['addr:suburb'] || ''),
		tags['addr:postcode'] || '',
		g.lat,
		g.lng,
		g.lat_min,
		g.lat_max,
		g.lng_min,
		g.lng_max,
		g.way_node_count
	]);
	ctx.stats.logradouro++;
	if (g.nodes_resolved === 0) ctx.stats.logradouroNoGeom++;
}

/**
 * After pass 2 filled streetCoords, emit deferred logradouros with real UF/geom.
 */
function emitPendingStreets(ctx) {
	var streetCache = {
		get: function (id) {
			return ctx.streetCoords.get(id);
		}
	};
	for (var i = 0; i < ctx.pendingStreets.length; i++) {
		var w = ctx.pendingStreets[i];
		var g = geomFromNodeIds(w.nodeIds, streetCache);
		var ibgeInfo = ufBr.extractIbge(w.tags);
		writeLogradouroRow(ctx, w.id, w.tags, w.name, w.nn, g, ibgeInfo);
	}
	ctx.pendingStreets = [];
}

function processFeatureRelation(ctx, rel) {
	var tags = rel.tags;
	var name = displayName(tags);
	var nn = nameNorm(name);
	var admin = tags.admin_level || '';
	var boundary = tags.boundary || '';
	var place = tags.place || '';
	var ibgeInfo = ufBr.extractIbge(tags);
	var ibgeMun = municipioIbgeOnly(ibgeInfo.ibge);
	// geometry: try admin_centre / label node members from cache
	var g = emptyGeom();
	var centreIds = [];
	if (rel.members && rel.members.length) {
		for (var i = 0; i < rel.members.length; i++) {
			var m = rel.members[i];
			if (m.type !== 0 /* NODE */) continue;
			var role = m.role || '';
			if (role === 'admin_centre' || role === 'label' || role === 'capital') {
				centreIds.push(m.id);
				var c = ctx.nodeCache.get(m.id);
				if (c && g.lat === '') {
					g = geomFromPoint(c[0], c[1]);
				}
			}
		}
	}

	if (wantDataset(ctx.datasets, 'estado')) {
		if (PLACE_STATE[place] || (boundary === 'administrative' && admin === '4')) {
			var ufE = ufBr.resolveUf({
				tags: tags,
				lat: g.lat === '' ? null : g.lat,
				lng: g.lng === '' ? null : g.lng,
				ibge: ibgeInfo.ibge
			});
			ctx.writer.write('OSM_ESTADO', [
				'relation',
				rel.id,
				ufE === 'XX' ? '' : ufE,
				name,
				nn,
				g.lat,
				g.lng,
				g.lat_min,
				g.lat_max,
				g.lng_min,
				g.lng_max,
				admin,
				place
			]);
			ctx.stats.estado++;
		}
	}

	if (wantDataset(ctx.datasets, 'municipio')) {
		var isMuniRel =
			PLACE_MUNI[place] ||
			(boundary === 'administrative' && admin === '8') ||
			(ibgeMun && name && admin === '8');
		if (isMuniRel) {
			// Two-pass: se não achou admin_centre no cache, agenda node ids
			if (
				g.lat === '' &&
				centreIds.length &&
				ctx.twoPassGeometry
			) {
				for (var ci = 0; ci < centreIds.length; ci++) {
					ctx.neededNodeIds.add(centreIds[ci]);
				}
				ctx.pendingAdminMuni.push({
					id: rel.id,
					tags: tags,
					name: name,
					nn: nn,
					admin: admin,
					place: place,
					ibgeMun: ibgeMun,
					source_tag: ibgeMun ? ibgeInfo.source_tag : '',
					centreIds: centreIds
				});
				ctx.stats.municipioPending++;
			} else {
				writeMunicipioRow(ctx, 'relation', rel.id, tags, name, nn, g, ibgeMun, ibgeMun ? ibgeInfo.source_tag : '', admin, place);
			}
		}
	}

	if (wantDataset(ctx.datasets, 'bairro') && PLACE_BAIRRO[place]) {
		var city = tags['addr:city'] || '';
		var ufB = ufBr.resolveUf({
			tags: tags,
			lat: g.lat === '' ? null : g.lat,
			lng: g.lng === '' ? null : g.lng
		});
		ctx.writer.write('OSM_BAIRRO', [
			'relation',
			rel.id,
			name,
			nn,
			ufB === 'XX' ? '' : ufB,
			city,
			nameNorm(city),
			ibgeMun,
			g.lat,
			g.lng,
			g.lat_min,
			g.lat_max,
			g.lng_min,
			g.lng_max,
			place
		]);
		ctx.stats.bairro++;
	}
}

function writeMunicipioRow(ctx, osmType, osmId, tags, name, nn, g, ibgeMun, sourceTag, admin, place) {
	var ufM = ufBr.resolveUf({
		tags: tags,
		lat: g.lat === '' ? null : g.lat,
		lng: g.lng === '' ? null : g.lng,
		ibge: ibgeMun
	});
	ctx.writer.write('OSM_MUNICIPIO', [
		osmType,
		osmId,
		ibgeMun,
		ufM === 'XX' ? '' : ufM,
		name,
		nn,
		g.lat,
		g.lng,
		g.lat_min,
		g.lat_max,
		g.lng_min,
		g.lng_max,
		admin,
		place,
		sourceTag || ''
	]);
	ctx.stats.municipio++;
}

function emitPendingAdminMuni(ctx) {
	for (var i = 0; i < ctx.pendingAdminMuni.length; i++) {
		var p = ctx.pendingAdminMuni[i];
		var g = emptyGeom();
		for (var j = 0; j < p.centreIds.length; j++) {
			var c = ctx.streetCoords.get(p.centreIds[j]);
			if (c) {
				g = geomFromPoint(c[0], c[1]);
				break;
			}
		}
		writeMunicipioRow(
			ctx,
			'relation',
			p.id,
			p.tags,
			p.name,
			p.nn,
			g,
			p.ibgeMun,
			p.source_tag,
			p.admin,
			p.place
		);
	}
	ctx.pendingAdminMuni = [];
}

function processPrimitiveBlock(block, ctx) {
	var strings = stringTableToArray(block.stringtable);
	var granularity = block.granularity || 100;
	var latOffset = block.lat_offset || 0;
	var lonOffset = block.lon_offset || 0;
	var groups = block.primitivegroup || [];

	for (var g = 0; g < groups.length; g++) {
		var pg = groups[g];

		if (pg.nodes && pg.nodes.length) {
			for (var ni = 0; ni < pg.nodes.length; ni++) {
				var nd = pg.nodes[ni];
				var tags = tagsFromKeysVals(nd.keys, nd.vals, strings);
				var lat = decodeCoord(nd.lat, granularity, latOffset);
				var lon = decodeCoord(nd.lon, granularity, lonOffset);
				processFeatureNode(ctx, nd.id, lat, lon, tags);
				ctx.stats.nodes++;
			}
		}

		if (pg.dense) {
			forEachDenseNode(
				pg.dense,
				strings,
				granularity,
				latOffset,
				lonOffset,
				function (id, lat, lon, tags) {
					processFeatureNode(ctx, id, lat, lon, tags);
					ctx.stats.nodes++;
				}
			);
		}

		if (pg.ways && pg.ways.length) {
			for (var wi = 0; wi < pg.ways.length; wi++) {
				var w = pg.ways[wi];
				processFeatureWay(ctx, {
					id: w.id,
					tags: tagsFromKeysVals(w.keys, w.vals, strings),
					refs: w.refs || []
				});
				ctx.stats.ways++;
			}
		}

		if (pg.relations && pg.relations.length) {
			for (var ri = 0; ri < pg.relations.length; ri++) {
				var r = pg.relations[ri];
				var roles = r.roles_sid || [];
				var memids = r.memids || [];
				var types = r.types || [];
				var members = [];
				var mid = 0;
				for (var mi = 0; mi < memids.length; mi++) {
					mid += memids[mi];
					members.push({
						id: mid,
						type: types[mi],
						role: strings[roles[mi]] || ''
					});
				}
				processFeatureRelation(ctx, {
					id: r.id,
					tags: tagsFromKeysVals(r.keys, r.vals, strings),
					members: members
				});
				ctx.stats.relations++;
			}
		}
	}
}

/** Pass 2: only harvest coordinates for node ids needed by pending streets. */
function harvestStreetNodes(block, neededNodeIds, streetCoords, stats) {
	var strings = stringTableToArray(block.stringtable);
	var granularity = block.granularity || 100;
	var latOffset = block.lat_offset || 0;
	var lonOffset = block.lon_offset || 0;
	var groups = block.primitivegroup || [];
	for (var g = 0; g < groups.length; g++) {
		var pg = groups[g];
		if (pg.nodes && pg.nodes.length) {
			for (var ni = 0; ni < pg.nodes.length; ni++) {
				var nd = pg.nodes[ni];
				if (!neededNodeIds.has(nd.id)) continue;
				streetCoords.set(nd.id, [
					decodeCoord(nd.lat, granularity, latOffset),
					decodeCoord(nd.lon, granularity, lonOffset)
				]);
				stats.streetNodesResolved++;
			}
		}
		if (pg.dense) {
			forEachDenseNode(
				pg.dense,
				strings,
				granularity,
				latOffset,
				lonOffset,
				function (id, lat, lon) {
					if (!neededNodeIds.has(id)) return;
					streetCoords.set(id, [lat, lon]);
					stats.streetNodesResolved++;
				}
			);
		}
	}
}

function parseDatasets(str) {
	var all = {
		estado: true,
		municipio: true,
		bairro: true,
		logradouro: true,
		addr: false
	};
	if (!str || str === 'all') return all;
	var out = {
		estado: false,
		municipio: false,
		bairro: false,
		logradouro: false,
		addr: false
	};
	var parts = String(str).split(/[,+\s]+/);
	for (var i = 0; i < parts.length; i++) {
		var p = parts[i].toLowerCase().trim();
		if (p === 'addr' || p === 'addr_point' || p === 'addrpoint') out.addr = true;
		else if (out[p] !== undefined) out[p] = true;
	}
	return out;
}

function writeReadmeColunas(outDir) {
	var md =
		'# OSM geo extract — contrato de colunas (delimitador `@`)\n\n' +
		'Encoding: UTF-8. Sem header. Campos vazios = string vazia.\n' +
		'Campos com `@` ou quebras de linha são sanitizados no extract.\n\n' +
		'## OSM_ESTADO.TXT\n\n' +
		'`osm_type@osm_id@uf@name@name_norm@lat@lng@lat_min@lat_max@lng_min@lng_max@admin_level@place`\n\n' +
		'## OSM_MUNICIPIO.TXT\n\n' +
		'`osm_type@osm_id@ibge@uf@name@name_norm@lat@lng@lat_min@lat_max@lng_min@lng_max@admin_level@place@source_tag`\n\n' +
		'## OSM_BAIRRO.TXT\n\n' +
		'`osm_type@osm_id@name@name_norm@uf@city@city_norm@ibge_hint@lat@lng@lat_min@lat_max@lng_min@lng_max@place`\n\n' +
		'## OSM_LOGRADOURO_{UF}.TXT\n\n' +
		'Um arquivo por UF (`SP`, `RJ`, `MG`, `ES`, …) + residual `XX`.\n\n' +
		'`osm_id@name@name_norm@highway@uf@city@city_norm@suburb@suburb_norm@postcode@lat@lng@lat_min@lat_max@lng_min@lng_max@way_node_count`\n\n' +
		'## OSM_ADDR_POINT_{UF}.TXT (opcional)\n\n' +
		'`osm_id@lat@lng@street@street_norm@housenumber@city@suburb@postcode@name`\n\n' +
		'Plano: `docs/plans/osm-para-locais-geo.md`\n';
	fs.writeFileSync(path.join(outDir, 'README-colunas.md'), md, 'utf8');
}

/**
 * @param {object} options
 * @param {string} options.inputPath
 * @param {string} options.outDir
 * @param {object} [options.datasets]
 * @param {boolean} [options.quiet]
 * @param {boolean} [options.resume]
 * @param {number} [options.nodeCacheMax]
 * @param {number} [options.softStopMaxMs]
 * @param {function} [options.onControl]
 */
function runExtractGeocode(options) {
	options = options || {};
	var inputPath = options.inputPath;
	if (!inputPath) {
		return Promise.reject(new Error('runExtractGeocode: inputPath required'));
	}
	var outDir = options.outDir;
	if (!outDir) {
		return Promise.reject(new Error('runExtractGeocode: outDir required'));
	}
	var quiet = !!options.quiet;
	var resume = !!options.resume;
	var softStopMaxMs =
		options.softStopMaxMs == null ? DEFAULT_SOFT_STOP_MS : options.softStopMaxMs;
	var datasets = options.datasets || parseDatasets('all');
	// Two-pass geometry (default on): pass1 agenda node ids (streets + admin_centre);
	// pass2 resolve coords. LRU single-pass falhava em ~99% das ways.
	var twoPassGeometry =
		options.twoPassStreets !== false &&
		options.twoPassGeometry !== false &&
		(wantDataset(datasets, 'logradouro') || wantDataset(datasets, 'municipio'));
	var twoPassStreets = twoPassGeometry && wantDataset(datasets, 'logradouro');
	var statsPath = path.join(outDir, 'extract-checkpoint.json');

	return new Promise(function (resolve, reject) {
		var finished = false;
		var softStopRequested = false;
		var softStopDeadline = 0;
		var hardStopRequested = false;
		var stopReason = null;
		var stoppedEarly = false;

		function fail(err) {
			if (finished) return;
			finished = true;
			reject(err);
		}
		function ok(result) {
			if (finished) return;
			finished = true;
			resolve(result);
		}

		function requestSoftStop(reason) {
			if (hardStopRequested || finished || softStopRequested) return;
			softStopRequested = true;
			softStopDeadline = Date.now() + softStopMaxMs;
			if (!quiet) {
				console.error(
					'\nSoft-stop extract: fim do blob atual (max ' +
						Math.round(softStopMaxMs / 1000) +
						's)' +
						(reason ? ' — ' + reason : '') +
						'\n'
				);
			}
		}
		function requestHardStop() {
			hardStopRequested = true;
			softStopRequested = true;
			softStopDeadline = Date.now();
			if (!quiet) console.error('\nHard-stop extract...\n');
		}
		if (typeof options.onControl === 'function') {
			options.onControl({
				softStop: requestSoftStop,
				hardStop: requestHardStop
			});
		}

		var fileSize;
		try {
			fileSize = fs.statSync(inputPath).size;
		} catch (e) {
			return fail(e);
		}

		fs.mkdirSync(outDir, { recursive: true });
		if (!resume) {
			// wipe previous TXT in outDir that we own
			try {
				var existing = fs.readdirSync(outDir);
				for (var ei = 0; ei < existing.length; ei++) {
					if (/^OSM_.*\.TXT$/i.test(existing[ei])) {
						fs.unlinkSync(path.join(outDir, existing[ei]));
					}
				}
			} catch (_) {}
		}

		var startOffset = 0;
		var startBlobIndex = 0;
		var prevStats = null;
		if (resume && fs.existsSync(statsPath)) {
			try {
				prevStats = JSON.parse(fs.readFileSync(statsPath, 'utf8'));
				startOffset = (prevStats.cursor && prevStats.cursor.fileOffset) || 0;
				startBlobIndex = (prevStats.cursor && prevStats.cursor.blobIndex) || 0;
				if (!quiet) {
					console.error(
						'Retomando extract offset=' + startOffset + ' blob=' + startBlobIndex
					);
				}
			} catch (e) {
				if (!quiet) console.error('Aviso: checkpoint inválido', e.message);
			}
		}

		var writer = txtAt.createTxtAtWriter(outDir, { append: resume && startOffset > 0 });
		writeReadmeColunas(outDir);

		var ctx = {
			writer: writer,
			datasets: datasets,
			twoPassStreets: twoPassStreets,
			twoPassGeometry: twoPassGeometry,
			pendingStreets: [],
			pendingAdminMuni: [],
			neededNodeIds: new Set(),
			streetCoords: new Map(),
			nodeCache: createNodeCache(options.nodeCacheMax || DEFAULT_NODE_CACHE),
			stats: Object.assign(
				{
					nodes: 0,
					ways: 0,
					relations: 0,
					estado: 0,
					municipio: 0,
					municipioPending: 0,
					bairro: 0,
					logradouro: 0,
					logradouroPending: 0,
					logradouroNoGeom: 0,
					streetNodesResolved: 0,
					addr: 0
				},
				// não misturar contagens de run anterior no two-pass
				twoPassGeometry ? {} : (prevStats && prevStats.counts) || {}
			)
		};

		var headerInfo = (prevStats && prevStats.header) || null;
		var dataBlobs = 0;
		var bytesReadEstimate = startOffset;
		var tRun = Date.now();
		var lastPrint = 0;

		if (!quiet) {
			console.error('Extract geocode PBF → TXT @');
			console.error('Arquivo: ' + inputPath + ' (' + dataSize(fileSize) + ')');
			console.error('Saída:   ' + outDir);
			console.error(
				'Datasets: ' +
					Object.keys(datasets)
						.filter(function (k) {
							return datasets[k];
						})
						.join(',')
			);
			if (twoPassGeometry) {
				console.error(
					'Two-pass geometry: pass1 agenda (logradouro + admin_centre), pass2 resolve nós'
				);
			}
			console.error('');
		}

		function printProgress(blobIndex, force) {
			if (quiet) return;
			var now = Date.now();
			if (!force && now - lastPrint < 250) return;
			lastPrint = now;
			var elapsed = (now - tRun) * 0.001;
			var percent = (100 * bytesReadEstimate) / fileSize;
			var speed = elapsed > 0 ? (bytesReadEstimate - startOffset) / elapsed : 0;
			process.stdout.write(
				'\r' +
					percent.toFixed(3) +
					' ' +
					dataSize(speed) +
					'/s ' +
					printTime(elapsed) +
					' blob ' +
					blobIndex +
					' mun=' +
					ctx.stats.municipio +
					' log=' +
					ctx.stats.logradouro +
					' cache=' +
					ctx.nodeCache.size +
					'   '
			);
		}

		function shouldStop() {
			if (hardStopRequested) return true;
			if (!softStopRequested) return false;
			return Date.now() >= softStopDeadline;
		}

		function headerBBoxToDegrees(bbox) {
			if (!bbox) return null;
			return {
				left: bbox.left * 1e-9,
				right: bbox.right * 1e-9,
				top: bbox.top * 1e-9,
				bottom: bbox.bottom * 1e-9
			};
		}

		try {
			pbfReader.forEachBlob(
				inputPath,
				{ startOffset: startOffset, startBlobIndex: startBlobIndex },
				function (blob) {
					bytesReadEstimate = blob.nextOffset;
					if (blob.type === 'OSMHeader') {
						var hb = osmformat.HeaderBlock.read(new Pbf(blob.data));
						headerInfo = {
							bbox: headerBBoxToDegrees(hb.bbox),
							writingprogram: hb.writingprogram || '',
							source: hb.source || ''
						};
					} else if (blob.type === 'OSMData') {
						var pb = osmformat.PrimitiveBlock.read(new Pbf(blob.data));
						processPrimitiveBlock(pb, ctx);
						dataBlobs++;
					}
					printProgress(blob.blobIndex, false);
					if (shouldStop()) {
						stoppedEarly = true;
						stopReason = hardStopRequested ? 'hard-stop' : 'soft-stop';
						return 'stop';
					}
				}
			);
		} catch (err) {
			return fail(err);
		}

		printProgress(startBlobIndex + dataBlobs, true);
		if (!quiet) process.stdout.write('\n');

		// Pass 2: coords para logradouros e admin_centre de municípios
		var needPass2 =
			twoPassGeometry &&
			!stoppedEarly &&
			ctx.neededNodeIds.size > 0 &&
			(ctx.pendingStreets.length > 0 || ctx.pendingAdminMuni.length > 0);
		if (needPass2) {
			if (!quiet) {
				console.error(
					'Pass 2/2: resolvendo ' +
						ctx.neededNodeIds.size +
						' nós (logradouro=' +
						ctx.pendingStreets.length +
						' admin_muni=' +
						ctx.pendingAdminMuni.length +
						')...'
				);
			}
			try {
				var lastPrint2 = 0;
				pbfReader.forEachBlob(inputPath, { startOffset: 0, startBlobIndex: 0 }, function (
					blob
				) {
					if (blob.type === 'OSMData') {
						var pb2 = osmformat.PrimitiveBlock.read(new Pbf(blob.data));
						harvestStreetNodes(
							pb2,
							ctx.neededNodeIds,
							ctx.streetCoords,
							ctx.stats
						);
					}
					if (!quiet) {
						var now2 = Date.now();
						if (now2 - lastPrint2 > 400) {
							lastPrint2 = now2;
							process.stdout.write(
								'\r pass2 blob ' +
									blob.blobIndex +
									' resolved=' +
									ctx.streetCoords.size +
									'/' +
									ctx.neededNodeIds.size +
									'   '
							);
						}
					}
					if (shouldStop()) {
						stoppedEarly = true;
						stopReason = hardStopRequested ? 'hard-stop' : 'soft-stop';
						return 'stop';
					}
				});
				if (!quiet) process.stdout.write('\n');
				if (!stoppedEarly) {
					if (ctx.pendingAdminMuni.length) {
						emitPendingAdminMuni(ctx);
					}
					if (ctx.pendingStreets.length) {
						emitPendingStreets(ctx);
					}
					if (!quiet) {
						console.error(
							'Municípios: ' +
								ctx.stats.municipio +
								' | Logradouros: ' +
								ctx.stats.logradouro +
								' (sem geom: ' +
								ctx.stats.logradouroNoGeom +
								')'
						);
					}
				}
			} catch (err2) {
				return fail(err2);
			}
		} else if (
			twoPassGeometry &&
			(ctx.pendingStreets.length || ctx.pendingAdminMuni.length) &&
			stoppedEarly
		) {
			if (!quiet) {
				console.error(
					'Soft-stop antes da pass 2 — pendentes não gravados. Rode de novo sem --resume.'
				);
			}
		}

		writer
			.flush()
			.then(function () {
				var cursor = {
					fileOffset: bytesReadEstimate,
					blobIndex: startBlobIndex + dataBlobs,
					eof: bytesReadEstimate >= fileSize && !stoppedEarly
				};
				var payload = {
					header: headerInfo,
					cursor: cursor,
					counts: ctx.stats,
					datasets: datasets,
					twoPassStreets: twoPassStreets,
					writerCounts: writer.counts,
					inputPath: inputPath,
					outDir: outDir,
					stoppedEarly: stoppedEarly,
					stopReason: stopReason,
					nodeCacheSize: ctx.nodeCache.size,
					streetCoordsSize: ctx.streetCoords.size,
					neededNodeIds: ctx.neededNodeIds.size
				};
				fs.writeFileSync(statsPath, JSON.stringify(payload, null, 2), 'utf8');
				if (!quiet) {
					console.error('Checkpoint: ' + statsPath);
					console.error('Linhas por arquivo:', JSON.stringify(writer.counts));
					console.error('Contagens:', JSON.stringify(ctx.stats));
				}
				ok({
					error: null,
					stoppedEarly: stoppedEarly,
					stopReason: stopReason,
					header: headerInfo,
					cursor: cursor,
					counts: ctx.stats,
					writerCounts: Object.assign({}, writer.counts),
					outDir: outDir,
					statsPath: statsPath
				});
			})
			.catch(fail);
	});
}

function parseCli(argv) {
	var args = argv.slice(2);
	var positional = [];
	var opts = {
		outDir: null,
		datasets: null,
		resume: false,
		quiet: false,
		nodeCacheMax: DEFAULT_NODE_CACHE
	};
	for (var i = 0; i < args.length; i++) {
		var a = args[i];
		if (a === '--resume') opts.resume = true;
		else if (a === '--no-resume') opts.resume = false;
		else if (a === '--quiet' || a === '-q') opts.quiet = true;
		else if (a.indexOf('--out=') === 0) opts.outDir = a.slice(6);
		else if (a === '--out') opts.outDir = args[++i];
		else if (a.indexOf('--datasets=') === 0) opts.datasets = parseDatasets(a.slice(11));
		else if (a === '--datasets') opts.datasets = parseDatasets(args[++i]);
		else if (a.indexOf('--node-cache=') === 0)
			opts.nodeCacheMax = parseInt(a.slice(13), 10) || DEFAULT_NODE_CACHE;
		else if (a === '--addr-points') {
			opts.datasets = opts.datasets || parseDatasets('all');
			opts.datasets.addr = true;
		} else if (a.indexOf('-') === 0) {
			// ignore unknown flags
		} else {
			positional.push(a);
		}
	}
	opts.inputPath =
		positional[0] ||
		process.env.OSM_PBF_INPUT ||
		path.resolve('G:\\sudeste-260725.osm.pbf');
	opts.outDir =
		opts.outDir ||
		positional[1] ||
		process.env.OSM_GEO_OUT ||
		opts.inputPath.replace(/\.pbf$/i, '') + '-geo';
	if (!opts.datasets) opts.datasets = parseDatasets('all');
	return opts;
}

function main() {
	var opts = parseCli(process.argv);
	var softCount = 0;
	function onSig() {
		softCount++;
		if (softCount === 1 && control) control.softStop('SIGINT');
		else if (softCount === 2 && control) control.hardStop();
		else process.exit(130);
	}
	var control = null;
	process.on('SIGINT', onSig);
	process.on('SIGTERM', onSig);

	runExtractGeocode({
		inputPath: opts.inputPath,
		outDir: opts.outDir,
		datasets: opts.datasets,
		resume: opts.resume,
		quiet: opts.quiet,
		nodeCacheMax: opts.nodeCacheMax,
		onControl: function (c) {
			control = c;
		}
	})
		.then(function (result) {
			process.exit(result.stoppedEarly ? 0 : 0);
		})
		.catch(function (err) {
			console.error(err);
			process.exit(1);
		});
}

module.exports = {
	runExtractGeocode: runExtractGeocode,
	parseDatasets: parseDatasets,
	parseCli: parseCli,
	processPrimitiveBlock: processPrimitiveBlock,
	processFeatureNode: processFeatureNode,
	processFeatureWay: processFeatureWay,
	emitPendingStreets: emitPendingStreets,
	harvestStreetNodes: harvestStreetNodes,
	geomFromNodeIds: geomFromNodeIds,
	decodeWayRefs: decodeWayRefs,
	createNodeCache: createNodeCache,
	nameNorm: nameNorm
};

if (require.main === module) {
	main();
}
