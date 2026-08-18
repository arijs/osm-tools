'use strict';

/**
 * Extract geocode features from OSM PBF → DNE-style @-delimited TXT files.
 *
 * CLI:
 *   node extract-geocode-pbf.js [file.osm.pbf] --out=DIR
 *   --datasets=estado,municipio,bairro,logradouro[,addr][,geom]
 *   --way-geom       (= --datasets=…,geom: OSM_LOGRADOURO_GEOM_{UF} com o traçado)
 *   --resume / --no-resume
 *   --node-cache=N   (default 500000)
 *   --shard-lines=N  (0=flat .TXT; N>0 → OSM_KEY/{N}-linhas/000001.txt + MANIFEST)
 *   --shard-datasets=logradouro,addr  (default when shard-lines>0)
 *   --uf=SP,RJ       (só estas UFs; reduz saída; combina com --region)
 *   --region=norte|nordeste|centro-oeste|sudeste|sul
 *   --wave-nodes=N   (default 8e6; flush pass2 antes do limite do Set do V8)
 *   --wave-streets=N (default 150000 pending logradouros por onda)
 *
 * Cancel (Ctrl+C): 1º soft-stop no fim do blob (sem novas waves);
 * 2º hard-stop (+ exit forçado em 2s); 3º process.exit(130).
 * Blobs via forEachBlobAsync para o SIGINT não ficar preso dentro da wave.
 *
 * Brasil (~2GB): o two-pass agenda todos os nós de logradouro num Set; V8 estoura
 * (~16M). Waves + fatias --uf/--region evitam RangeError: Set maximum size exceeded.
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
var polyline = require('./geo-polyline');
var oneway = require('./osm-oneway');

var DEFAULT_SOFT_STOP_MS = 30000;
var DEFAULT_NODE_CACHE = 500000;
/** Abaixo do teto ~16.7M do Set no V8; margem para admin + crescimento. */
var DEFAULT_WAVE_NODES = 8000000;
var DEFAULT_WAVE_STREETS = 150000;

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

/**
 * @param {Array<number>} nodeIds
 * @param {{get: function}} cache
 * @param {boolean} [coletarPontos] guarda a lista de coordenadas em `g.pontos`.
 *   Opt-in porque a lista é lixo para quem só quer centroide/bbox — e esta
 *   função roda para TODA way do PBF, não só para logradouro.
 */
function geomFromNodeIds(nodeIds, cache, coletarPontos) {
	var g = emptyGeom();
	g.way_node_count = nodeIds.length;
	if (coletarPontos) g.pontos = [];
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
		if (g.pontos) g.pontos.push([lat, lon]);
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

var NAME_ALT_TAGS = [
	'alt_name',
	'short_name',
	'old_name',
	'loc_name',
	'name:pt-BR',
	'official_name'
];

/**
 * Denominações alternativas do OSM (equivalente ao LOG_VAR_LOG do DNE).
 * Multi-valor separado por `;` — mesma convenção do próprio OSM.
 */
function altNames(tags, primary) {
	if (!tags) return [];
	var seen = Object.create(null);
	if (primary) seen[primary] = 1;
	var out = [];
	for (var i = 0; i < NAME_ALT_TAGS.length; i++) {
		var raw = tags[NAME_ALT_TAGS[i]];
		if (!raw) continue;
		var parts = String(raw).split(';');
		for (var j = 0; j < parts.length; j++) {
			var v = parts[j].trim();
			if (!v || seen[v]) continue;
			seen[v] = 1;
			out.push(v);
		}
	}
	return out;
}

/**
 * Shape que o DNE trata como logradouro. Rua/avenida é `highway`; praça, largo
 * e parque não são — são área (`place=square`, `leisure=park`) e por isso nunca
 * entravam. Medido: `Praça` casava 35,9% contra 86,3% de `Rua` (docs/geo/
 * melhoria-extracao-coordenadas.md §10.1).
 *
 * ponytail: só way (fechada ou não) e node. Relation multipolygon fica de fora —
 * custaria um 3º passe e vale ~25 linhas de "Parque" na capital. Se doer:
 * resolver members da relation no mesmo two-pass dos streets.
 */
function logradouroKind(tags) {
	if (!tags) return '';
	if (tags.highway) return tags.highway;
	if (tags.place === 'square') return 'square';
	if (tags.leisure === 'park' || tags.leisure === 'garden') return 'park';
	if (tags.landuse === 'village_green') return 'park';
	return '';
}

function wantDataset(set, name) {
	return set[name] === true;
}

/** Keep feature when no UF filter, or UF/ponto está na fatia pedida. */
function keepByUfFilter(ctx, uf, lat, lon) {
	return ufBr.passesUfFilter(ctx.ufAllow, uf, lat, lon);
}

function latLonFromGeom(g) {
	if (!g) return { lat: null, lon: null };
	var lat = g.lat === '' || g.lat == null ? null : Number(g.lat);
	var lon = g.lng === '' || g.lng == null ? null : Number(g.lng);
	if ((lat == null || !isFinite(lat)) && g.lat_min !== '' && g.lat_max !== '') {
		lat = (Number(g.lat_min) + Number(g.lat_max)) / 2;
		lon = (Number(g.lng_min) + Number(g.lng_max)) / 2;
	}
	return { lat: lat, lon: lon };
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
			if (keepByUfFilter(ctx, uf, lat, lon)) {
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
			if (keepByUfFilter(ctx, ufM, lat, lon)) {
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
	}

	if (wantDataset(ctx.datasets, 'bairro') && PLACE_BAIRRO[place]) {
		var city = tags['addr:city'] || tags['is_in:city'] || tags['is_in'] || '';
		var ufB = ufBr.resolveUf({ tags: tags, lat: lat, lng: lon });
		if (keepByUfFilter(ctx, ufB, lat, lon)) {
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
	}

	// Praça mapeada como nó: ponto exato, sem extensão (bbox degenerada).
	// É coordenada real — não confundir com centroide de fallback.
	if (wantDataset(ctx.datasets, 'logradouro') && tags.place === 'square' && name) {
		writeLogradouroRow(ctx, 'node', id, tags, name, nn, g, ibgeInfo);
	}

	if (wantDataset(ctx.datasets, 'addr') && tags['addr:street']) {
		var street = tags['addr:street'];
		var ufA = ufBr.resolveUf({ tags: tags, lat: lat, lng: lon });
		if (keepByUfFilter(ctx, ufA, lat, lon)) {
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
}

function processFeatureWay(ctx, way) {
	var tags = way.tags;
	var name = displayName(tags);
	var admin = tags.admin_level || '';
	var boundary = tags.boundary || '';
	var place = tags.place || '';
	var nodeIds = decodeWayRefs(way.refs);
	// Com two-pass, a way de logradouro é AGENDADA e o `g` daqui não chega a
	// virar linha — a geometria boa sai na pass 2, com os nós resolvidos. Coletar
	// pontos aqui seria alocar uma lista por way, para todas as ways do PBF, e
	// jogar fora. Sem two-pass (arquivo pequeno) o `g` é o que vai para a linha.
	var g = geomFromNodeIds(nodeIds, ctx.nodeCache, ctx.wantGeom && !ctx.twoPassStreets);
	var ibgeInfo = ufBr.extractIbge(tags);
	var nn = nameNorm(name);

	if (wantDataset(ctx.datasets, 'logradouro') && logradouroKind(tags) && name) {
		// Early reject se tags já apontam UF fora da fatia (economiza agenda).
		if (!ufBr.tagsDisallowedByFilter(ctx.ufAllow, tags, ibgeInfo.ibge)) {
			if (ctx.twoPassStreets) {
				// Pass 1: só agenda; coords resolvidas na pass 2 (ou wave flush)
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
				writeLogradouroRow(ctx, 'way', way.id, tags, name, nn, g, ibgeInfo);
			}
		} else {
			ctx.stats.logradouroSkippedFilter =
				(ctx.stats.logradouroSkippedFilter || 0) + 1;
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
			var llE = latLonFromGeom(g);
			if (keepByUfFilter(ctx, ufE, llE.lat, llE.lon)) {
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
			var llM = latLonFromGeom(g);
			if (keepByUfFilter(ctx, ufM, llM.lat, llM.lon)) {
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
	}

	if (wantDataset(ctx.datasets, 'bairro') && PLACE_BAIRRO[place]) {
		var city = tags['addr:city'] || '';
		var ufB = ufBr.resolveUf({
			tags: tags,
			lat: g.lat === '' ? null : g.lat,
			lng: g.lng === '' ? null : g.lng
		});
		var llB = latLonFromGeom(g);
		if (keepByUfFilter(ctx, ufB, llB.lat, llB.lon)) {
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
}

/** IBGE município = exatamente 7 dígitos; senão string vazia. */
function municipioIbgeOnly(raw) {
	if (raw == null || raw === '') return '';
	var d = String(raw).replace(/\D/g, '');
	return d.length === 7 ? d : '';
}

function writeLogradouroRow(ctx, osmType, wayId, tags, name, nn, g, ibgeInfo) {
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
	var ll = latLonFromGeom(g);
	if (!keepByUfFilter(ctx, uf, ll.lat, ll.lon)) {
		ctx.stats.logradouroSkippedFilter =
			(ctx.stats.logradouroSkippedFilter || 0) + 1;
		return;
	}
	var base = 'OSM_LOGRADOURO_' + (uf || 'XX');
	var alts = altNames(tags, name);
	ctx.writer.write(base, [
		wayId,
		name,
		nn,
		logradouroKind(tags),
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
		g.way_node_count,
		alts.join(';'),
		alts.map(nameNorm).join(';'),
		osmType
	]);
	ctx.stats.logradouro++;
	if (g.nodes_resolved === 0) ctx.stats.logradouroNoGeom++;
	writeLogradouroGeomRow(ctx, uf, wayId, g, tags);
}

/**
 * Traçado da way, em arquivo IRMÃO do logradouro (`OSM_LOGRADOURO_GEOM_{UF}`).
 *
 * Irmão, e não uma coluna a mais na linha de logradouro, por dois motivos: o
 * `dne-geo-join.js` lê o arquivo de logradouro inteiro e não tem uso nenhum
 * para geometria (pagaria ~3× o I/O à toa), e quem quer só o traçado — o
 * carregador do ddsoft — não precisa varrer 20 colunas de nome e bbox.
 *
 * Fica na MESMA UF e passa pelo MESMO filtro do logradouro porque só é chamado
 * depois que a linha principal foi escrita: um id aqui sempre existe lá.
 *
 * Menos de dois pontos distintos não vira linha — praça mapeada como nó, way
 * cujos nós não foram resolvidos, way degenerada. Fica de fora e é contada, em
 * vez de virar uma linha vazia que o consumidor teria de filtrar.
 *
 * Coluna 3 `oneway` (0–3): sentido compacto — ver `osm-oneway.js`.
 */
function writeLogradouroGeomRow(ctx, uf, wayId, g, tags) {
	if (!ctx.wantGeom) return;
	if (!g || !g.pontos || g.pontos.length < 2) {
		ctx.stats.logradouroGeomVazio++;
		return;
	}
	var linha = polyline.encodePolyline(g.pontos);
	if (!linha || linha.indexOf(polyline.SEP_PONTO) < 0) {
		// Todos os nós colapsaram no mesmo ponto após o arredondamento.
		ctx.stats.logradouroGeomVazio++;
		return;
	}
	var sentido = oneway.onewayCode(tags);
	ctx.writer.write('OSM_LOGRADOURO_GEOM_' + (uf || 'XX'), [
		wayId,
		linha,
		String(sentido)
	]);
	ctx.stats.logradouroGeom++;
	ctx.stats.logradouroGeomPontos += polyline.countPolyline(linha);
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
		var g = geomFromNodeIds(w.nodeIds, streetCache, ctx.wantGeom);
		var ibgeInfo = ufBr.extractIbge(w.tags);
		writeLogradouroRow(ctx, 'way', w.id, w.tags, w.name, w.nn, g, ibgeInfo);
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
			var llE = latLonFromGeom(g);
			if (keepByUfFilter(ctx, ufE, llE.lat, llE.lon)) {
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
	}

	if (wantDataset(ctx.datasets, 'municipio')) {
		var isMuniRel =
			PLACE_MUNI[place] ||
			(boundary === 'administrative' && admin === '8') ||
			(ibgeMun && name && admin === '8');
		if (isMuniRel) {
			// Early reject por tags/IBGE quando há fatia UF
			if (ufBr.tagsDisallowedByFilter(ctx.ufAllow, tags, ibgeMun || ibgeInfo.ibge)) {
				ctx.stats.municipioSkippedFilter =
					(ctx.stats.municipioSkippedFilter || 0) + 1;
			} else if (
				g.lat === '' &&
				centreIds.length &&
				ctx.twoPassGeometry
			) {
				// Two-pass: agenda admin_centre em set separado (não entra no wave de ruas)
				for (var ci = 0; ci < centreIds.length; ci++) {
					ctx.neededAdminNodeIds.add(centreIds[ci]);
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
		var llB = latLonFromGeom(g);
		if (keepByUfFilter(ctx, ufB, llB.lat, llB.lon)) {
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
}

function writeMunicipioRow(ctx, osmType, osmId, tags, name, nn, g, ibgeMun, sourceTag, admin, place) {
	var ufM = ufBr.resolveUf({
		tags: tags,
		lat: g.lat === '' ? null : g.lat,
		lng: g.lng === '' ? null : g.lng,
		ibge: ibgeMun
	});
	var ll = latLonFromGeom(g);
	if (!keepByUfFilter(ctx, ufM, ll.lat, ll.lon)) {
		ctx.stats.municipioSkippedFilter =
			(ctx.stats.municipioSkippedFilter || 0) + 1;
		return;
	}
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

/**
 * Pass 2: harvest coordinates for node ids in neededA (and optional neededB).
 * neededB: admin_centre ids (não entram no wave de logradouro).
 */
function harvestStreetNodes(block, neededA, streetCoords, stats, neededB) {
	var strings = stringTableToArray(block.stringtable);
	var granularity = block.granularity || 100;
	var latOffset = block.lat_offset || 0;
	var lonOffset = block.lon_offset || 0;
	var groups = block.primitivegroup || [];
	function want(id) {
		return neededA.has(id) || (neededB && neededB.has(id));
	}
	for (var g = 0; g < groups.length; g++) {
		var pg = groups[g];
		if (pg.nodes && pg.nodes.length) {
			for (var ni = 0; ni < pg.nodes.length; ni++) {
				var nd = pg.nodes[ni];
				if (!want(nd.id)) continue;
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
					if (!want(id)) return;
					streetCoords.set(id, [lat, lon]);
					stats.streetNodesResolved++;
				}
			);
		}
	}
}

/**
 * Mid-pass flush: resolve e grava logradouros pendentes, libera o Set.
 * Evita RangeError: Set maximum size exceeded no Brasil inteiro.
 * Async: cede o event loop entre blobs para Ctrl+C funcionar durante a wave.
 * @returns {Promise<{ stopped: boolean }>}
 */
function flushStreetWave(ctx, inputPath, quiet, shouldStop) {
	if (!ctx.pendingStreets.length || !ctx.neededNodeIds.size) {
		return Promise.resolve({ stopped: false });
	}
	var waveNum = (ctx.stats.streetWaves || 0) + 1;
	if (!quiet) {
		console.error(
			'\nWave ' +
				waveNum +
				': resolvendo ' +
				ctx.neededNodeIds.size +
				' nós / ' +
				ctx.pendingStreets.length +
				' logradouros...'
		);
	}
	var stopped = false;
	var lastPrint = 0;
	var target = ctx.neededNodeIds.size;
	return pbfReader
		.forEachBlobAsync(inputPath, { startOffset: 0, startBlobIndex: 0 }, function (
			blob
		) {
			// Checa stop **antes** de processar — responde rápido ao Ctrl+C
			if (shouldStop && shouldStop()) {
				stopped = true;
				return 'stop';
			}
			if (blob.type === 'OSMData') {
				var pb = osmformat.PrimitiveBlock.read(new Pbf(blob.data));
				harvestStreetNodes(pb, ctx.neededNodeIds, ctx.streetCoords, ctx.stats);
				// Nós vêm antes das ways no PBF: se já temos tudo, pode parar.
				if (ctx.streetCoords.size >= target) return 'stop';
			}
			if (!quiet) {
				var now = Date.now();
				if (now - lastPrint > 400) {
					lastPrint = now;
					process.stdout.write(
						'\r wave' +
							waveNum +
							' blob ' +
							blob.blobIndex +
							' resolved=' +
							ctx.streetCoords.size +
							'/' +
							target +
							'   '
					);
				}
			}
			if (shouldStop && shouldStop()) {
				stopped = true;
				return 'stop';
			}
		})
		.then(function () {
			if (!quiet) process.stdout.write('\n');
			if (stopped) {
				// Wave incompleta: não emite (coords parciais). Caller trata cancel.
				if (!quiet) {
					console.error(
						'Wave ' + waveNum + ' cancelada — logradouros desta onda não gravados.'
					);
				}
				return { stopped: true };
			}
			emitPendingStreets(ctx);
			ctx.neededNodeIds = new Set();
			ctx.streetCoords = new Map();
			ctx.stats.streetWaves = waveNum;
			return { stopped: false };
		});
}

function shouldFlushStreetWave(ctx, waveNodes, waveStreets) {
	return (
		ctx.twoPassStreets &&
		(ctx.neededNodeIds.size >= waveNodes ||
			ctx.pendingStreets.length >= waveStreets)
	);
}

/** Linha de resumo final no stderr (datasets gravados + waves). */
function formatExtractSummary(stats) {
	stats = stats || {};
	var parts = [];
	if (stats.estado) parts.push('Estados: ' + stats.estado);
	parts.push('Municípios: ' + (stats.municipio || 0));
	parts.push('Bairros: ' + (stats.bairro || 0));
	parts.push(
		'Logradouros: ' +
			(stats.logradouro || 0) +
			' (sem geom: ' +
			(stats.logradouroNoGeom || 0) +
			')'
	);
	if (stats.addr) parts.push('Addr: ' + stats.addr);
	if (stats.logradouroGeom) {
		parts.push(
			'Geom: ' +
				stats.logradouroGeom +
				' vias, ' +
				(stats.logradouroGeomPontos || 0) +
				' pontos (sem traçado: ' +
				(stats.logradouroGeomVazio || 0) +
				')'
		);
	}
	if (stats.streetWaves) parts.push('waves=' + stats.streetWaves);
	return parts.join(' | ');
}

function parseDatasets(str) {
	var all = {
		estado: true,
		municipio: true,
		bairro: true,
		logradouro: true,
		addr: false,
		geom: false
	};
	if (!str || str === 'all') return all;
	var out = {
		estado: false,
		municipio: false,
		bairro: false,
		logradouro: false,
		addr: false,
		geom: false
	};
	var parts = String(str).split(/[,+\s]+/);
	for (var i = 0; i < parts.length; i++) {
		var p = parts[i].toLowerCase().trim();
		if (p === 'addr' || p === 'addr_point' || p === 'addrpoint') out.addr = true;
		else if (p === 'geom' || p === 'way_geom' || p === 'waygeom') out.geom = true;
		else if (out[p] !== undefined) out[p] = true;
	}
	// `geom` é o traçado DAS WAYS DE LOGRADOURO: sem o dataset que as emite ele
	// não teria de onde sair. Ligar junto evita a saída silenciosamente vazia de
	// `--datasets=geom`, que ninguém escreve querendo nada.
	if (out.geom && !out.logradouro) out.logradouro = true;
	return out;
}

function writeReadmeColunas(outDir, opts) {
	opts = opts || {};
	var shardLines = opts.shardLines || 0;
	var shardNote =
		shardLines > 0
			? '\n## Modo fatiado (`--shard-lines=' +
				shardLines +
				'`)\n\n' +
				'Datasets grandes (logradouro, bairro, addr) gravam:\n\n' +
				'```\nKEY/\n  ' +
				shardLines +
				'-linhas/\n    000001.txt\n    000002.txt\n  MANIFEST.json\n```\n\n' +
				'Cada `.txt` tem no máximo N linhas (última fatia pode ter menos). ' +
				'Colunas iguais ao monólito. Import: processar fatia a fatia via MANIFEST.\n\n' +
				'Monólito flat: `KEY.TXT` na raiz de `out/` quando `--shard-lines=0`.\n\n'
			: '\n## Modo flat (default)\n\nUm arquivo `KEY.TXT` por dataset na raiz de `out/`.\n\n';
	var md =
		'# OSM geo extract — contrato de colunas (delimitador `@`)\n\n' +
		'Encoding: UTF-8. Sem header. Campos vazios = string vazia.\n' +
		'Campos com `@` ou quebras de linha são sanitizados no extract.\n' +
		shardNote +
		'## OSM_ESTADO.TXT\n\n' +
		'`osm_type@osm_id@uf@name@name_norm@lat@lng@lat_min@lat_max@lng_min@lng_max@admin_level@place`\n\n' +
		'## OSM_MUNICIPIO.TXT\n\n' +
		'`osm_type@osm_id@ibge@uf@name@name_norm@lat@lng@lat_min@lat_max@lng_min@lng_max@admin_level@place@source_tag`\n\n' +
		'## OSM_BAIRRO\n\n' +
		'`osm_type@osm_id@name@name_norm@uf@city@city_norm@ibge_hint@lat@lng@lat_min@lat_max@lng_min@lng_max@place`\n\n' +
		'## OSM_LOGRADOURO_{UF}\n\n' +
		'Um dataset por UF (`SP`, `RJ`, `MG`, `ES`, …) + residual `XX`.\n\n' +
		'`osm_id@name@name_norm@kind@uf@city@city_norm@suburb@suburb_norm@postcode@lat@lng@lat_min@lat_max@lng_min@lng_max@way_node_count@name_alt@name_alt_norm@osm_type`\n\n' +
		'- `kind`: valor de `highway` (`residential`, `primary`, …) ou, para área, ' +
		'`square` (`place=square`) / `park` (`leisure=park|garden`, `landuse=village_green`).\n' +
		'- `name_alt` / `name_alt_norm`: `alt_name`, `short_name`, `old_name`, `loc_name`, ' +
		'`name:pt-BR`, `official_name` — multi-valor separado por `;`.\n' +
		'- `osm_type`: `way` ou `node` (praça mapeada como ponto: bbox degenerada).\n\n' +
		'**Match kind-aware:** candidato `square`/`park` só vale para linha DNE com ' +
		'`TLO_TX` de área (Praça, Largo, Parque, Jardim, Vila, Área). Sem essa guarda, ' +
		'`Parque Villa-Lobos` casa com `Rua Villa-Lobos`.\n\n' +
		'## OSM_LOGRADOURO_GEOM_{UF} (opcional, `--way-geom`)\n\n' +
		'`osm_id@polyline@oneway`\n\n' +
		'Traçado da way + sentido, para desenhar / rotear sem casar nome em runtime.\n' +
		'Irmão de `OSM_LOGRADOURO_{UF}`: mesma UF, mesmo filtro, e todo `osm_id` ' +
		'daqui existe lá (o inverso não vale).\n\n' +
		'- `polyline`: pontos separados por `;`, cada um `lat,lng` em **unidades de ' +
		'1e-6 grau** (inteiros). O primeiro é absoluto, os demais são **deltas** do ' +
		'anterior: `-23552000,-46632000;-1000,-1000`.\n' +
		'- `oneway` (compacto): `0` ausente · `1` frente (`yes`) · `2` reverso (`-1`) · ' +
		'`3` mão dupla explícita (`no`). Outros valores OSM → `0`.\n' +
		'- Só entra way com **2+ pontos distintos** após o arredondamento. Praça ' +
		'mapeada como nó, way sem nó resolvido e way degenerada ficam de fora — ' +
		'toda linha do arquivo desenha.\n' +
		'- Way fechada (praça, parque) repete o primeiro ponto no fim: o anel fecha.\n' +
		'- **Não** há simplificação (Douglas-Peucker): a única perda é o ' +
		'arredondamento (~0,11 m).\n\n' +
		'## OSM_ADDR_POINT_{UF} (opcional)\n\n' +
		'`osm_id@lat@lng@street@street_norm@housenumber@city@suburb@postcode@name`\n\n' +
		'Docs: `docs/geo/extract-e-artefatos.md`\n';
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
 * @param {object|null} [options.ufAllow] map UF→true from parseUfFilter
 * @param {number} [options.waveNodes]
 * @param {number} [options.waveStreets]
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
	var shardLines = options.shardLines > 0 ? (options.shardLines | 0) : 0;
	var shardOnly = options.shardOnly || null;
	if (shardLines > 0 && (!shardOnly || !shardOnly.length)) {
		// default: fatia só volumes grandes
		shardOnly = ['OSM_LOGRADOURO', 'OSM_ADDR_POINT', 'OSM_BAIRRO'];
	}
	var ufAllow =
		options.ufAllow ||
		ufBr.parseUfFilter(options.uf || '', options.region || '') ||
		null;
	var waveNodes =
		options.waveNodes > 0 ? options.waveNodes | 0 : DEFAULT_WAVE_NODES;
	var waveStreets =
		options.waveStreets > 0 ? options.waveStreets | 0 : DEFAULT_WAVE_STREETS;
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
					'\nSoft-stop: parando no fim do blob atual (sem novas waves)' +
						(reason ? ' — ' + reason : '') +
						'.\nCtrl+C de novo força saída imediata (descarta pendentes).\n'
				);
			}
		}
		function requestHardStop() {
			hardStopRequested = true;
			softStopRequested = true;
			softStopDeadline = Date.now();
			if (!quiet) {
				console.error(
					'\nHard-stop: cancelando agora (próximo blob). Ctrl+C outra vez = exit forçado.\n'
				);
			}
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
			txtAt.wipeOsmOutputs(outDir);
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

		// append + shard is awkward (would need to reopen last partial shard);
		// force write mode when sharded.
		var writer = txtAt.createTxtAtWriter(outDir, {
			append: resume && startOffset > 0 && shardLines <= 0,
			shardLines: shardLines,
			shardOnly: shardOnly,
			sourcePbf: inputPath
		});
		writeReadmeColunas(outDir, { shardLines: shardLines });

		var ctx = {
			writer: writer,
			datasets: datasets,
			// Atalho para o caminho quente: `geomFromNodeIds` roda para toda way
			// do PBF e a checagem entra no laço de nós.
			wantGeom: wantDataset(datasets, 'geom'),
			twoPassStreets: twoPassStreets,
			twoPassGeometry: twoPassGeometry,
			ufAllow: ufAllow,
			pendingStreets: [],
			pendingAdminMuni: [],
			neededNodeIds: new Set(),
			neededAdminNodeIds: new Set(),
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
					municipioSkippedFilter: 0,
					bairro: 0,
					logradouro: 0,
					logradouroPending: 0,
					logradouroNoGeom: 0,
					logradouroGeom: 0,
					logradouroGeomVazio: 0,
					logradouroGeomPontos: 0,
					logradouroSkippedFilter: 0,
					streetNodesResolved: 0,
					streetWaves: 0,
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
			if (ufAllow) {
				console.error('Filtro UF: ' + ufBr.ufAllowList(ufAllow).join(','));
			}
			if (twoPassGeometry) {
				console.error(
					'Two-pass geometry: pass1 agenda (logradouro + admin_centre), pass2 resolve nós'
				);
				if (twoPassStreets) {
					console.error(
						'Waves: flush se neededNodes>=' +
							waveNodes +
							' ou pendingStreets>=' +
							waveStreets +
							' (evita Set max size)'
					);
				}
			}
			if (shardLines > 0) {
				console.error(
					'Shard: ' +
						shardLines +
						' linhas/arquivo → KEY/' +
						shardLines +
						'-linhas/000001.txt'
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
					' pend=' +
					ctx.pendingStreets.length +
					' need=' +
					ctx.neededNodeIds.size +
					' cache=' +
					ctx.nodeCache.size +
					'   '
			);
		}

		function shouldStop() {
			// Soft-stop: para no próximo check de blob (não espera 30s de trabalho extra).
			// softStopDeadline fica só como safety se algo travar sem checar.
			if (hardStopRequested) return true;
			if (!softStopRequested) return false;
			return true;
		}

		function markStopped() {
			stoppedEarly = true;
			stopReason = hardStopRequested ? 'hard-stop' : 'soft-stop';
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

		// Async: forEachBlobAsync cede o event loop entre blobs → Ctrl+C chega
		// no meio de waves/pass2 (o loop síncrono engolia o SIGINT até o fim).
		(async function runBody() {
			try {
				await pbfReader.forEachBlobAsync(
					inputPath,
					{ startOffset: startOffset, startBlobIndex: startBlobIndex },
					async function (blob) {
						if (shouldStop()) {
							markStopped();
							return 'stop';
						}
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
							// Nunca inicia wave depois de cancelar — era o que prendia o Ctrl+C.
							if (
								!shouldStop() &&
								shouldFlushStreetWave(ctx, waveNodes, waveStreets)
							) {
								var waveRes = await flushStreetWave(
									ctx,
									inputPath,
									quiet,
									shouldStop
								);
								if (waveRes.stopped) {
									markStopped();
									return 'stop';
								}
							}
						}
						printProgress(blob.blobIndex, false);
						if (shouldStop()) {
							markStopped();
							return 'stop';
						}
					}
				);

				printProgress(startBlobIndex + dataBlobs, true);
				if (!quiet) process.stdout.write('\n');

				// Pass 2 final: resto de logradouros + admin_centre de municípios
				var needPass2 =
					twoPassGeometry &&
					!stoppedEarly &&
					(ctx.pendingStreets.length > 0 || ctx.pendingAdminMuni.length > 0);
				if (needPass2) {
					var targetNeed =
						ctx.neededNodeIds.size + ctx.neededAdminNodeIds.size;
					if (!quiet) {
						console.error(
							'Pass 2/2: resolvendo ' +
								targetNeed +
								' nós (logradouro=' +
								ctx.pendingStreets.length +
								' admin_muni=' +
								ctx.pendingAdminMuni.length +
								(ctx.stats.streetWaves
									? ' waves_prev=' + ctx.stats.streetWaves
									: '') +
								')...'
						);
					}
					if (targetNeed > 0) {
						var lastPrint2 = 0;
						await pbfReader.forEachBlobAsync(
							inputPath,
							{ startOffset: 0, startBlobIndex: 0 },
							function (blob) {
								if (shouldStop()) {
									markStopped();
									return 'stop';
								}
								if (blob.type === 'OSMData') {
									var pb2 = osmformat.PrimitiveBlock.read(
										new Pbf(blob.data)
									);
									harvestStreetNodes(
										pb2,
										ctx.neededNodeIds,
										ctx.streetCoords,
										ctx.stats,
										ctx.neededAdminNodeIds
									);
									if (ctx.streetCoords.size >= targetNeed) return 'stop';
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
												targetNeed +
												'   '
										);
									}
								}
								if (shouldStop()) {
									markStopped();
									return 'stop';
								}
							}
						);
						if (!quiet) process.stdout.write('\n');
					}
					if (!stoppedEarly) {
						if (ctx.pendingAdminMuni.length) {
							emitPendingAdminMuni(ctx);
						}
						if (ctx.pendingStreets.length) {
							emitPendingStreets(ctx);
						}
						if (!quiet) {
							console.error(formatExtractSummary(ctx.stats));
						}
					}
				} else if (
					twoPassGeometry &&
					(ctx.pendingStreets.length || ctx.pendingAdminMuni.length) &&
					stoppedEarly
				) {
					if (!quiet) {
						console.error(
							'Cancelado com pendentes não gravados. Apague a pasta e recomece (não use --resume).'
						);
					}
				}

				await writer.flush();
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
					shardLines: shardLines,
					writerCounts: writer.counts,
					shards:
						typeof writer.getShardSnapshot === 'function'
							? writer.getShardSnapshot()
							: {},
					inputPath: inputPath,
					outDir: outDir,
					stoppedEarly: stoppedEarly,
					stopReason: stopReason,
					nodeCacheSize: ctx.nodeCache.size,
					streetCoordsSize: ctx.streetCoords.size,
					neededNodeIds: ctx.neededNodeIds.size,
					neededAdminNodeIds: ctx.neededAdminNodeIds.size,
					ufFilter: ufAllow ? ufBr.ufAllowList(ufAllow) : null,
					waveNodes: waveNodes,
					waveStreets: waveStreets
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
			} catch (err) {
				fail(err);
			}
		})();
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
		nodeCacheMax: DEFAULT_NODE_CACHE,
		shardLines: 0,
		shardOnly: null,
		uf: '',
		region: '',
		waveNodes: DEFAULT_WAVE_NODES,
		waveStreets: DEFAULT_WAVE_STREETS
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
		else if (a.indexOf('--shard-lines=') === 0)
			opts.shardLines = parseInt(a.slice(14), 10) || 0;
		else if (a === '--shard-lines') opts.shardLines = parseInt(args[++i], 10) || 0;
		else if (a.indexOf('--uf=') === 0) opts.uf = a.slice(5);
		else if (a === '--uf') opts.uf = args[++i] || '';
		else if (a.indexOf('--region=') === 0) opts.region = a.slice(9);
		else if (a === '--region') opts.region = args[++i] || '';
		else if (a.indexOf('--wave-nodes=') === 0)
			opts.waveNodes = parseInt(a.slice(13), 10) || DEFAULT_WAVE_NODES;
		else if (a === '--wave-nodes')
			opts.waveNodes = parseInt(args[++i], 10) || DEFAULT_WAVE_NODES;
		else if (a.indexOf('--wave-streets=') === 0)
			opts.waveStreets = parseInt(a.slice(15), 10) || DEFAULT_WAVE_STREETS;
		else if (a === '--wave-streets')
			opts.waveStreets = parseInt(args[++i], 10) || DEFAULT_WAVE_STREETS;
		else if (a.indexOf('--shard-datasets=') === 0) {
			opts.shardOnly = a
				.slice(17)
				.split(/[,+\s]+/)
				.filter(Boolean)
				.map(function (s) {
					s = s.trim();
					if (s.toLowerCase() === 'logradouro') return 'OSM_LOGRADOURO';
					if (s.toLowerCase() === 'bairro') return 'OSM_BAIRRO';
					if (s.toLowerCase() === 'addr' || s.toLowerCase() === 'addr_point')
						return 'OSM_ADDR_POINT';
					if (s.toLowerCase() === 'municipio') return 'OSM_MUNICIPIO';
					if (s.toLowerCase() === 'estado') return 'OSM_ESTADO';
					return s;
				});
		} else if (a === '--addr-points') {
			opts.datasets = opts.datasets || parseDatasets('all');
			opts.datasets.addr = true;
		} else if (a === '--way-geom') {
			opts.datasets = opts.datasets || parseDatasets('all');
			opts.datasets.geom = true;
			opts.datasets.logradouro = true;
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
	opts.ufAllow = ufBr.parseUfFilter(opts.uf, opts.region);
	return opts;
}

function main() {
	var opts = parseCli(process.argv);
	var softCount = 0;
	var forceExitTimer = null;
	var control = null;

	function onSig() {
		softCount++;
		// Sem control ainda (startup): sai na hora.
		if (!control) {
			process.exit(130);
			return;
		}
		if (softCount === 1) {
			control.softStop('SIGINT');
			return;
		}
		if (softCount === 2) {
			control.hardStop();
			// Se o loop ainda não cedeu, força exit em 2s (terminal fica aberto).
			if (!forceExitTimer) {
				forceExitTimer = setTimeout(function () {
					console.error('\nTimeout hard-stop — process.exit(130)\n');
					process.exit(130);
				}, 2000);
				if (forceExitTimer.unref) forceExitTimer.unref();
			}
			return;
		}
		// 3º Ctrl+C: exit imediato sem esperar flush
		process.exit(130);
	}
	process.on('SIGINT', onSig);
	process.on('SIGTERM', onSig);

	runExtractGeocode({
		inputPath: opts.inputPath,
		outDir: opts.outDir,
		datasets: opts.datasets,
		resume: opts.resume,
		quiet: opts.quiet,
		nodeCacheMax: opts.nodeCacheMax,
		shardLines: opts.shardLines,
		shardOnly: opts.shardOnly,
		ufAllow: opts.ufAllow,
		uf: opts.uf,
		region: opts.region,
		waveNodes: opts.waveNodes,
		waveStreets: opts.waveStreets,
		onControl: function (c) {
			control = c;
		}
	})
		.then(function () {
			if (forceExitTimer) clearTimeout(forceExitTimer);
			process.exit(0);
		})
		.catch(function (err) {
			if (forceExitTimer) clearTimeout(forceExitTimer);
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
	flushStreetWave: flushStreetWave,
	shouldFlushStreetWave: shouldFlushStreetWave,
	formatExtractSummary: formatExtractSummary,
	geomFromNodeIds: geomFromNodeIds,
	decodeWayRefs: decodeWayRefs,
	createNodeCache: createNodeCache,
	logradouroKind: logradouroKind,
	altNames: altNames,
	nameNorm: nameNorm,
	onewayCode: oneway.onewayCode,
	DEFAULT_WAVE_NODES: DEFAULT_WAVE_NODES,
	DEFAULT_WAVE_STREETS: DEFAULT_WAVE_STREETS
};

if (require.main === module) {
	main();
}
