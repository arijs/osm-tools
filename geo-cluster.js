'use strict';

/**
 * Clusterização de features por proximidade e footprint municipal.
 *
 * Ver docs/geo/dne-geo-join.md §Fase 1 e §Fase 3.
 *
 * Feature = { lat, lng, latMin, latMax, lngMin, lngMax, n, kind }
 * (`n` = way_node_count, peso do centroide; `kind` = coluna 4 do extract)
 */

var CELL_CLUSTER = 0.02;   // ~2,2 km — separa vias homônimas dentro do município
var CELL_FOOTPRINT = 0.01; // ~1,1 km — resolução da pegada municipal

function cellKey(lat, lng, cell) {
	return Math.floor(lat / cell) + ':' + Math.floor(lng / cell);
}

/**
 * Agrupa features por proximidade: células ocupadas unidas por vizinhança 8.
 *
 * Sem isso, agregar todas as ways de mesmo nome num município dá bbox inútil —
 * `Rua Augusta` tem 33 ways em 4 lugares da capital: 28 × 28 km agregado,
 * 2 × 3 km no cluster certo.
 *
 * @param {Array} feats
 * @param {number} [cell]
 * @returns {Array<Array>} grupos de features
 */
function clusterFeatures(feats, cell) {
	cell = cell || CELL_CLUSTER;
	if (feats.length <= 1) return feats.length ? [feats.slice()] : [];

	var buckets = new Map();
	for (var i = 0; i < feats.length; i++) {
		var k = cellKey(feats[i].lat, feats[i].lng, cell);
		var b = buckets.get(k);
		if (b) b.push(feats[i]);
		else buckets.set(k, [feats[i]]);
	}

	// union-find sobre as células ocupadas
	var parent = new Map();
	buckets.forEach(function (_v, k) { parent.set(k, k); });
	function find(k) {
		var r = k;
		while (parent.get(r) !== r) r = parent.get(r);
		while (parent.get(k) !== r) { var nx = parent.get(k); parent.set(k, r); k = nx; }
		return r;
	}
	buckets.forEach(function (_v, k) {
		var p = k.split(':');
		var a = +p[0], b2 = +p[1];
		for (var da = -1; da <= 1; da++) {
			for (var db = -1; db <= 1; db++) {
				if (da === 0 && db === 0) continue;
				var v = (a + da) + ':' + (b2 + db);
				if (!parent.has(v)) continue;
				var ra = find(k), rv = find(v);
				if (ra !== rv) parent.set(ra, rv);
			}
		}
	});

	var groups = new Map();
	buckets.forEach(function (arr, k) {
		var r = find(k);
		var g = groups.get(r);
		if (g) { for (var j = 0; j < arr.length; j++) g.push(arr[j]); }
		else groups.set(r, arr.slice());
	});
	return Array.from(groups.values());
}

/**
 * Geometria agregada de um grupo: bbox união, centroide ponderado por `n`.
 * `kinds` fica ordenado para a saída ser determinística.
 */
function aggregate(feats) {
	var w = 0, sLat = 0, sLng = 0;
	var latMin = Infinity, latMax = -Infinity, lngMin = Infinity, lngMax = -Infinity;
	var kinds = Object.create(null);
	for (var i = 0; i < feats.length; i++) {
		var f = feats[i];
		var p = f.n > 0 ? f.n : 1;
		w += p;
		sLat += f.lat * p;
		sLng += f.lng * p;
		if (f.latMin < latMin) latMin = f.latMin;
		if (f.latMax > latMax) latMax = f.latMax;
		if (f.lngMin < lngMin) lngMin = f.lngMin;
		if (f.lngMax > lngMax) lngMax = f.lngMax;
		if (f.kind) kinds[f.kind] = 1;
	}
	return {
		lat: sLat / w,
		lng: sLng / w,
		latMin: latMin,
		latMax: latMax,
		lngMin: lngMin,
		lngMax: lngMax,
		ways: feats.length,
		weight: w,
		kinds: Object.keys(kinds).sort()
	};
}

/**
 * Pegada do município: grade de células ocupadas pelos pontos-âncora, dilatada
 * em `dilate` células para cobrir borda.
 *
 * Não é bbox nem polígono: é o formato real, que aguenta município alongado ou
 * não-convexo (São Paulo vs. Taboão da Serra não se resolvem por retângulo).
 */
function buildFootprint(points, cell, dilate) {
	cell = cell || CELL_FOOTPRINT;
	dilate = dilate == null ? 1 : dilate;
	var core = new Set();
	for (var i = 0; i < points.length; i++) {
		core.add(cellKey(points[i].lat, points[i].lng, cell));
	}
	if (dilate <= 0) return { cell: cell, cells: core, size: core.size };

	var out = new Set();
	core.forEach(function (k) {
		var p = k.split(':');
		var a = +p[0], b = +p[1];
		for (var da = -dilate; da <= dilate; da++) {
			for (var db = -dilate; db <= dilate; db++) {
				out.add((a + da) + ':' + (b + db));
			}
		}
	});
	return { cell: cell, cells: out, size: core.size };
}

function inFootprint(fp, lat, lng) {
	if (!fp) return false;
	return fp.cells.has(cellKey(lat, lng, fp.cell));
}

/** Fração das features do grupo que cai dentro da pegada (0..1). */
function footprintOverlap(fp, feats) {
	if (!fp || !feats.length) return 0;
	var hit = 0;
	for (var i = 0; i < feats.length; i++) {
		if (inFootprint(fp, feats[i].lat, feats[i].lng)) hit++;
	}
	return hit / feats.length;
}

/** Distância planar aproximada em km (suficiente para desempate local). */
function distKm(lat1, lng1, lat2, lng2) {
	var dLat = (lat1 - lat2) * 111;
	var dLng = (lng1 - lng2) * 111 * Math.cos((lat1 + lat2) / 2 * Math.PI / 180);
	return Math.sqrt(dLat * dLat + dLng * dLng);
}

module.exports = {
	clusterFeatures: clusterFeatures,
	aggregate: aggregate,
	buildFootprint: buildFootprint,
	inFootprint: inFootprint,
	footprintOverlap: footprintOverlap,
	distKm: distKm,
	CELL_CLUSTER: CELL_CLUSTER,
	CELL_FOOTPRINT: CELL_FOOTPRINT
};
