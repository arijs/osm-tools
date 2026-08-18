'use strict';

/**
 * Geometria de vias para cruzamentos e densificação.
 * Ver docs/geo/via-cruzamentos-densificar.md.
 *
 * Coordenadas em graus [lat, lng]. Distâncias via aproximação local
 * (mesma fórmula de geo-cluster.distKm).
 */

var geo = require('./geo-cluster');

/** ~111 m — teto de seção contínua sem âncora. */
var MAX_SEG_KM_DEFAULT = 0.111;

/** Tol. para endpoint sobre segmento / endpoints coincidentes (~1,5 m). */
var TOUCH_KM_DEFAULT = 0.0015;

function distKm(lat1, lng1, lat2, lng2) {
	return geo.distKm(lat1, lng1, lat2, lng2);
}

function cellKey(lat, lng, cell) {
	return Math.floor(lat / cell) + ':' + Math.floor(lng / cell);
}

/**
 * Interseção própria de segmentos abertos (não só extremo compartilhado).
 * Devolve { lat, lng, t, u } com t,u ∈ (0,1), ou null.
 */
function segmentIntersection(a, b, c, d) {
	var ax = a[1], ay = a[0], bx = b[1], by = b[0];
	var cx = c[1], cy = c[0], dx = d[1], dy = d[0];
	var rX = bx - ax, rY = by - ay;
	var sX = dx - cx, sY = dy - cy;
	var den = rX * sY - rY * sX;
	if (Math.abs(den) < 1e-18) return null; // paralelo / colinear
	var qpx = cx - ax, qpy = cy - ay;
	var t = (qpx * sY - qpy * sX) / den;
	var u = (qpx * rY - qpy * rX) / den;
	// extremos exclusivos: toque em vértice trata-se em outro passo
	var eps = 1e-9;
	if (t <= eps || t >= 1 - eps || u <= eps || u >= 1 - eps) return null;
	return {
		lat: ay + t * rY,
		lng: ax + t * rX,
		t: t,
		u: u
	};
}

/**
 * Projeção do ponto P no segmento AB.
 * Devolve { lat, lng, t, distKm } com t ∈ [0,1].
 */
function projectPointOnSegment(p, a, b) {
	var ax = a[1], ay = a[0], bx = b[1], by = b[0];
	var px = p[1], py = p[0];
	var abx = bx - ax, aby = by - ay;
	var len2 = abx * abx + aby * aby;
	var t = 0;
	if (len2 > 0) {
		t = ((px - ax) * abx + (py - ay) * aby) / len2;
		if (t < 0) t = 0;
		else if (t > 1) t = 1;
	}
	var lat = ay + t * aby;
	var lng = ax + t * abx;
	return {
		lat: lat,
		lng: lng,
		t: t,
		distKm: distKm(py, px, lat, lng)
	};
}

/**
 * Posição ao longo da polyline (km desde o início) do ponto mais próximo.
 * Devolve { lat, lng, alongKm, segIndex, t, distKm }.
 */
function projectPointOnPolyline(pts, lat, lng) {
	if (!pts || pts.length < 1) return null;
	if (pts.length === 1) {
		return {
			lat: pts[0][0],
			lng: pts[0][1],
			alongKm: 0,
			segIndex: 0,
			t: 0,
			distKm: distKm(lat, lng, pts[0][0], pts[0][1])
		};
	}
	var best = null;
	var along = 0;
	for (var i = 0; i < pts.length - 1; i++) {
		var a = pts[i];
		var b = pts[i + 1];
		var segLen = distKm(a[0], a[1], b[0], b[1]);
		var pr = projectPointOnSegment([lat, lng], a, b);
		if (!best || pr.distKm < best.distKm) {
			best = {
				lat: pr.lat,
				lng: pr.lng,
				alongKm: along + pr.t * segLen,
				segIndex: i,
				t: pr.t,
				distKm: pr.distKm
			};
		}
		along += segLen;
	}
	return best;
}

/**
 * Pontos intermediários equidistantes entre A e B para seções ≤ maxSegKm.
 * Devolve lista de [lat,lng] (sem A nem B). Mínimo de seções: ceil(d/max).
 */
function densifyBetween(a, b, maxSegKm) {
	maxSegKm = maxSegKm || MAX_SEG_KM_DEFAULT;
	var d = distKm(a[0], a[1], b[0], b[1]);
	if (d <= maxSegKm || d < 1e-9) return [];
	var nSec = Math.ceil(d / maxSegKm);
	var out = [];
	for (var i = 1; i < nSec; i++) {
		var t = i / nSec;
		out.push([a[0] + t * (b[0] - a[0]), a[1] + t * (b[1] - a[1])]);
	}
	return out;
}

/**
 * Comprimento total da polyline (km).
 */
function polylineLengthKm(pts) {
	var s = 0;
	for (var i = 0; i < pts.length - 1; i++) {
		s += distKm(pts[i][0], pts[i][1], pts[i + 1][0], pts[i + 1][1]);
	}
	return s;
}

/**
 * Âncoras ao longo da via: vértices originais + pontos extras (cruzamentos).
 * extras = [{ lat, lng }, …]. Ordena por alongKm; funde quase-duplicatas.
 * Devolve [{ lat, lng, alongKm, origem }], origem = 'vertice' | (do extra).
 */
function buildAnchors(pts, extras, mergeKm) {
	mergeKm = mergeKm == null ? 0.0005 : mergeKm; // ~0,5 m
	var raw = [];
	var along = 0;
	for (var i = 0; i < pts.length; i++) {
		if (i > 0) along += distKm(pts[i - 1][0], pts[i - 1][1], pts[i][0], pts[i][1]);
		raw.push({ lat: pts[i][0], lng: pts[i][1], alongKm: along, origem: 'vertice' });
	}
	if (extras) {
		for (var e = 0; e < extras.length; e++) {
			var ex = extras[e];
			var pr = projectPointOnPolyline(pts, ex.lat, ex.lng);
			if (!pr) continue;
			raw.push({
				lat: pr.lat,
				lng: pr.lng,
				alongKm: pr.alongKm,
				origem: ex.origem || 'cruzamento'
			});
		}
	}
	raw.sort(function (x, y) {
		return x.alongKm - y.alongKm || x.lat - y.lat || x.lng - y.lng;
	});
	var out = [];
	for (var j = 0; j < raw.length; j++) {
		var cur = raw[j];
		if (
			out.length &&
			Math.abs(out[out.length - 1].alongKm - cur.alongKm) <= mergeKm
		) {
			// preferir origem mais específica que vertice se colidir
			if (out[out.length - 1].origem === 'vertice' && cur.origem !== 'vertice') {
				out[out.length - 1].origem = cur.origem;
			}
			continue;
		}
		out.push(cur);
	}
	return out;
}

/**
 * Emite pontos: âncoras + amostras densificadas entre elas.
 * Cada item: { lat, lng, origem, alongKm }.
 */
function emitDensifiedPoints(pts, extras, maxSegKm) {
	var anchors = buildAnchors(pts, extras);
	var out = [];
	for (var i = 0; i < anchors.length; i++) {
		var a = anchors[i];
		out.push({
			lat: a.lat,
			lng: a.lng,
			origem: a.origem,
			alongKm: a.alongKm
		});
		if (i + 1 >= anchors.length) break;
		var b = anchors[i + 1];
		var mid = densifyBetween([a.lat, a.lng], [b.lat, b.lng], maxSegKm);
		var gap = b.alongKm - a.alongKm;
		for (var m = 0; m < mid.length; m++) {
			var t = (m + 1) / (mid.length + 1);
			out.push({
				lat: mid[m][0],
				lng: mid[m][1],
				origem: 'amostra',
				alongKm: a.alongKm + t * gap
			});
		}
	}
	return out;
}

/**
 * Arredonda lat/lng para chave estável (1e-6 ° ≈ 0,11 m).
 */
function pointKey(lat, lng) {
	return Math.round(lat * 1e6) + ',' + Math.round(lng * 1e6);
}

/**
 * Células cobertas pelo bbox do segmento (expansão 1 célula).
 */
function segmentCellKeys(a, b, cell) {
	var latMin = Math.min(a[0], b[0]);
	var latMax = Math.max(a[0], b[0]);
	var lngMin = Math.min(a[1], b[1]);
	var lngMax = Math.max(a[1], b[1]);
	var i0 = Math.floor(latMin / cell) - 1;
	var i1 = Math.floor(latMax / cell) + 1;
	var j0 = Math.floor(lngMin / cell) - 1;
	var j1 = Math.floor(lngMax / cell) + 1;
	var keys = [];
	for (var i = i0; i <= i1; i++) {
		for (var j = j0; j <= j1; j++) keys.push(i + ':' + j);
	}
	return keys;
}

module.exports = {
	MAX_SEG_KM_DEFAULT: MAX_SEG_KM_DEFAULT,
	TOUCH_KM_DEFAULT: TOUCH_KM_DEFAULT,
	distKm: distKm,
	cellKey: cellKey,
	segmentIntersection: segmentIntersection,
	projectPointOnSegment: projectPointOnSegment,
	projectPointOnPolyline: projectPointOnPolyline,
	densifyBetween: densifyBetween,
	polylineLengthKm: polylineLengthKm,
	buildAnchors: buildAnchors,
	emitDensifiedPoints: emitDensifiedPoints,
	pointKey: pointKey,
	segmentCellKeys: segmentCellKeys
};
