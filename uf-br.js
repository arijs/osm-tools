'use strict';

/**
 * Brazilian UF helpers for OSM extract (Sudeste focus, extensible).
 */

/** First two digits of IBGE município → UF */
var IBGE_UF = {
	'11': 'RO',
	'12': 'AC',
	'13': 'AM',
	'14': 'RR',
	'15': 'PA',
	'16': 'AP',
	'17': 'TO',
	'21': 'MA',
	'22': 'PI',
	'23': 'CE',
	'24': 'RN',
	'25': 'PB',
	'26': 'PE',
	'27': 'AL',
	'28': 'SE',
	'29': 'BA',
	'31': 'MG',
	'32': 'ES',
	'33': 'RJ',
	'35': 'SP',
	'41': 'PR',
	'42': 'SC',
	'43': 'RS',
	'50': 'MS',
	'51': 'MT',
	'52': 'GO',
	'53': 'DF'
};

/** Approximate bboxes for Sudeste UFs (degrees). Borders may overlap. */
var UF_BBOX_SE = {
	SP: { latMin: -25.35, latMax: -19.75, lngMin: -53.15, lngMax: -44.05 },
	RJ: { latMin: -23.4, latMax: -20.7, lngMin: -44.9, lngMax: -40.85 },
	MG: { latMin: -22.95, latMax: -14.2, lngMin: -51.05, lngMax: -39.75 },
	ES: { latMin: -21.35, latMax: -17.85, lngMin: -41.9, lngMax: -39.65 }
};

var NOME_UF = {
	acre: 'AC',
	alagoas: 'AL',
	amapa: 'AP',
	amazonas: 'AM',
	bahia: 'BA',
	ceara: 'CE',
	'distrito federal': 'DF',
	'espirito santo': 'ES',
	goias: 'GO',
	maranhao: 'MA',
	'mato grosso': 'MT',
	'mato grosso do sul': 'MS',
	'minas gerais': 'MG',
	para: 'PA',
	paraiba: 'PB',
	parana: 'PR',
	pernambuco: 'PE',
	piaui: 'PI',
	'rio de janeiro': 'RJ',
	'rio grande do norte': 'RN',
	'rio grande do sul': 'RS',
	rondonia: 'RO',
	roraima: 'RR',
	'santa catarina': 'SC',
	'sao paulo': 'SP',
	sergipe: 'SE',
	tocantins: 'TO'
};

function ufFromIbge(ibge) {
	if (ibge == null || ibge === '') return '';
	var s = String(ibge).replace(/\D/g, '');
	if (s.length < 2) return '';
	return IBGE_UF[s.slice(0, 2)] || '';
}

function normalizeUfToken(raw) {
	if (raw == null || raw === '') return '';
	var s = String(raw).trim();
	if (!s) return '';
	// ISO3166-2:BR-SP or BR-SP
	var m = s.match(/^(?:ISO3166-2:)?BR-([A-Za-z]{2})$/i);
	if (m) return m[1].toUpperCase();
	if (/^[A-Za-z]{2}$/.test(s)) return s.toUpperCase();
	var lower = s
		.toLowerCase()
		.normalize('NFD')
		.replace(/[\u0300-\u036f]/g, '');
	if (NOME_UF[lower]) return NOME_UF[lower];
	return '';
}

/**
 * Resolve UF from OSM tags (order: ISO, addr:state, is_in:state, ref, name for states).
 */
function ufFromTags(tags) {
	if (!tags) return '';
	var keys = [
		'ISO3166-2',
		'addr:state',
		'is_in:state',
		'is_in:state_code',
		'state',
		'ref'
	];
	for (var i = 0; i < keys.length; i++) {
		var u = normalizeUfToken(tags[keys[i]]);
		if (u && u.length === 2) return u;
	}
	if (tags.place === 'state' || tags.admin_level === '4') {
		u = normalizeUfToken(tags.name) || normalizeUfToken(tags['name:pt']);
		if (u) return u;
	}
	return '';
}

function pointInBbox(lat, lon, box) {
	return (
		lat >= box.latMin &&
		lat <= box.latMax &&
		lon >= box.lngMin &&
		lon <= box.lngMax
	);
}

/**
 * Heuristic UF from a point using Sudeste bboxes.
 * Prefer smaller boxes when multiple match (ES/RJ over MG/SP when nested edge).
 */
function ufFromPoint(lat, lon, bboxes) {
	if (lat == null || lon == null || !isFinite(lat) || !isFinite(lon)) return '';
	bboxes = bboxes || UF_BBOX_SE;
	var hits = [];
	for (var uf in bboxes) {
		if (!Object.prototype.hasOwnProperty.call(bboxes, uf)) continue;
		var b = bboxes[uf];
		if (pointInBbox(lat, lon, b)) {
			var area = (b.latMax - b.latMin) * (b.lngMax - b.lngMin);
			hits.push({ uf: uf, area: area });
		}
	}
	if (!hits.length) return '';
	hits.sort(function (a, b) {
		return a.area - b.area;
	});
	return hits[0].uf;
}

/**
 * UF from feature: tags → IBGE → point/bbox centroid.
 */
function resolveUf(options) {
	options = options || {};
	var u = ufFromTags(options.tags);
	if (u) return u;
	if (options.ibge) {
		u = ufFromIbge(options.ibge);
		if (u) return u;
	}
	var lat = options.lat;
	var lon = options.lng != null ? options.lng : options.lon;
	if (lat == null && options.lat_min != null && options.lat_max != null) {
		lat = (Number(options.lat_min) + Number(options.lat_max)) / 2;
		lon = (Number(options.lng_min) + Number(options.lng_max)) / 2;
	}
	u = ufFromPoint(lat, lon, options.bboxes);
	return u || 'XX';
}

function extractIbge(tags) {
	if (!tags) return { ibge: '', source_tag: '' };
	if (tags['IBGE:GEOCODIGO']) {
		return { ibge: String(tags['IBGE:GEOCODIGO']).trim(), source_tag: 'IBGE:GEOCODIGO' };
	}
	if (tags['ref:IBGE']) {
		return { ibge: String(tags['ref:IBGE']).trim(), source_tag: 'ref:IBGE' };
	}
	if (tags['IBGE']) {
		return { ibge: String(tags['IBGE']).trim(), source_tag: 'IBGE' };
	}
	return { ibge: '', source_tag: '' };
}

module.exports = {
	IBGE_UF: IBGE_UF,
	UF_BBOX_SE: UF_BBOX_SE,
	ufFromIbge: ufFromIbge,
	normalizeUfToken: normalizeUfToken,
	ufFromTags: ufFromTags,
	ufFromPoint: ufFromPoint,
	resolveUf: resolveUf,
	extractIbge: extractIbge
};
