'use strict';

/**
 * Brazilian UF helpers for OSM extract (nacional, extensível).
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

/**
 * Approximate UF bboxes (degrees). Borders overlap on purpose —
 * ufFromPoint picks the smaller box when multiple match.
 * Fontes: limites administrativos aproximados (não oficiais IBGE).
 */
var UF_BBOX = {
	AC: { latMin: -11.15, latMax: -7.11, lngMin: -73.99, lngMax: -66.62 },
	AL: { latMin: -10.5, latMax: -8.81, lngMin: -38.24, lngMax: -35.15 },
	AP: { latMin: -1.24, latMax: 4.44, lngMin: -54.88, lngMax: -49.86 },
	AM: { latMin: -9.82, latMax: 2.25, lngMin: -73.8, lngMax: -56.1 },
	BA: { latMin: -18.35, latMax: -8.53, lngMin: -46.62, lngMax: -37.34 },
	CE: { latMin: -7.86, latMax: -2.78, lngMin: -41.42, lngMax: -37.25 },
	DF: { latMin: -16.05, latMax: -15.5, lngMin: -48.29, lngMax: -47.31 },
	ES: { latMin: -21.35, latMax: -17.85, lngMin: -41.9, lngMax: -39.65 },
	GO: { latMin: -19.5, latMax: -12.4, lngMin: -53.25, lngMax: -45.9 },
	MA: { latMin: -10.26, latMax: -1.05, lngMin: -48.76, lngMax: -41.8 },
	MT: { latMin: -18.04, latMax: -7.35, lngMin: -61.63, lngMax: -50.22 },
	MS: { latMin: -24.07, latMax: -17.17, lngMin: -58.17, lngMax: -50.92 },
	MG: { latMin: -22.95, latMax: -14.2, lngMin: -51.05, lngMax: -39.75 },
	PA: { latMin: -9.84, latMax: 2.59, lngMin: -58.9, lngMax: -46.03 },
	PB: { latMin: -8.3, latMax: -6.03, lngMin: -38.77, lngMax: -34.79 },
	PR: { latMin: -26.72, latMax: -22.52, lngMin: -54.62, lngMax: -48.02 },
	PE: { latMin: -9.48, latMax: -7.15, lngMin: -41.36, lngMax: -34.81 },
	PI: { latMin: -10.93, latMax: -2.74, lngMin: -45.99, lngMax: -40.37 },
	RJ: { latMin: -23.4, latMax: -20.7, lngMin: -44.9, lngMax: -40.85 },
	RN: { latMin: -6.98, latMax: -4.83, lngMin: -38.58, lngMax: -34.97 },
	RS: { latMin: -33.75, latMax: -27.08, lngMin: -57.65, lngMax: -49.69 },
	RO: { latMin: -13.69, latMax: -7.97, lngMin: -66.81, lngMax: -59.77 },
	RR: { latMin: -1.58, latMax: 5.27, lngMin: -64.82, lngMax: -58.89 },
	SC: { latMin: -29.35, latMax: -25.96, lngMin: -53.84, lngMax: -48.33 },
	SP: { latMin: -25.35, latMax: -19.75, lngMin: -53.15, lngMax: -44.05 },
	SE: { latMin: -11.57, latMax: -9.52, lngMin: -38.24, lngMax: -36.39 },
	TO: { latMin: -13.47, latMax: -5.17, lngMin: -50.74, lngMax: -45.7 }
};

/** Alias histórico (Sudeste) — mesmos retângulos de UF_BBOX. */
var UF_BBOX_SE = {
	SP: UF_BBOX.SP,
	RJ: UF_BBOX.RJ,
	MG: UF_BBOX.MG,
	ES: UF_BBOX.ES
};

/** Regiões IBGE → lista de UFs (para --region=). */
var REGIOES = {
	norte: ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'],
	nordeste: ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'],
	'centro-oeste': ['DF', 'GO', 'MT', 'MS'],
	centrooeste: ['DF', 'GO', 'MT', 'MS'],
	co: ['DF', 'GO', 'MT', 'MS'],
	sudeste: ['ES', 'MG', 'RJ', 'SP'],
	se: ['ES', 'MG', 'RJ', 'SP'],
	sul: ['PR', 'RS', 'SC']
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
 * Heuristic UF from a point using UF bboxes (default: Brasil inteiro).
 * Prefer smaller boxes when multiple match (ES/RJ over MG/SP when nested edge).
 */
function ufFromPoint(lat, lon, bboxes) {
	if (lat == null || lon == null || !isFinite(lat) || !isFinite(lon)) return '';
	bboxes = bboxes || UF_BBOX;
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

/**
 * Expand --region / --uf CLI tokens into a map of UF codes.
 * Accepts: "SP,RJ", "sudeste", "norte+nordeste", "centro-oeste".
 * Returns null if empty (sem filtro).
 */
function parseUfFilter(ufStr, regionStr) {
	var out = Object.create(null);
	var n = 0;

	function addUf(u) {
		if (!u || !isKnownUf(u) || out[u]) return;
		out[u] = true;
		n++;
	}

	function addToken(tok) {
		if (!tok) return;
		tok = String(tok).trim().toLowerCase();
		if (!tok) return;
		var regKey = tok
			.normalize('NFD')
			.replace(/[\u0300-\u036f]/g, '')
			.replace(/\s+/g, '-');
		if (REGIOES[regKey]) {
			REGIOES[regKey].forEach(addUf);
			return;
		}
		addUf(normalizeUfToken(tok));
	}

	function splitList(s) {
		if (!s) return;
		String(s)
			.split(/[,+\s]+/)
			.forEach(addToken);
	}

	splitList(ufStr);
	splitList(regionStr);

	if (!n) return null;
	return out;
}

function isKnownUf(u) {
	if (!u || u.length !== 2) return false;
	for (var k in IBGE_UF) {
		if (IBGE_UF[k] === u) return true;
	}
	return false;
}

/** Bboxes only for UFs in the allow-map (or all if null). */
function bboxesForFilter(ufAllow) {
	if (!ufAllow) return UF_BBOX;
	var out = {};
	for (var uf in ufAllow) {
		if (ufAllow[uf] && UF_BBOX[uf]) out[uf] = UF_BBOX[uf];
	}
	return out;
}

/**
 * Whether a resolved UF / point should be kept under an optional filter.
 * XX só passa se o ponto cair em algum bbox permitido.
 */
function passesUfFilter(ufAllow, uf, lat, lon) {
	if (!ufAllow) return true;
	if (uf && uf !== 'XX' && ufAllow[uf]) return true;
	if (lat != null && lon != null && isFinite(lat) && isFinite(lon)) {
		var boxes = bboxesForFilter(ufAllow);
		for (var k in boxes) {
			if (pointInBbox(lat, lon, boxes[k])) return true;
		}
	}
	return false;
}

/** Early reject when tags already resolve to a disallowed UF. */
function tagsDisallowedByFilter(ufAllow, tags, ibge) {
	if (!ufAllow) return false;
	var u = ufFromTags(tags);
	if (!u && ibge) u = ufFromIbge(ibge);
	if (u && u !== 'XX' && !ufAllow[u]) return true;
	return false;
}

function ufAllowList(ufAllow) {
	if (!ufAllow) return [];
	return Object.keys(ufAllow).filter(function (k) {
		return ufAllow[k];
	}).sort();
}

module.exports = {
	IBGE_UF: IBGE_UF,
	UF_BBOX: UF_BBOX,
	UF_BBOX_SE: UF_BBOX_SE,
	REGIOES: REGIOES,
	ufFromIbge: ufFromIbge,
	normalizeUfToken: normalizeUfToken,
	ufFromTags: ufFromTags,
	ufFromPoint: ufFromPoint,
	resolveUf: resolveUf,
	extractIbge: extractIbge,
	pointInBbox: pointInBbox,
	parseUfFilter: parseUfFilter,
	bboxesForFilter: bboxesForFilter,
	passesUfFilter: passesUfFilter,
	tagsDisallowedByFilter: tagsDisallowedByFilter,
	ufAllowList: ufAllowList,
	isKnownUf: isKnownUf
};
