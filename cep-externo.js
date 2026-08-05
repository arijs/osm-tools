'use strict';

/**
 * Cache de CEP externo no formato DNE (UTF-8, @, sem header).
 * Spec: docs/geo/cep-externo.md
 *
 * Chave = CEP 8 dígitos. Uma linha por CEP consultado (sucesso ou falha).
 */

var fs = require('fs');
var path = require('path');
var readline = require('readline');

var COLS = 13;
var FONTE_DEFAULT = 'awesomeapi';

/** Normaliza para 8 dígitos ou string vazia. */
function digitsCep(c) {
	var d = String(c == null ? '' : c).replace(/\D/g, '');
	if (d.length > 8) d = d.slice(0, 8);
	if (d.length > 0 && d.length < 8) d = d.padStart(8, '0');
	return d.length === 8 ? d : '';
}

function numOrEmpty(v) {
	if (v === undefined || v === null || v === '') return '';
	var n = Number(v);
	return isFinite(n) ? String(v) : '';
}

function sanitizeField(s) {
	if (s == null || s === '') return '';
	return String(s).replace(/@/g, ' ').replace(/[\r\n]+/g, ' ').trim();
}

/**
 * @param {object} r
 * @returns {string} linha sem \n
 */
function formatRow(r) {
	var cep = digitsCep(r.cep);
	if (!cep) throw new Error('CEP inválido: ' + r.cep);
	var status = r.status || 'error';
	var lat = status === 'ok' ? numOrEmpty(r.lat) : '';
	var lng = status === 'ok' ? numOrEmpty(r.lng) : '';
	// empty_coords: 200 sem ponto — lat/lng vazios de propósito
	if (status === 'empty_coords') {
		lat = '';
		lng = '';
	}
	return [
		cep,
		String(r.http_status != null ? r.http_status : 0),
		status,
		lat,
		lng,
		sanitizeField(r.api_city),
		sanitizeField(r.api_state),
		sanitizeField(r.api_district),
		sanitizeField(r.api_ibge),
		sanitizeField(r.api_address),
		sanitizeField(r.api_address_type),
		sanitizeField(r.consultado_em || new Date().toISOString()),
		sanitizeField(r.fonte || FONTE_DEFAULT)
	].join('@');
}

/**
 * @param {string} line
 * @returns {object|null}
 */
function parseRow(line) {
	if (!line) return null;
	var p = line.split('@');
	if (p.length < 5) return null;
	var cep = digitsCep(p[0]);
	if (!cep) return null;
	// tolera linhas antigas com menos colunas
	while (p.length < COLS) p.push('');
	var status = p[2] || 'error';
	var lat = p[3] === '' ? null : Number(p[3]);
	var lng = p[4] === '' ? null : Number(p[4]);
	if (lat !== null && !isFinite(lat)) lat = null;
	if (lng !== null && !isFinite(lng)) lng = null;
	return {
		cep: cep,
		http_status: Number(p[1]) || 0,
		status: status,
		lat: lat,
		lng: lng,
		api_city: p[5] || '',
		api_state: p[6] || '',
		api_district: p[7] || '',
		api_ibge: p[8] || '',
		api_address: p[9] || '',
		api_address_type: p[10] || '',
		consultado_em: p[11] || '',
		fonte: p[12] || FONTE_DEFAULT,
		_raw: line
	};
}

/**
 * Classifica resposta HTTP + body da AwesomeAPI.
 * @returns {object} campos para formatRow
 */
function fromAwesomeResponse(cep, http, body, fonte) {
	var now = new Date().toISOString();
	var base = {
		cep: digitsCep(cep),
		http_status: http,
		consultado_em: now,
		fonte: fonte || FONTE_DEFAULT,
		api_city: '',
		api_state: '',
		api_district: '',
		api_ibge: '',
		api_address: '',
		api_address_type: '',
		lat: null,
		lng: null,
		status: 'error'
	};
	if (http === 404) {
		base.status = 'not_found';
		return base;
	}
	if (http === 400) {
		base.status = 'invalid';
		return base;
	}
	if (http !== 200 || !body || typeof body !== 'object') {
		base.status = 'error';
		return base;
	}
	base.api_city = body.city || '';
	base.api_state = body.state || '';
	base.api_district = body.district || '';
	base.api_ibge = body.city_ibge != null ? String(body.city_ibge) : '';
	base.api_address = body.address || '';
	base.api_address_type = body.address_type || '';
	var lat = body.lat != null && body.lat !== '' ? Number(body.lat) : NaN;
	var lng = body.lng != null && body.lng !== '' ? Number(body.lng) : NaN;
	if (isFinite(lat) && isFinite(lng)) {
		base.lat = lat;
		base.lng = lng;
		base.status = 'ok';
	} else {
		base.status = 'empty_coords';
	}
	return base;
}

/**
 * Carrega o TXT em Map(cep → row).
 * @param {string} file
 * @returns {Promise<Map<string, object>>}
 */
function loadCache(file) {
	return new Promise(function (resolve, reject) {
		var map = new Map();
		if (!file || !fs.existsSync(file)) {
			resolve(map);
			return;
		}
		var rl = readline.createInterface({
			input: fs.createReadStream(file, { encoding: 'utf8' }),
			crlfDelay: Infinity
		});
		rl.on('line', function (line) {
			var row = parseRow(line);
			if (row) map.set(row.cep, row);
		});
		rl.on('close', function () { resolve(map); });
		rl.on('error', reject);
	});
}

/**
 * Escreve o Map inteiro ordenado por CEP (estável, mesclável).
 * @param {string} file
 * @param {Map<string, object>} map
 */
function writeCache(file, map) {
	fs.mkdirSync(path.dirname(file) || '.', { recursive: true });
	var keys = Array.from(map.keys()).sort();
	var fd = fs.openSync(file, 'w');
	try {
		for (var i = 0; i < keys.length; i++) {
			var row = map.get(keys[i]);
			var line = row._raw && row._raw.split('@').length >= COLS
				? row._raw
				: formatRow(row);
			// se status/lat mudaram, reformatar
			if (!row._raw || row._dirty) line = formatRow(row);
			fs.writeSync(fd, line + '\n');
		}
	} finally {
		fs.closeSync(fd);
	}
}

/**
 * Append de linhas novas (sem reordenar). Preferir writeCache após lote.
 * @param {string} file
 * @param {object[]} rows
 */
function appendRows(file, rows) {
	fs.mkdirSync(path.dirname(file) || '.', { recursive: true });
	var chunk = rows.map(formatRow).join('\n') + (rows.length ? '\n' : '');
	fs.appendFileSync(file, chunk, 'utf8');
}

/**
 * Mescla rows no map e grava ordenado.
 * @param {string} file
 * @param {Map<string, object>} map
 * @param {object[]} rows
 */
function mergeAndSave(file, map, rows) {
	for (var i = 0; i < rows.length; i++) {
		var r = rows[i];
		var cep = digitsCep(r.cep);
		if (!cep) continue;
		r.cep = cep;
		r._dirty = true;
		delete r._raw;
		map.set(cep, r);
	}
	writeCache(file, map);
}

// ---------------------------------------------------------------- multi-UF

/** UFs com arquivo `CEP_EXTERNO_{UF}.TXT` (2 letras). Residual sem estado API → XX. */
var UF_RE = /^[A-Z]{2}$/;

function normalizeUfToken(uf) {
	var u = String(uf == null ? '' : uf).trim().toUpperCase();
	return UF_RE.test(u) ? u : '';
}

/** UF do registro (api_state) ou XX se vazio/inválido. */
function rowUf(row) {
	var u = normalizeUfToken(row && row.api_state);
	return u || 'XX';
}

/**
 * Caminho do cache de uma UF: `dir/CEP_EXTERNO_SP.TXT`.
 * @param {string} dir
 * @param {string} uf
 */
function cachePathForUf(dir, uf) {
	var u = normalizeUfToken(uf) || 'XX';
	return path.join(dir, 'CEP_EXTERNO_' + u + '.TXT');
}

/**
 * Lista arquivos de cache em `dir`:
 * - monólito `CEP_EXTERNO.TXT` (legado)
 * - `CEP_EXTERNO_{UF}.TXT` (e XX)
 * @param {string} dir
 * @param {string[]|null} [ufs] se definido, só essas UFs (+ monólito se existir)
 * @returns {{ path: string, uf: string|null, monolithic: boolean }[]}
 */
function listCacheFiles(dir, ufs) {
	var out = [];
	if (!dir || !fs.existsSync(dir)) return out;
	var allow = null;
	if (ufs && ufs.length) {
		allow = Object.create(null);
		for (var i = 0; i < ufs.length; i++) {
			var u = normalizeUfToken(ufs[i]);
			if (u) allow[u] = true;
		}
		allow.XX = true;
	}
	var names = fs.readdirSync(dir);
	for (var n = 0; n < names.length; n++) {
		var name = names[n];
		if (!/^CEP_EXTERNO/i.test(name) || !/\.TXT$/i.test(name)) continue;
		if (/RELATORIO/i.test(name)) continue;
		var full = path.join(dir, name);
		if (!fs.statSync(full).isFile()) continue;
		var base = name.replace(/\.TXT$/i, '');
		if (/^CEP_EXTERNO$/i.test(base)) {
			out.push({ path: full, uf: null, monolithic: true });
			continue;
		}
		var m = base.match(/^CEP_EXTERNO_([A-Za-z]{2})$/i);
		if (!m) continue;
		var uf = m[1].toUpperCase();
		if (allow && !allow[uf]) continue;
		out.push({ path: full, uf: uf, monolithic: false });
	}
	out.sort(function (a, b) {
		var ka = a.monolithic ? '' : a.uf;
		var kb = b.monolithic ? '' : b.uf;
		return ka < kb ? -1 : ka > kb ? 1 : 0;
	});
	return out;
}

/**
 * Carrega um ou mais arquivos de cache em um único Map(cep → row).
 * Aceita:
 * - caminho de arquivo (legado)
 * - diretório (todos `CEP_EXTERNO*.TXT` / filtrados por `ufs`)
 * @param {string} fileOrDir
 * @param {{ ufs?: string[] }} [opts]
 * @returns {Promise<Map<string, object>>}
 */
async function loadCacheMulti(fileOrDir, opts) {
	opts = opts || {};
	var map = new Map();
	if (!fileOrDir) return map;

	var files = [];
	if (fs.existsSync(fileOrDir) && fs.statSync(fileOrDir).isDirectory()) {
		files = listCacheFiles(fileOrDir, opts.ufs || null).map(function (f) {
			return f.path;
		});
	} else if (fs.existsSync(fileOrDir)) {
		files = [fileOrDir];
	} else {
		return map;
	}

	for (var i = 0; i < files.length; i++) {
		var part = await loadCache(files[i]);
		part.forEach(function (row, cep) {
			map.set(cep, row);
		});
	}
	return map;
}

/**
 * Mescla `rows` no map e grava **um arquivo por UF** em `dir`
 * (`CEP_EXTERNO_{UF}.TXT`). Só reescreve UFs tocadas pelas rows (e as que
 * já estavam no map com aquele `api_state`).
 *
 * @param {string} dir
 * @param {Map<string, object>} map
 * @param {object[]} rows
 * @returns {{ ufs: string[], files: string[], size: number }}
 */
function mergeAndSaveByUf(dir, map, rows) {
	var touched = Object.create(null);
	for (var i = 0; i < rows.length; i++) {
		var r = rows[i];
		var cep = digitsCep(r.cep);
		if (!cep) continue;
		r.cep = cep;
		r._dirty = true;
		delete r._raw;
		map.set(cep, r);
		touched[rowUf(r)] = true;
	}
	// reescreve cada UF tocada com **todas** as linhas do map daquela UF
	var byUf = Object.create(null);
	map.forEach(function (row) {
		var uf = rowUf(row);
		if (!byUf[uf]) byUf[uf] = new Map();
		byUf[uf].set(row.cep, row);
	});
	var written = [];
	var ufs = Object.keys(touched).sort();
	for (var j = 0; j < ufs.length; j++) {
		var uf = ufs[j];
		var m = byUf[uf] || new Map();
		var file = cachePathForUf(dir, uf);
		writeCache(file, m);
		written.push(file);
	}
	return { ufs: ufs, files: written, size: map.size };
}

/**
 * Parte um monólito `CEP_EXTERNO.TXT` em `CEP_EXTERNO_{UF}.TXT` por `api_state`.
 * @param {string} srcFile
 * @param {string} outDir
 * @param {{ removeSource?: boolean }} [opts]
 * @returns {Promise<{ byUf: Object.<string, number>, total: number, files: string[] }>}
 */
async function splitCacheByUf(srcFile, outDir, opts) {
	opts = opts || {};
	var map = await loadCache(srcFile);
	var byUf = Object.create(null);
	map.forEach(function (row) {
		var uf = rowUf(row);
		if (!byUf[uf]) byUf[uf] = new Map();
		byUf[uf].set(row.cep, row);
	});
	fs.mkdirSync(outDir || path.dirname(srcFile) || '.', { recursive: true });
	var files = [];
	var counts = Object.create(null);
	var ufs = Object.keys(byUf).sort();
	for (var i = 0; i < ufs.length; i++) {
		var uf = ufs[i];
		var file = cachePathForUf(outDir, uf);
		writeCache(file, byUf[uf]);
		files.push(file);
		counts[uf] = byUf[uf].size;
	}
	if (opts.removeSource && fs.existsSync(srcFile)) {
		fs.unlinkSync(srcFile);
	}
	return { byUf: counts, total: map.size, files: files };
}

module.exports = {
	COLS: COLS,
	FONTE_DEFAULT: FONTE_DEFAULT,
	digitsCep: digitsCep,
	formatRow: formatRow,
	parseRow: parseRow,
	fromAwesomeResponse: fromAwesomeResponse,
	loadCache: loadCache,
	writeCache: writeCache,
	appendRows: appendRows,
	mergeAndSave: mergeAndSave,
	normalizeUfToken: normalizeUfToken,
	rowUf: rowUf,
	cachePathForUf: cachePathForUf,
	listCacheFiles: listCacheFiles,
	loadCacheMulti: loadCacheMulti,
	mergeAndSaveByUf: mergeAndSaveByUf,
	splitCacheByUf: splitCacheByUf
};
