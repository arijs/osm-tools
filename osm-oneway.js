'use strict';

/**
 * Sentido de circulação OSM → código compacto no GEOM.
 *
 *   0 = ausente (sem tag oneway)
 *   1 = frente  (oneway=yes|true|1) — no sentido dos nós da way
 *   2 = reverso (oneway=-1|reverse)
 *   3 = mão dupla explícita (oneway=no|false|0)
 *
 * Outros valores (`alternating`, `reversible`, …) → 0.
 * Spec: docs/geo/geometria-via-destaque.md
 */

var ONEWAY_AUSENTE = 0;
var ONEWAY_FRENTE = 1;
var ONEWAY_REVERSO = 2;
var ONEWAY_DUPLO = 3;

/**
 * @param {object|null|undefined} tags
 * @returns {0|1|2|3}
 */
function onewayCode(tags) {
	if (!tags) return ONEWAY_AUSENTE;
	var raw = tags.oneway;
	if (raw == null || raw === '') return ONEWAY_AUSENTE;
	var v = String(raw).trim().toLowerCase();
	if (v === 'yes' || v === 'true' || v === '1') return ONEWAY_FRENTE;
	if (v === '-1' || v === 'reverse') return ONEWAY_REVERSO;
	if (v === 'no' || v === 'false' || v === '0') return ONEWAY_DUPLO;
	return ONEWAY_AUSENTE;
}

/**
 * Lê a coluna de sentido de uma linha GEOM (2 ou 3 campos).
 * Legado `osm_id@polyline` → 0.
 * @param {string|number|undefined} field
 * @returns {0|1|2|3}
 */
function parseOnewayField(field) {
	if (field == null || field === '') return ONEWAY_AUSENTE;
	var n = Number(field);
	if (n === 1 || n === 2 || n === 3) return n;
	return ONEWAY_AUSENTE;
}

/**
 * Parte `resto` após `osm_id@` em { polyline, oneway }.
 * Aceita legado (só polyline) e novo (`polyline@code`).
 */
function splitGeomPayload(resto) {
	if (resto == null || resto === '') {
		return { polyline: '', oneway: ONEWAY_AUSENTE };
	}
	var at = String(resto).indexOf('@');
	if (at < 0) {
		return { polyline: String(resto), oneway: ONEWAY_AUSENTE };
	}
	return {
		polyline: String(resto).slice(0, at),
		oneway: parseOnewayField(String(resto).slice(at + 1))
	};
}

module.exports = {
	ONEWAY_AUSENTE: ONEWAY_AUSENTE,
	ONEWAY_FRENTE: ONEWAY_FRENTE,
	ONEWAY_REVERSO: ONEWAY_REVERSO,
	ONEWAY_DUPLO: ONEWAY_DUPLO,
	onewayCode: onewayCode,
	parseOnewayField: parseOnewayField,
	splitGeomPayload: splitGeomPayload
};
