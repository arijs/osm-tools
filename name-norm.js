'use strict';

/**
 * Normalize place/street names for matching (OSM ↔ DNE/locais).
 * Lowercase, strip combining marks, collapse whitespace.
 */

function nameNorm(value) {
	if (value == null || value === '') return '';
	var s = String(value);
	try {
		s = s.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
	} catch (_) {
		// older engines without normalize
	}
	s = s.toLowerCase();
	s = s.replace(/[^a-z0-9]+/g, ' ').trim();
	s = s.replace(/\s+/g, ' ');
	return s;
}

module.exports = {
	nameNorm: nameNorm
};
