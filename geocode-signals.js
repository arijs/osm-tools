'use strict';

/**
 * Shared geocoding-signal counters (inventory phase).
 * Used by index0.js (XML) and index-pbf.js (PBF).
 */

var hop = Object.prototype.hasOwnProperty;

var thousandsUnits = ['', 'k', 'm', 'b'];
function formatCount(x) {
	x = +x;
	var t = 0;
	while (x > 1e3 && t < 3) {
		x /= 1e3;
		t++;
	}
	return x.toFixed(t).replace('.', thousandsUnits[t]);
}

function createGeocodeSignals() {
	return {
		node: 0,
		way: 0,
		relation: 0,
		nodeWithLat: 0,
		nodeWithLon: 0,
		nodeWithLatLon: 0,
		addrAny: 0,
		addrStreet: 0,
		addrHousenumber: 0,
		addrCity: 0,
		addrPostcode: 0,
		addrCountry: 0,
		addrByKey: {},
		name: 0,
		nameLocalized: 0,
		place: 0,
		highway: 0,
		building: 0
	};
}

/**
 * @param {object} g createGeocodeSignals()
 * @param {string} cname element name: node|way|relation|tag
 * @param {object} attrs
 */
function noteGeocodeOpen(g, cname, attrs) {
	attrs = attrs || {};
	if (cname === 'node') {
		g.node++;
		var hasLat = attrs.lat != null && attrs.lat !== '';
		var hasLon = attrs.lon != null && attrs.lon !== '';
		if (hasLat) g.nodeWithLat++;
		if (hasLon) g.nodeWithLon++;
		if (hasLat && hasLon) g.nodeWithLatLon++;
		return;
	}
	if (cname === 'way') {
		g.way++;
		return;
	}
	if (cname === 'relation') {
		g.relation++;
		return;
	}
	if (cname === 'tag' && attrs.k != null && attrs.k !== '') {
		var k = String(attrs.k);
		if (k.indexOf('addr:') === 0) {
			g.addrAny++;
			g.addrByKey[k] = (g.addrByKey[k] || 0) + 1;
			if (k === 'addr:street') g.addrStreet++;
			else if (k === 'addr:housenumber') g.addrHousenumber++;
			else if (k === 'addr:city' || k === 'addr:municipality') g.addrCity++;
			else if (k === 'addr:postcode') g.addrPostcode++;
			else if (k === 'addr:country') g.addrCountry++;
		} else if (k === 'name') {
			g.name++;
		} else if (k.indexOf('name:') === 0) {
			g.nameLocalized++;
		} else if (k === 'place') {
			g.place++;
		} else if (k === 'highway') {
			g.highway++;
		} else if (k === 'building') {
			g.building++;
		}
	}
}

/**
 * Note OSM tags as {k:v} map (PBF stringtable resolved).
 */
function noteGeocodeTags(g, tags) {
	if (!tags) return;
	for (var k in tags) {
		if (hop.call(tags, k)) noteGeocodeOpen(g, 'tag', { k: k, v: tags[k] });
	}
}

function geocodeHints(g) {
	var hasPointGeometry = g.nodeWithLatLon > 0;
	var hasAddressTags = g.addrAny > 0;
	var hasPlaceTags = g.place > 0;
	var hasNamedFeatures = g.name > 0 || g.nameLocalized > 0;
	var hasRoadNetwork = g.highway > 0 || g.way > 0;
	return {
		hasPointGeometry: hasPointGeometry,
		hasAddressTags: hasAddressTags,
		hasPlaceTags: hasPlaceTags,
		hasNamedFeatures: hasNamedFeatures,
		hasRoadNetwork: hasRoadNetwork,
		likelyHasGeocodeMaterial:
			hasPointGeometry && (hasAddressTags || hasPlaceTags || hasNamedFeatures)
	};
}

function snapshotGeocodeSignals(g) {
	var hints = geocodeHints(g);
	var addrByKey = {};
	var keys = Object.keys(g.addrByKey || {});
	for (var i = 0; i < keys.length; i++) {
		addrByKey[keys[i]] = g.addrByKey[keys[i]];
	}
	return {
		node: g.node,
		way: g.way,
		relation: g.relation,
		nodeWithLat: g.nodeWithLat,
		nodeWithLon: g.nodeWithLon,
		nodeWithLatLon: g.nodeWithLatLon,
		addrAny: g.addrAny,
		addrStreet: g.addrStreet,
		addrHousenumber: g.addrHousenumber,
		addrCity: g.addrCity,
		addrPostcode: g.addrPostcode,
		addrCountry: g.addrCountry,
		addrByKey: addrByKey,
		name: g.name,
		nameLocalized: g.nameLocalized,
		place: g.place,
		highway: g.highway,
		building: g.building,
		hints: hints
	};
}

function formatGeocodeSignals(g) {
	var hints = geocodeHints(g);
	var parts = [];
	var nPart = 'n=' + formatCount(g.node);
	if (g.nodeWithLatLon) {
		nPart += '(ll=' + formatCount(g.nodeWithLatLon) + ')';
	}
	parts.push(nPart);
	if (g.way) parts.push('w=' + formatCount(g.way));
	if (g.relation) parts.push('r=' + formatCount(g.relation));
	if (g.addrAny) {
		parts.push('addr=' + formatCount(g.addrAny));
		if (g.addrStreet) parts.push('st=' + formatCount(g.addrStreet));
		if (g.addrHousenumber) parts.push('hn=' + formatCount(g.addrHousenumber));
		if (g.addrCity) parts.push('city=' + formatCount(g.addrCity));
	}
	if (g.name) parts.push('name=' + formatCount(g.name));
	if (g.place) parts.push('place=' + formatCount(g.place));
	if (g.highway) parts.push('hwy=' + formatCount(g.highway));
	var label = hints.likelyHasGeocodeMaterial ? 'GEO' : 'geo';
	return label + ' ' + parts.join(' ');
}

function recomputeGeocodeSignalsFromTree(root) {
	var g = createGeocodeSignals();
	function walk(node, elementName) {
		if (!node) return;
		if (elementName === 'node') {
			var n = node.count || 0;
			g.node += n;
			var latC = node.attrs && node.attrs.lat ? node.attrs.lat.count || 0 : 0;
			var lonC = node.attrs && node.attrs.lon ? node.attrs.lon.count || 0 : 0;
			g.nodeWithLat += latC;
			g.nodeWithLon += lonC;
			g.nodeWithLatLon += Math.min(latC, lonC);
		} else if (elementName === 'way') {
			g.way += node.count || 0;
		} else if (elementName === 'relation') {
			g.relation += node.count || 0;
		}
		var tk = node.tag_k_map;
		if (tk) {
			for (var k in tk) {
				if (!hop.call(tk, k)) continue;
				var c = tk[k].count || 0;
				if (k.indexOf('addr:') === 0) {
					g.addrAny += c;
					g.addrByKey[k] = (g.addrByKey[k] || 0) + c;
					if (k === 'addr:street') g.addrStreet += c;
					else if (k === 'addr:housenumber') g.addrHousenumber += c;
					else if (k === 'addr:city' || k === 'addr:municipality')
						g.addrCity += c;
					else if (k === 'addr:postcode') g.addrPostcode += c;
					else if (k === 'addr:country') g.addrCountry += c;
				} else if (k === 'name') g.name += c;
				else if (k.indexOf('name:') === 0) g.nameLocalized += c;
				else if (k === 'place') g.place += c;
				else if (k === 'highway') g.highway += c;
				else if (k === 'building') g.building += c;
				walk(tk[k], 'tag');
			}
		}
		var kids = node.tags;
		if (kids) {
			for (var childName in kids) {
				if (hop.call(kids, childName)) walk(kids[childName], childName);
			}
		}
	}
	if (root && root.tags) {
		for (var top in root.tags) {
			if (hop.call(root.tags, top)) walk(root.tags[top], top);
		}
	} else {
		walk(root, null);
	}
	return g;
}

function restoreGeocodeSignals(saved) {
	var g = createGeocodeSignals();
	if (!saved || typeof saved !== 'object') return g;
	for (var k in saved) {
		if (!hop.call(saved, k) || k === 'hints') continue;
		if (k === 'addrByKey' && saved.addrByKey) {
			g.addrByKey = Object.assign({}, saved.addrByKey);
		} else if (typeof saved[k] === 'number') {
			g[k] = saved[k];
		}
	}
	return g;
}

module.exports = {
	formatCount: formatCount,
	createGeocodeSignals: createGeocodeSignals,
	noteGeocodeOpen: noteGeocodeOpen,
	noteGeocodeTags: noteGeocodeTags,
	geocodeHints: geocodeHints,
	snapshotGeocodeSignals: snapshotGeocodeSignals,
	formatGeocodeSignals: formatGeocodeSignals,
	recomputeGeocodeSignalsFromTree: recomputeGeocodeSignalsFromTree,
	restoreGeocodeSignals: restoreGeocodeSignals
};
