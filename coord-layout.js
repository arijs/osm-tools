'use strict';

/**
 * Compact coordinate layout along the OSM XML stream.
 * Distances are in degree-space (hypot(dLat, dLon)), not km / Mercator.
 */

var DEFAULT_SAMPLE_SIZE = 512;
var DEFAULT_JUMP_SMALL_DEG = 0.01;
var MAX_BLOCKS_BEFORE_MERGE = 50000;

function jumpDeg(lat1, lon1, lat2, lon2) {
	var dLat = lat2 - lat1;
	var dLon = lon2 - lon1;
	// shortest wrap across antimeridian
	if (dLon > 180) dLon -= 360;
	else if (dLon < -180) dLon += 360;
	return Math.sqrt(dLat * dLat + dLon * dLon);
}

function createCoordLayout(options) {
	options = options || {};
	return {
		mode: 'always-light',
		addressMappingEnabled: false,
		addressMappingEnabledAt: null,
		sampleSize: options.sampleSize || DEFAULT_SAMPLE_SIZE,
		jumpSmallDeg:
			options.jumpSmallDeg != null
				? options.jumpSmallDeg
				: DEFAULT_JUMP_SMALL_DEG,
		// running sequence (global)
		prevLat: null,
		prevLon: null,
		nodeOrdinal: 0,
		sumJumpDeg: 0,
		jumpCount: 0,
		maxJumpDeg: 0,
		smallJumpCount: 0,
		// current bzip block accumulator
		curBlock: null,
		// persisted
		blocks: [],
		samples: [],
		sampleSeen: 0, // for reservoir
		addrSamples: [],
		addrSampleSeen: 0
	};
}

function emptyBlockAcc(meta) {
	return {
		bzFile: meta.bzFile,
		bzBlock: meta.bzBlock,
		fileOffset: meta.fileOffset,
		byteOffsetStart: meta.byteOffsetStart,
		byteOffsetEnd: meta.byteOffsetEnd,
		chunkPosIn: meta.chunkPosIn,
		chunkPosOut: meta.chunkPosOut,
		nodes: 0,
		addrTaggedNodes: 0,
		namedNodes: 0,
		placeNodes: 0,
		bbox: null,
		first: null,
		last: null,
		sumJumpDeg: 0,
		jumpCount: 0,
		maxJumpDeg: 0,
		smallJumpCount: 0,
		// internal: last point in this block for jumps
		_prevLat: null,
		_prevLon: null
	};
}

function ensureCurBlock(layout, meta) {
	if (
		!layout.curBlock ||
		layout.curBlock.bzFile !== meta.bzFile ||
		layout.curBlock.bzBlock !== meta.bzBlock
	) {
		// if previous cur never finalized and had nodes, flush
		if (layout.curBlock && layout.curBlock.nodes > 0) {
			pushBlock(layout, layout.curBlock);
		}
		layout.curBlock = emptyBlockAcc(meta);
	} else {
		// refresh end positions
		layout.curBlock.byteOffsetEnd = meta.byteOffsetEnd;
		layout.curBlock.chunkPosIn = meta.chunkPosIn;
		layout.curBlock.chunkPosOut = meta.chunkPosOut;
	}
	return layout.curBlock;
}

function expandBbox(bbox, lat, lon) {
	if (!bbox) {
		return { minLat: lat, maxLat: lat, minLon: lon, maxLon: lon };
	}
	if (lat < bbox.minLat) bbox.minLat = lat;
	if (lat > bbox.maxLat) bbox.maxLat = lat;
	if (lon < bbox.minLon) bbox.minLon = lon;
	if (lon > bbox.maxLon) bbox.maxLon = lon;
	return bbox;
}

function pointSample(lat, lon, id) {
	var p = { lat: lat, lon: lon };
	if (id != null && id !== '') p.id = String(id);
	return p;
}

function reservoirPush(arr, seen, max, item) {
	seen++;
	if (arr.length < max) {
		arr.push(item);
	} else {
		var j = Math.floor(Math.random() * seen);
		if (j < max) arr[j] = item;
	}
	return seen;
}

/**
 * Record a closed node with lat/lon.
 * @param {object} layout
 * @param {object} node { lat, lon, id, hasAddr, hasName, hasPlace }
 * @param {object} pos { bzFile, bzBlock, fileOffset, byteOffset, chunkPos }
 */
function noteNodeCoord(layout, node, pos) {
	var lat = Number(node.lat);
	var lon = Number(node.lon);
	if (!isFinite(lat) || !isFinite(lon)) return layout;

	layout.nodeOrdinal++;
	var meta = {
		bzFile: pos.bzFile || 0,
		bzBlock: pos.bzBlock || 0,
		fileOffset: pos.fileOffset || 0,
		byteOffsetStart: pos.byteOffsetStart != null ? pos.byteOffsetStart : pos.byteOffset || 0,
		byteOffsetEnd: pos.byteOffset != null ? pos.byteOffset : 0,
		chunkPosIn: pos.chunkPos ? pos.chunkPos[0] : 0,
		chunkPosOut: pos.chunkPos ? pos.chunkPos[1] : 0
	};
	var b = ensureCurBlock(layout, meta);
	b.nodes++;
	b.bbox = expandBbox(b.bbox, lat, lon);
	var pt = pointSample(lat, lon, node.id);
	if (!b.first) b.first = pt;
	b.last = pt;

	// jumps within block
	if (b._prevLat != null) {
		var j1 = jumpDeg(b._prevLat, b._prevLon, lat, lon);
		b.sumJumpDeg += j1;
		b.jumpCount++;
		if (j1 > b.maxJumpDeg) b.maxJumpDeg = j1;
		if (j1 < layout.jumpSmallDeg) b.smallJumpCount++;
	}
	b._prevLat = lat;
	b._prevLon = lon;

	// global jumps
	if (layout.prevLat != null) {
		var j2 = jumpDeg(layout.prevLat, layout.prevLon, lat, lon);
		layout.sumJumpDeg += j2;
		layout.jumpCount++;
		if (j2 > layout.maxJumpDeg) layout.maxJumpDeg = j2;
		if (j2 < layout.jumpSmallDeg) layout.smallJumpCount++;
	}
	layout.prevLat = lat;
	layout.prevLon = lon;

	var kind = 'any';
	if (node.hasAddr) {
		b.addrTaggedNodes++;
		kind = 'addr';
	} else if (node.hasPlace) {
		b.placeNodes++;
		kind = 'place';
	} else if (node.hasName) {
		b.namedNodes++;
		kind = 'name';
	}

	var sample = {
		lat: lat,
		lon: lon,
		id: node.id != null ? String(node.id) : undefined,
		kind: kind,
		bzFile: meta.bzFile,
		bzBlock: meta.bzBlock,
		chunkPos: pos.chunkPos ? pos.chunkPos.slice() : [0, 0, 0],
		nodeOrdinal: layout.nodeOrdinal,
		fileOffsetEstimate: meta.fileOffset + (meta.byteOffsetEnd || 0)
	};

	layout.sampleSeen = reservoirPush(
		layout.samples,
		layout.sampleSeen,
		layout.sampleSize,
		sample
	);

	if (kind !== 'any' || layout.addressMappingEnabled) {
		// denser reservoir for interesting points (half of sampleSize)
		var half = Math.max(32, Math.floor(layout.sampleSize / 2));
		layout.addrSampleSeen = reservoirPush(
			layout.addrSamples,
			layout.addrSampleSeen,
			half,
			sample
		);
	}

	return layout;
}

function maybeEnableAddressMapping(layout, geoSignals, pos) {
	if (layout.addressMappingEnabled) return layout;
	var enable =
		(geoSignals && geoSignals.addrAny > 0) ||
		(geoSignals &&
			geoSignals.nodeWithLatLon > 0 &&
			(geoSignals.name > 0 || geoSignals.place > 0 || geoSignals.addrAny > 0));
	// stricter: enable on first addr:* as primary trigger; also name/place with points
	if (geoSignals && geoSignals.addrAny > 0) enable = true;
	else if (
		geoSignals &&
		geoSignals.nodeWithLatLon > 0 &&
		(geoSignals.name > 0 || geoSignals.place > 0)
	)
		enable = true;
	else enable = false;

	if (!enable) return layout;
	layout.addressMappingEnabled = true;
	layout.addressMappingEnabledAt = {
		bzFile: pos.bzFile || 0,
		bzBlock: pos.bzBlock || 0,
		chunkPos: pos.chunkPos ? pos.chunkPos.slice() : null,
		nodeOrdinal: layout.nodeOrdinal
	};
	return layout;
}

function stripInternalBlock(b) {
	return {
		bzFile: b.bzFile,
		bzBlock: b.bzBlock,
		fileOffset: b.fileOffset,
		byteOffsetStart: b.byteOffsetStart,
		byteOffsetEnd: b.byteOffsetEnd,
		chunkPosIn: b.chunkPosIn,
		chunkPosOut: b.chunkPosOut,
		nodes: b.nodes,
		addrTaggedNodes: b.addrTaggedNodes,
		namedNodes: b.namedNodes,
		placeNodes: b.placeNodes,
		bbox: b.bbox,
		first: b.first,
		last: b.last,
		sumJumpDeg: b.sumJumpDeg,
		jumpCount: b.jumpCount,
		maxJumpDeg: b.maxJumpDeg,
		smallJumpCount: b.smallJumpCount,
		meanJumpDeg: b.jumpCount ? b.sumJumpDeg / b.jumpCount : 0,
		pctSmallJumps: b.jumpCount ? b.smallJumpCount / b.jumpCount : 0
	};
}

function mergeTwoBlocks(a, b) {
	var m = {
		bzFile: a.bzFile,
		bzBlock: a.bzBlock + '-' + b.bzBlock,
		fileOffset: a.fileOffset,
		byteOffsetStart: a.byteOffsetStart,
		byteOffsetEnd: b.byteOffsetEnd,
		chunkPosIn: b.chunkPosIn,
		chunkPosOut: b.chunkPosOut,
		nodes: a.nodes + b.nodes,
		addrTaggedNodes: a.addrTaggedNodes + b.addrTaggedNodes,
		namedNodes: a.namedNodes + b.namedNodes,
		placeNodes: a.placeNodes + b.placeNodes,
		bbox: a.bbox
			? b.bbox
				? expandBbox(
						expandBbox(
							{
								minLat: a.bbox.minLat,
								maxLat: a.bbox.maxLat,
								minLon: a.bbox.minLon,
								maxLon: a.bbox.maxLon
							},
							b.bbox.minLat,
							b.bbox.minLon
						),
						b.bbox.maxLat,
						b.bbox.maxLon
					)
				: a.bbox
			: b.bbox,
		first: a.first,
		last: b.last || a.last,
		sumJumpDeg: a.sumJumpDeg + b.sumJumpDeg,
		jumpCount: a.jumpCount + b.jumpCount,
		maxJumpDeg: Math.max(a.maxJumpDeg, b.maxJumpDeg),
		smallJumpCount: a.smallJumpCount + b.smallJumpCount
	};
	m.meanJumpDeg = m.jumpCount ? m.sumJumpDeg / m.jumpCount : 0;
	m.pctSmallJumps = m.jumpCount ? m.smallJumpCount / m.jumpCount : 0;
	return m;
}

function maybeMergeBlocks(layout) {
	while (layout.blocks.length > MAX_BLOCKS_BEFORE_MERGE) {
		var next = [];
		for (var i = 0; i < layout.blocks.length; i += 2) {
			if (i + 1 < layout.blocks.length) {
				next.push(mergeTwoBlocks(layout.blocks[i], layout.blocks[i + 1]));
			} else {
				next.push(layout.blocks[i]);
			}
		}
		layout.blocks = next;
	}
}

function pushBlock(layout, acc) {
	if (!acc || acc.nodes <= 0) return;
	layout.blocks.push(stripInternalBlock(acc));
	maybeMergeBlocks(layout);
}

/**
 * Call when a bzip block finishes (after nodes closed during that block period).
 * Updates end positions and flushes accumulator if it belongs to this block.
 */
function finalizeBzBlockLayout(layout, meta) {
	if (!layout.curBlock) return layout;
	if (
		layout.curBlock.bzFile === meta.bzFile &&
		layout.curBlock.bzBlock === meta.bzBlock
	) {
		layout.curBlock.byteOffsetEnd = meta.byteOffsetEnd;
		layout.curBlock.chunkPosIn = meta.chunkPosIn;
		layout.curBlock.chunkPosOut = meta.chunkPosOut;
		layout.curBlock.fileOffset = meta.fileOffset;
		if (layout.curBlock.nodes > 0) {
			pushBlock(layout, layout.curBlock);
		}
		layout.curBlock = null;
	}
	return layout;
}

/** Flush any open block acc (soft-stop / end of file). */
function flushCoordLayout(layout) {
	if (layout.curBlock && layout.curBlock.nodes > 0) {
		pushBlock(layout, layout.curBlock);
	}
	layout.curBlock = null;
	return layout;
}

function sequenceSnapshot(layout) {
	return {
		meanJumpDeg: layout.jumpCount ? layout.sumJumpDeg / layout.jumpCount : 0,
		pctSmallJumps: layout.jumpCount
			? layout.smallJumpCount / layout.jumpCount
			: 0,
		maxJumpDeg: layout.maxJumpDeg,
		jumpCount: layout.jumpCount,
		sumJumpDeg: layout.sumJumpDeg,
		nodeOrdinal: layout.nodeOrdinal,
		jumpSmallDeg: layout.jumpSmallDeg
	};
}

/**
 * @param {object} layout
 * @param {object} [options]
 * @param {number|null} [options.maxBlocks] max blocks to keep in snapshot.
 *   - undefined/null: keep all (legacy)
 *   - 0: omit blocks (sequence + samples only) — default for new inventory saves
 *   - N>0: merge-pair downsample until length <= N
 */
function snapshotCoordLayout(layout, options) {
	flushCoordLayout(layout);
	options = options || {};
	var blocks = layout.blocks.slice();
	var maxBlocks = options.maxBlocks;
	var blocksTotal = blocks.length;
	if (maxBlocks === 0) {
		blocks = [];
	} else if (maxBlocks != null && maxBlocks > 0) {
		while (blocks.length > maxBlocks) {
			var next = [];
			for (var i = 0; i < blocks.length; i += 2) {
				if (i + 1 < blocks.length) {
					next.push(mergeTwoBlocks(blocks[i], blocks[i + 1]));
				} else {
					next.push(blocks[i]);
				}
			}
			if (next.length >= blocks.length) break;
			blocks = next;
		}
	}
	return {
		mode: layout.mode,
		addressMappingEnabled: layout.addressMappingEnabled,
		addressMappingEnabledAt: layout.addressMappingEnabledAt,
		sampleSize: layout.sampleSize,
		jumpSmallDeg: layout.jumpSmallDeg,
		sequence: sequenceSnapshot(layout),
		blocks: blocks,
		blocksTotal: blocksTotal,
		samples: layout.samples.slice(),
		addrSamples: layout.addrSamples.slice()
	};
}

function restoreCoordLayout(saved, options) {
	var layout = createCoordLayout(options);
	if (!saved || typeof saved !== 'object') return layout;
	layout.mode = saved.mode || layout.mode;
	layout.addressMappingEnabled = !!saved.addressMappingEnabled;
	layout.addressMappingEnabledAt = saved.addressMappingEnabledAt || null;
	if (saved.sampleSize) layout.sampleSize = saved.sampleSize;
	if (saved.jumpSmallDeg != null) layout.jumpSmallDeg = saved.jumpSmallDeg;
	layout.blocks = Array.isArray(saved.blocks) ? saved.blocks.slice() : [];
	layout.samples = Array.isArray(saved.samples) ? saved.samples.slice() : [];
	layout.addrSamples = Array.isArray(saved.addrSamples)
		? saved.addrSamples.slice()
		: [];
	layout.sampleSeen = layout.samples.length;
	layout.addrSampleSeen = layout.addrSamples.length;
	var seq = saved.sequence || {};
	layout.sumJumpDeg = seq.sumJumpDeg || 0;
	layout.jumpCount = seq.jumpCount || 0;
	layout.maxJumpDeg = seq.maxJumpDeg || 0;
	layout.smallJumpCount = seq.smallJumpCount || 0;
	layout.nodeOrdinal = seq.nodeOrdinal || 0;
	// restore prev from last sample or last block
	var lastPt = null;
	if (layout.samples.length) {
		lastPt = layout.samples[layout.samples.length - 1];
	} else if (layout.blocks.length && layout.blocks[layout.blocks.length - 1].last) {
		lastPt = layout.blocks[layout.blocks.length - 1].last;
	}
	if (lastPt) {
		layout.prevLat = lastPt.lat;
		layout.prevLon = lastPt.lon;
	}
	return layout;
}

function formatCoordLayout(layout) {
	var seq = sequenceSnapshot(layout);
	var blocks =
		layout.blocks.length + (layout.curBlock && layout.curBlock.nodes ? 1 : 0);
	var mean =
		seq.jumpCount > 0 ? seq.meanJumpDeg.toFixed(seq.meanJumpDeg >= 1 ? 2 : 3) : '—';
	var seqStr =
		seq.jumpCount > 0 ? seq.pctSmallJumps.toFixed(2) : '—';
	var label =
		seq.jumpCount > 20 && seq.pctSmallJumps < 0.2
			? 'lay~rand'
			: seq.jumpCount > 20 && seq.pctSmallJumps > 0.6
				? 'lay~seq'
				: 'lay';
	return (
		label +
		' seq=' +
		seqStr +
		' meanJump=' +
		mean +
		'° blocks=' +
		blocks +
		' samples=' +
		layout.samples.length +
		(layout.addressMappingEnabled ? ' +addr' : '')
	);
}

module.exports = {
	DEFAULT_SAMPLE_SIZE: DEFAULT_SAMPLE_SIZE,
	DEFAULT_JUMP_SMALL_DEG: DEFAULT_JUMP_SMALL_DEG,
	jumpDeg: jumpDeg,
	createCoordLayout: createCoordLayout,
	noteNodeCoord: noteNodeCoord,
	maybeEnableAddressMapping: maybeEnableAddressMapping,
	finalizeBzBlockLayout: finalizeBzBlockLayout,
	flushCoordLayout: flushCoordLayout,
	snapshotCoordLayout: snapshotCoordLayout,
	restoreCoordLayout: restoreCoordLayout,
	formatCoordLayout: formatCoordLayout,
	sequenceSnapshot: sequenceSnapshot
};
