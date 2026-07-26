'use strict';

/**
 * OSM PBF inventory for geocoding exploration (independent of index0 XML/bz2).
 *
 * CLI:
 *   node index-pbf.js [file.osm.pbf] [stats.json] [results.json]
 *   OSM_PBF_INPUT / OSM_PBF_STATS / OSM_PBF_RESULTS
 *
 * Soft-stop: Ctrl+C (30s max to finish current blob); 2nd hard-stop; 3rd exit.
 */

var fs = require('fs');
var path = require('path');
var Pbf = require('pbf');
var osmformat = require('./osmformat.proto.js');
var pbfReader = require('./pbf-reader');
var dataSize = require('./datasize');
var GeoSig = require('./geocode-signals');
var CoordLayout = require('./coord-layout');

var DEFAULT_SOFT_STOP_MS = 30000;

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
		out.push(Buffer.isBuffer(b) ? b.toString('utf8') : Buffer.from(b).toString('utf8'));
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

/**
 * Decode DenseNodes into callbacks per node.
 * @param {object} dense DenseNodes
 * @param {string[]} strings
 * @param {number} granularity
 * @param {number} latOffset
 * @param {number} lonOffset
 * @param {function} onNode (id, lat, lon, tags)
 */
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
		var latDeg = decodeCoord(lat, granularity, latOffset);
		var lonDeg = decodeCoord(lon, granularity, lonOffset);
		onNode(id, latDeg, lonDeg, tags);
	}
}

function processPrimitiveBlock(block, geo, layout, posMeta, tagKeyCounts) {
	var strings = stringTableToArray(block.stringtable);
	var granularity = block.granularity || 100;
	var latOffset = block.lat_offset || 0;
	var lonOffset = block.lon_offset || 0;
	var groups = block.primitivegroup || [];

	for (var g = 0; g < groups.length; g++) {
		var pg = groups[g];

		// Sparse nodes
		if (pg.nodes && pg.nodes.length) {
			for (var ni = 0; ni < pg.nodes.length; ni++) {
				var nd = pg.nodes[ni];
				var tags = tagsFromKeysVals(nd.keys, nd.vals, strings);
				var lat = decodeCoord(nd.lat, granularity, latOffset);
				var lon = decodeCoord(nd.lon, granularity, lonOffset);
				handleNode(geo, layout, posMeta, tagKeyCounts, nd.id, lat, lon, tags);
			}
		}

		// Dense nodes
		if (pg.dense) {
			forEachDenseNode(
				pg.dense,
				strings,
				granularity,
				latOffset,
				lonOffset,
				function (id, lat, lon, tags) {
					handleNode(geo, layout, posMeta, tagKeyCounts, id, lat, lon, tags);
				}
			);
		}

		// Ways
		if (pg.ways && pg.ways.length) {
			for (var wi = 0; wi < pg.ways.length; wi++) {
				var w = pg.ways[wi];
				var wt = tagsFromKeysVals(w.keys, w.vals, strings);
				GeoSig.noteGeocodeOpen(geo, 'way', { id: String(w.id) });
				GeoSig.noteGeocodeTags(geo, wt);
				countTags(tagKeyCounts, wt);
			}
		}

		// Relations
		if (pg.relations && pg.relations.length) {
			for (var ri = 0; ri < pg.relations.length; ri++) {
				var r = pg.relations[ri];
				var rt = tagsFromKeysVals(r.keys, r.vals, strings);
				GeoSig.noteGeocodeOpen(geo, 'relation', { id: String(r.id) });
				GeoSig.noteGeocodeTags(geo, rt);
				countTags(tagKeyCounts, rt);
			}
		}
	}
}

function countTags(map, tags) {
	for (var k in tags) {
		if (Object.prototype.hasOwnProperty.call(tags, k)) {
			map[k] = (map[k] || 0) + 1;
		}
	}
}

function handleNode(geo, layout, posMeta, tagKeyCounts, id, lat, lon, tags) {
	var attrs = {
		id: String(id),
		lat: String(lat),
		lon: String(lon)
	};
	GeoSig.noteGeocodeOpen(geo, 'node', attrs);
	GeoSig.noteGeocodeTags(geo, tags);
	countTags(tagKeyCounts, tags);

	var hasAddr = false;
	var hasName = false;
	var hasPlace = false;
	for (var k in tags) {
		if (!Object.prototype.hasOwnProperty.call(tags, k)) continue;
		if (k.indexOf('addr:') === 0) hasAddr = true;
		else if (k === 'name' || k.indexOf('name:') === 0) hasName = true;
		else if (k === 'place') hasPlace = true;
	}
	if (hasAddr || hasName || hasPlace) {
		CoordLayout.maybeEnableAddressMapping(layout, geo, posMeta);
	}
	CoordLayout.noteNodeCoord(
		layout,
		{
			lat: lat,
			lon: lon,
			id: id,
			hasAddr: hasAddr,
			hasName: hasName,
			hasPlace: hasPlace
		},
		posMeta
	);
}

function headerBBoxToDegrees(bbox) {
	if (!bbox) return null;
	// nanodegrees
	return {
		left: bbox.left * 1e-9,
		right: bbox.right * 1e-9,
		top: bbox.top * 1e-9,
		bottom: bbox.bottom * 1e-9
	};
}

/**
 * @param {object} options
 * @param {string} options.inputPath
 * @param {string} [options.statsPath]
 * @param {string} [options.resultsPath]
 * @param {boolean} [options.quiet]
 * @param {boolean} [options.resume]
 * @param {boolean} [options.saveStats]
 * @param {number} [options.softStopMaxMs]
 * @param {function} [options.onControl]
 */
function runPbfProcess(options) {
	options = options || {};
	var inputPath = options.inputPath;
	if (!inputPath) {
		return Promise.reject(new Error('runPbfProcess: inputPath required'));
	}
	var statsPath =
		options.statsPath || inputPath.replace(/\.pbf$/i, '') + '-pbf-stats.json';
	var resultsPath =
		options.resultsPath ||
		inputPath.replace(/\.pbf$/i, '') + '-pbf-results.json';
	var quiet = !!options.quiet;
	var resume = options.resume !== false;
	var saveStats = options.saveStats !== false;
	var softStopMaxMs =
		options.softStopMaxMs == null ? DEFAULT_SOFT_STOP_MS : options.softStopMaxMs;
	// Default: omit full coordLayout.blocks from JSON (was ~10MB on Sudeste).
	// Pass layoutMaxBlocks: null for all, or N to downsample; layoutFullBlocks: true = all.
	var layoutMaxBlocks = options.layoutFullBlocks
		? null
		: options.layoutMaxBlocks !== undefined
			? options.layoutMaxBlocks
			: 0;

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
					'\nSoft-stop: parando no fim do blob PBF atual (max ' +
						Math.round(softStopMaxMs / 1000) +
						's)' +
						(reason ? ' — ' + reason : '') +
						'\n'
				);
			}
		}
		function requestHardStop() {
			hardStopRequested = true;
			softStopRequested = true;
			softStopDeadline = Date.now();
			if (!quiet) console.error('\nHard-stop: encerrando após o blob atual...\n');
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

		var tstart = Date.now();
		var totaltime = 0;
		var previousRuns = [];
		var startOffset = 0;
		var startBlobIndex = 0;
		var geo = GeoSig.createGeocodeSignals();
		var layout = CoordLayout.createCoordLayout({
			sampleSize: options.coordSampleSize,
			jumpSmallDeg: options.jumpSmallDeg
		});
		var tagKeyCounts = {};
		var headerInfo = null;
		var dataBlobs = 0;
		var bytesReadEstimate = 0;
		var previousBytes = 0;

		// resume
		if (resume && fs.existsSync(statsPath)) {
			try {
				var prev = JSON.parse(fs.readFileSync(statsPath, 'utf8'));
				if (prev.cursor) {
					startOffset = prev.cursor.fileOffset || 0;
					startBlobIndex = prev.cursor.blobIndex || 0;
				}
				if (prev.geocodeSignals) {
					geo = GeoSig.restoreGeocodeSignals(prev.geocodeSignals);
				}
				if (prev.coordLayout) {
					layout = CoordLayout.restoreCoordLayout(prev.coordLayout, {
						sampleSize: options.coordSampleSize,
						jumpSmallDeg: options.jumpSmallDeg
					});
				}
				if (prev.tagKeyCounts) tagKeyCounts = prev.tagKeyCounts;
				if (prev.header) headerInfo = prev.header;
				if (prev.runs) previousRuns = prev.runs;
				if (prev.current && typeof prev.current.time === 'number') {
					totaltime = prev.current.time;
				}
				if (prev.current && prev.current.bytesRead) {
					previousBytes = prev.current.bytesRead;
					bytesReadEstimate = previousBytes;
				}
				if (!quiet) {
					console.error(
						'Retomando PBF offset=' +
							startOffset +
							' blob=' +
							startBlobIndex +
							' | ' +
							GeoSig.formatGeocodeSignals(geo) +
							' | ' +
							CoordLayout.formatCoordLayout(layout)
					);
				}
			} catch (e) {
				if (!quiet) console.error('Aviso: não foi possível retomar stats', e.message);
			}
		}

		if (!quiet) {
			console.error('Arquivo:  ' + inputPath);
			console.error('Tamanho:  ' + dataSize(fileSize));
			console.error('Stats:    ' + statsPath);
			console.error('Results:  ' + resultsPath);
			console.error('');
			console.error(
				'percent  speed  elapsed  eta  /  blob  /  geoSignals  /  layout'
			);
		}

		var tRun = Date.now();
		var lastPrint = 0;

		function printProgress(blobIndex, force) {
			if (quiet) return;
			var now = Date.now();
			if (!force && now - lastPrint < 200) return;
			lastPrint = now;
			var elapsed = (now - tRun) * 0.001;
			var pos = bytesReadEstimate;
			var percent = (100 * pos) / fileSize;
			var runBytes = pos - previousBytes;
			var speed = elapsed > 0 ? runBytes / elapsed : 0;
			var remain =
				speed > 0 ? (fileSize - pos) / speed : Number.POSITIVE_INFINITY;
			var remainStr =
				isFinite(remain) && remain >= 0 ? printTime(remain) : '--:--';
			process.stdout.write(
				'\r' +
					percent.toFixed(3) +
					' ' +
					dataSize(speed) +
					'/s ' +
					printTime(elapsed) +
					' ' +
					remainStr +
					' / blob ' +
					blobIndex +
					' / ' +
					GeoSig.formatGeocodeSignals(geo) +
					' / ' +
					CoordLayout.formatCoordLayout(layout) +
					'   '
			);
		}

		function shouldStop() {
			if (hardStopRequested) return true;
			if (!softStopRequested) return false;
			return Date.now() >= softStopDeadline;
		}

		try {
			pbfReader.forEachBlob(
				inputPath,
				{ startOffset: startOffset, startBlobIndex: startBlobIndex },
				function (blob) {
					bytesReadEstimate = blob.nextOffset;
					var posMeta = {
						bzFile: 0,
						bzBlock: blob.blobIndex,
						fileOffset: blob.fileOffset,
						byteOffset: 0,
						byteOffsetStart: 0,
						chunkPos: [blob.fileOffset, blob.nextOffset, dataBlobs]
					};

					if (blob.type === 'OSMHeader') {
						var hb = osmformat.HeaderBlock.read(new Pbf(blob.data));
						headerInfo = {
							bbox: headerBBoxToDegrees(hb.bbox),
							required_features: hb.required_features || [],
							optional_features: hb.optional_features || [],
							writingprogram: hb.writingprogram || '',
							source: hb.source || '',
							osmosis_replication_timestamp:
								hb.osmosis_replication_timestamp || 0
						};
					} else if (blob.type === 'OSMData') {
						var pb = osmformat.PrimitiveBlock.read(new Pbf(blob.data));
						processPrimitiveBlock(
							pb,
							geo,
							layout,
							posMeta,
							tagKeyCounts
						);
						dataBlobs++;
						// finalize segment for this blob (like bzip block)
						CoordLayout.finalizeBzBlockLayout(layout, {
							bzFile: 0,
							bzBlock: blob.blobIndex,
							fileOffset: blob.fileOffset,
							byteOffsetEnd: blob.blobSize,
							chunkPosIn: blob.fileOffset,
							chunkPosOut: blob.nextOffset
						});
					}

					printProgress(blob.blobIndex, false);

					if (shouldStop()) {
						stoppedEarly = true;
						stopReason = hardStopRequested
							? 'hard-stop'
							: softStopRequested
								? 'soft-stop'
								: 'stop';
						return 'stop';
					}
				}
			);
		} catch (err) {
			return fail(err);
		}

		printProgress(startBlobIndex + dataBlobs, true);
		if (!quiet) process.stdout.write('\n');

		CoordLayout.flushCoordLayout(layout);
		var geoSnap = GeoSig.snapshotGeocodeSignals(geo);
		var layoutSnap = CoordLayout.snapshotCoordLayout(layout, {
			maxBlocks: layoutMaxBlocks
		});
		var elapsedMs = Date.now() - tstart;
		var runTime = (Date.now() - tRun) * 0.001;

		var result = {
			error: null,
			stoppedEarly: stoppedEarly,
			stopReason: stopReason,
			inputPath: inputPath,
			statsPath: statsPath,
			resultsPath: resultsPath,
			fileSize: fileSize,
			header: headerInfo,
			dataBlobs: dataBlobs,
			cursor: {
				fileOffset: bytesReadEstimate,
				blobIndex: startBlobIndex + dataBlobs,
				// at EOF, cursor points past last blob
				eof: bytesReadEstimate >= fileSize && !stoppedEarly
			},
			geocodeSignals: geoSnap,
			coordLayout: layoutSnap,
			tagKeyCountsTop: topTagKeys(tagKeyCounts, 40),
			elapsedMs: elapsedMs,
			xmlStackMismatches: 0
		};

		function finishWrite(err) {
			if (err) return fail(err);
			if (!quiet) {
				if (stoppedEarly) {
					console.error(
						'Parou antes do final (' +
							(stopReason || 'soft-stop') +
							'). Stats salvos para retomar.'
					);
				} else {
					console.error('Fim do PBF. ' + GeoSig.formatGeocodeSignals(geo));
					if (geoSnap.hints && geoSnap.hints.likelyHasGeocodeMaterial) {
						console.error(
							'→ Sinais de geocoding detectados (pontos + addr/name/place).'
						);
					}
				}
			}
			ok(result);
		}

		if (!saveStats) {
			return finishWrite(null);
		}

		var percent = bytesReadEstimate / fileSize;
		var speed = runTime > 0 ? (bytesReadEstimate - previousBytes) / runTime : 0;
		var payload = {
			header: headerInfo,
			cursor: result.cursor,
			geocodeSignals: geoSnap,
			coordLayout: layoutSnap,
			tagKeyCounts: tagKeyCounts,
			tagKeyCountsTop: result.tagKeyCountsTop,
			runs: previousRuns.concat([
				{
					start: new Date(tRun).toISOString(),
					time: Math.round(runTime),
					timeStr: printTime(runTime),
					percent: percent,
					speed: Math.round(speed),
					speedStr: dataSize(speed),
					bytesRead: bytesReadEstimate,
					dataBlobs: dataBlobs,
					blobIndex: result.cursor.blobIndex
				}
			]),
			current: {
				percent: percent,
				time: totaltime + Math.round(runTime),
				timeStr: printTime(totaltime + runTime),
				bytesRead: bytesReadEstimate,
				blobIndex: result.cursor.blobIndex,
				dataBlobs: dataBlobs
			}
		};

		fs.writeFile(statsPath, JSON.stringify(payload), function (err) {
			if (err) {
				if (!quiet) console.error('Erro ao salvar stats', err);
				return fail(err);
			}
			if (!quiet) console.error('Stats salvos: ' + statsPath);
			fs.writeFile(resultsPath, JSON.stringify(result), function (err2) {
				if (err2) {
					if (!quiet) console.error('Erro ao salvar results', err2);
					return fail(err2);
				}
				if (!quiet) console.error('Results salvos: ' + resultsPath);
				finishWrite(null);
			});
		});
	});
}

function topTagKeys(map, n) {
	var arr = [];
	for (var k in map) {
		if (Object.prototype.hasOwnProperty.call(map, k)) {
			arr.push({ k: k, count: map[k] });
		}
	}
	arr.sort(function (a, b) {
		return b.count - a.count;
	});
	return arr.slice(0, n || 40);
}

function defaultCliPaths() {
	var argv = process.argv.slice(2).filter(function (a) {
		return a.indexOf('-') !== 0;
	});
	var input =
		argv[0] ||
		process.env.OSM_PBF_INPUT ||
		path.resolve('G:\\sudeste-260725.osm.pbf');
	var stats =
		argv[1] ||
		process.env.OSM_PBF_STATS ||
		input.replace(/\.pbf$/i, '') + '-pbf-stats.json';
	var results =
		argv[2] ||
		process.env.OSM_PBF_RESULTS ||
		input.replace(/\.pbf$/i, '') + '-pbf-results.json';
	return { inputPath: input, statsPath: stats, resultsPath: results };
}

function parseLayoutCliFlags(argv) {
	var layoutFullBlocks = false;
	var layoutMaxBlocks = undefined;
	for (var i = 0; i < argv.length; i++) {
		var a = argv[i];
		if (a === '--layout-full-blocks') layoutFullBlocks = true;
		else if (a.indexOf('--layout-max-blocks=') === 0) {
			layoutMaxBlocks = parseInt(a.slice(20), 10);
			if (isNaN(layoutMaxBlocks)) layoutMaxBlocks = 0;
		}
	}
	return { layoutFullBlocks: layoutFullBlocks, layoutMaxBlocks: layoutMaxBlocks };
}

function main() {
	var paths = defaultCliPaths();
	var layoutFlags = parseLayoutCliFlags(process.argv.slice(2));
	var control = null;
	var sigintCount = 0;
	process.on('SIGINT', function () {
		sigintCount++;
		if (!control) {
			console.error('\nAinda inicializando...\n');
			return;
		}
		if (sigintCount === 1) {
			control.softStop('Ctrl+C');
			return;
		}
		if (sigintCount === 2) {
			control.hardStop();
			return;
		}
		console.error('\nSaindo imediatamente.\n');
		process.exit(130);
	});

	runPbfProcess({
		inputPath: paths.inputPath,
		statsPath: paths.statsPath,
		resultsPath: paths.resultsPath,
		quiet: false,
		resume: true,
		saveStats: true,
		softStopMaxMs: DEFAULT_SOFT_STOP_MS,
		layoutFullBlocks: layoutFlags.layoutFullBlocks,
		layoutMaxBlocks: layoutFlags.layoutMaxBlocks,
		onControl: function (ctl) {
			control = ctl;
		}
	})
		.then(function () {
			process.exitCode = 0;
		})
		.catch(function (err) {
			console.error(err);
			process.exitCode = 1;
		});
}

module.exports = {
	runPbfProcess: runPbfProcess,
	defaultCliPaths: defaultCliPaths,
	processPrimitiveBlock: processPrimitiveBlock,
	forEachDenseNode: forEachDenseNode,
	decodeCoord: decodeCoord,
	DEFAULT_SOFT_STOP_MS: DEFAULT_SOFT_STOP_MS
};

if (require.main === module) {
	main();
}
