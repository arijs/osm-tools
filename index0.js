'use strict';

/**
 * Streaming OSM .osm.bz2 processor:
 *   seek-bzip blocks → StreamingDecoder → LineSplitter → XMLParser
 * Tracks XML tag stats and optional resume via a stats JSON file.
 *
 * CLI:
 *   node index0.js [input.osm.bz2] [stats.json]
 *   OSM_INPUT / OSM_STATS env vars also accepted
 *
 *   bzip/bzStats NUNCA vão embutidos em stats.json / results.json.
 *   Sem flag: esses dados são descartados ao salvar.
 *   --save-bz-stats  → grava sidecars:
 *     {stats}-bzip.json
 *     {results}-bzip.json
 *   --save-bz-stats-per-member  → o mesmo + um JSON por membro bzip
 *   OSM_SAVE_BZ_STATS=1  /  OSM_SAVE_BZ_STATS_PER_MEMBER=1
 *
 * Library:
 *   const { runProcess } = require('./index0');
 *   await runProcess({ inputPath, statsPath, quiet: true, saveBzStatsSeparate: true });
 */

var fs = require('fs');
var path = require('path');
var util = require('util');
var stream = require('stream');
var SeekBzip = require('@arijs/seek-bzip');
var { XMLParser } = require('@arijs/stream-xml-parser');
var dataSize = require('./datasize');
var StreamingDecoder = require('./streaming-decoder');
var LineSplitter = require('./line-splitter');
var CoordLayout = require('./coord-layout');
var GeoSig = require('./geocode-signals');
var createGeocodeSignals = GeoSig.createGeocodeSignals;
var noteGeocodeOpen = GeoSig.noteGeocodeOpen;
var geocodeHints = GeoSig.geocodeHints;
var snapshotGeocodeSignals = GeoSig.snapshotGeocodeSignals;
var formatGeocodeSignals = GeoSig.formatGeocodeSignals;
var recomputeGeocodeSignalsFromTree = GeoSig.recomputeGeocodeSignalsFromTree;

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

function printCRC(crc) {
	return 'number' === typeof crc
		? ('00000000' + crc.toString(16)).substr(-8, 8)
		: crc;
}
function readCRC(crc) {
	return 'string' === typeof crc && crc.length <= 8
		? parseInt(crc, 16)
		: crc;
}

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
			: d >= 100
				? (' ' + d.toFixed(0)).substr(-4) + 'd'
				: d.toFixed(d >= 10 ? 1 : 2) + 'd';
}

function subtractArrays(a1, a2) {
	var b = [];
	var c = a1.length;
	for (var i = 0; i < c; i++) {
		b[i] = (a1[i] || 0) - (a2[i] || 0);
	}
	return b;
}

function cleanXMLTree(tree) {
	var t = tree.tags;
	var ctags = {};
	var ttags = {};
	for (var k in t) {
		if (hop.call(t, k)) ctags[k] = cleanXMLTree(t[k]);
	}
	t = tree.tag_k_map;
	for (var k2 in t) {
		if (hop.call(t, k2)) ttags[k2] = cleanXMLTree(t[k2]);
	}
	return {
		tags: ctags,
		tag_k_map: ttags,
		attrs: tree.attrs,
		firstOpen: tree.firstOpen,
		firstClose: tree.firstClose,
		firstIndex: tree.firstIndex,
		lastOpen: tree.lastOpen,
		lastClose: tree.lastClose,
		lastIndex: tree.lastIndex,
		count: tree.count
	};
}

function polluteXMLTree(tree, parent) {
	var t = tree.tags;
	var ctags = {};
	tree = parent
		? {
				parent: parent,
				tags: ctags,
				attrs: tree.attrs,
				firstOpen: tree.firstOpen,
				firstClose: tree.firstClose,
				firstIndex: tree.firstIndex,
				lastOpen: tree.lastOpen,
				lastClose: tree.lastClose,
				lastIndex: tree.lastIndex,
				count: tree.count
			}
		: { tags: ctags };
	for (var k in t) {
		if (hop.call(t, k)) ctags[k] = polluteXMLTree(t[k], tree);
	}
	return tree;
}

function cleanFopt(fopt) {
	if (!fopt) return null;
	return {
		fileCount: fopt.fileCount,
		fileOffset: fopt.fileOffset,
		byteOffset: fopt.byteOffset,
		bytesInput: fopt.bytesInput,
		bytesOutput: fopt.bytesOutput,
		bytesInputPos: fopt.bytesInputPos,
		bytesOutputPos: fopt.bytesOutputPos,
		bitOffset: fopt.bitOffset,
		bitOffsetEnd: fopt.bitOffsetEnd,
		blockCount: fopt.blockCount,
		blockCRC: printCRC(fopt.blockCRC),
		streamPartialCRC: printCRC(fopt.streamPartialCRC),
		streamCRC: printCRC(fopt.streamCRC),
		bzLevelBlock: fopt.bzLevelBlock,
		bzLevelFile: fopt.bzLevelFile
	};
}

function polluteFopt(fopt) {
	if (!fopt) return null;
	return {
		fileCount: fopt.fileCount,
		fileOffset: fopt.fileOffset,
		byteOffset: fopt.byteOffset,
		bytesInput: fopt.bytesInput,
		bytesOutput: fopt.bytesOutput,
		bytesInputPos: fopt.bytesInputPos,
		bytesOutputPos: fopt.bytesOutputPos,
		bitOffset: fopt.bitOffset,
		bitOffsetEnd: fopt.bitOffsetEnd,
		blockCount: fopt.blockCount,
		blockCRC: readCRC(fopt.blockCRC),
		streamPartialCRC: readCRC(fopt.streamPartialCRC),
		streamCRC: readCRC(fopt.streamCRC),
		bzLevelBlock: fopt.bzLevelBlock,
		bzLevelFile: fopt.bzLevelFile
	};
}

function cleanBzStats(bzStats) {
	var cfs = [];
	for (var i = 0; i < bzStats.length; i++) {
		var fi = bzStats[i];
		var cbs = [];
		for (var j = 0; j < fi.blocks.length; j++) {
			var fbi = fi.blocks[j];
			cbs.push({
				crc: printCRC(fbi.crc),
				crcStream: printCRC(fbi.crcStream),
				offsetStart: fbi.offsetStart,
				offsetEnd: fbi.offsetEnd,
				offsetBit: fbi.offsetBit,
				input: fbi.input,
				output: fbi.output
			});
		}
		cfs.push({
			blocks: cbs,
			crc: printCRC(fi.crc),
			offsetStart: fi.offsetStart,
			offsetEnd: fi.offsetEnd,
			offsetBitEnd: fi.offsetBitEnd,
			input: fi.input,
			output: fi.output,
			level: fi.level
		});
	}
	return cfs;
}

function polluteBzStats(bzStats) {
	var cfs = [];
	for (var i = 0; i < bzStats.length; i++) {
		var fi = bzStats[i];
		var cbs = [];
		for (var j = 0; j < fi.blocks.length; j++) {
			var fbi = fi.blocks[j];
			cbs.push({
				crc: readCRC(fbi.crc),
				crcStream: readCRC(fbi.crcStream),
				offsetStart: fbi.offsetStart,
				offsetEnd: fbi.offsetEnd,
				offsetBit: fbi.offsetBit,
				input: fbi.input,
				output: fbi.output
			});
		}
		cfs.push({
			blocks: cbs,
			crc: readCRC(fi.crc),
			offsetStart: fi.offsetStart,
			offsetEnd: fi.offsetEnd,
			offsetBitEnd: fi.offsetBitEnd,
			input: fi.input,
			output: fi.output,
			level: fi.level
		});
	}
	return cfs;
}

function searchHexPi(fopt) {
	var pi = '314159265359';
	var bzRead = fopt.sread;
	bzRead.fillBufferDisabled = true;
	var m = SeekBzip.searchHexString(pi, fopt.bz, bzRead, {
		searchMult: 10,
		onBitStart: function (bit) {
			console.error('---- bit ' + bit + ' ----', {
				fp: bzRead.filePos,
				p: bzRead.pos,
				e: bzRead.end
			});
			console.error(bzRead.lastSeek);
		},
		onFound: function (res, bit) {
			console.error('. '.concat(res.c, ' ', res.n, ' ', res.ix, ' ', res.s1, '-', res.s2));
		}
	});
	for (var i = 0; i < m.length; i++) {
		var mil = m[i].length;
		console.error(': bit ' + i + (mil ? ' - ' + mil + ' matches' : ''));
		for (var j = 0; j < mil; j++) {
			console.error(m[i][j]);
		}
	}
	bzRead.fillBufferDisabled = false;
}

/** Default max wait for a soft stop to reach a good boundary (ms). */
var DEFAULT_SOFT_STOP_MS = 30000;

/**
 * Normalize in-progress bzip member stats (handles legacy JSON without this field).
 */
function ensureBzFile(file, nextOffset) {
	var off = nextOffset || 0;
	if (!file || typeof file !== 'object') {
		return {
			blocks: [],
			crc: null,
			offsetStart: off,
			offsetEnd: off,
			offsetBitEnd: 0,
			input: 0,
			output: 0,
			level: 0
		};
	}
	if (!Array.isArray(file.blocks)) file.blocks = [];
	if (file.offsetStart == null) file.offsetStart = off;
	if (file.offsetEnd == null) file.offsetEnd = off;
	if (file.input == null) file.input = 0;
	if (file.output == null) file.output = 0;
	return file;
}

function polluteBzCurrentFile(file, nextOffset) {
	var f = ensureBzFile(file, nextOffset);
	if (f.crc != null) f.crc = readCRC(f.crc);
	for (var j = 0; j < f.blocks.length; j++) {
		var b = f.blocks[j];
		if (b) {
			b.crc = readCRC(b.crc);
			b.crcStream = readCRC(b.crcStream);
		}
	}
	return f;
}

/**
 * @param {object} options
 * @param {string} options.inputPath - path to .osm.bz2
 * @param {string} [options.statsPath] - stats JSON path (default: inputPath + '-stats.json')
 * @param {boolean} [options.quiet=false] - suppress progress / messages
 * @param {boolean} [options.resume=true] - load stats and continue if present
 * @param {boolean} [options.saveStats=true] - write stats JSON on finish
 * @param {boolean} [options.saveBzStatsSeparate=false] - write *-bzip.json sidecars
 *   (off by default). If false, bzip/bzStats are discarded (never embedded in main JSON).
 * @param {string} [options.bzStatsPath] - path for stats sidecar bzip JSON
 *   (default: statsPath with `-bzip` before `.json`, e.g. foo-stats-bzip.json)
 * @param {string} [options.resultsBzStatsPath] - path for results sidecar bzip JSON
 *   (default: resultsPath with `-bzip` before `.json`)
 * @param {boolean} [options.saveBzStatsPerMember=false] - also write one JSON per
 *   completed bzip member under `{bzStatsPath without .json}/` (implies separate)
 * @param {boolean} [options.strictXmlStack=false] - throw on XML open/close mismatch
 *   (default: recover with warning so planet dumps can continue)
 * @param {number|null} [options.checkpointEveryFiles=300] - stop after N completed
 *   bzip members for checkpoint (null = never stop early for this reason)
 * @param {boolean} [options.syncSchedule=false] - use process.nextTick instead of setTimeout
 * @param {number} [options.softStopMaxMs=30000] - max wait after soft-stop for a good boundary
 * @param {function} [options.onControl] - receives { softStop, hardStop }
 * @returns {Promise<object>} result snapshot
 */
function defaultBzStatsPath(jsonPath) {
	return String(jsonPath).replace(/\.json$/i, '') + '-bzip.json';
}

function runProcess(options) {
	options = options || {};
	var inputPath = options.inputPath;
	if (!inputPath) {
		return Promise.reject(new Error('runProcess: options.inputPath is required'));
	}
	var statsPath =
		options.statsPath || inputPath.replace(/\.bz2$/i, '') + '-stats.json';
	var resultsPath =
		options.resultsPath || inputPath.replace(/\.bz2$/i, '') + '-results.json';
	var quiet = !!options.quiet;
	var resume = options.resume !== false;
	var saveStats = options.saveStats !== false;
	var saveBzStatsPerMember = !!options.saveBzStatsPerMember;
	// Sidecars only when requested (per-member implies separate)
	var saveBzStatsSeparate =
		!!options.saveBzStatsSeparate || saveBzStatsPerMember;
	var strictXmlStack = !!options.strictXmlStack;
	var bzStatsPath =
		options.bzStatsPath || defaultBzStatsPath(statsPath);
	var resultsBzStatsPath =
		options.resultsBzStatsPath || defaultBzStatsPath(resultsPath);
	var checkpointEveryFiles =
		options.checkpointEveryFiles === undefined
			? 300
			: options.checkpointEveryFiles;
	var syncSchedule = !!options.syncSchedule;
	var softStopMaxMs =
		options.softStopMaxMs == null ? DEFAULT_SOFT_STOP_MS : options.softStopMaxMs;

	return new Promise(function (resolve, reject) {
		var finished = false;
		var softStopRequested = false;
		var softStopDeadline = 0;
		var hardStopRequested = false;
		var progressLegendPrinted = false;
		var xmlStackMismatches = 0;
		var geoSignals = createGeocodeSignals();
		var coordLayout = CoordLayout.createCoordLayout({
			sampleSize: options.coordSampleSize,
			jumpSmallDeg: options.jumpSmallDeg
		});
		var pendingNode = null;

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
			if (hardStopRequested || finished) return;
			if (softStopRequested) return;
			softStopRequested = true;
			softStopDeadline = Date.now() + softStopMaxMs;
			if (!quiet) {
				console.error(
					'\n\nSoft-stop: parando no melhor ponto ' +
						'(fim de membro bzip, ou XML perto da raiz; ' +
						'forçado em até ' +
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
			softStopDeadline = Date.now(); // stop after current block
			if (!quiet) {
				console.error('\nHard-stop: encerrando após o bloco atual...\n');
			}
		}

		if (typeof options.onControl === 'function') {
			options.onControl({
				softStop: requestSoftStop,
				hardStop: requestHardStop
			});
		}

		var chunkCount = [0, 0, 0];
		var chunkPos = [0, 0, 0];
		var previousChunkCount = chunkCount.slice();
		var previousChunkPos = chunkPos.slice();

		function setChunkPos(chunk, i) {
			chunkCount[i] += 1;
			chunkPos[i] += chunk.length;
		}
		function streamPos(i) {
			return new stream.Transform({
				highWaterMark: 0,
				transform: function (chunk, encoding, callback) {
					setChunkPos(chunk, i);
					callback(null, chunk);
				}
			});
		}

		function printXMLStructure(cxsNext, tagName) {
			var ct = cxsNext.tags;
			var tags = [];
			var tagtags = [];
			var tagcount = 0;
			var k;
			for (k in ct) {
				if (hop.call(ct, k)) tags.push(printXMLStructure(ct[k], k));
			}
			ct = cxsNext.tag_k_map;
			if (ct)
				for (k in ct) {
					if (hop.call(ct, k)) {
						tagcount += ct[k].count;
						var sub = printXMLStructure(ct[k]);
						if (sub) tagtags.push(sub);
					}
				}
			if (tagcount) {
				tagtags = tagtags.join(',');
				tagtags =
					'tag ' +
					formatCount(tagcount) +
					(tagtags ? '(' + tagtags + ')' : '');
				tags.push(tagtags);
			}
			tags = tags.join(',');
			if (tagName) {
				tags =
					tagName +
					' ' +
					formatCount(cxsNext.count) +
					(tags ? '(' + tags + ')' : '');
			}
			return tags;
		}

		function printProgressLegend() {
			if (quiet || progressLegendPrinted) return;
			progressLegendPrinted = true;
			console.error(
				[
					'',
					'Legenda da linha de progresso (campos separados por espaços e "/"):',
					'  1) percent     — % do arquivo .bz2 já lido (chunkPos[0] / tamanho)',
					'  2) speed       — taxa de leitura do .bz2 nesta execução (dados comprimidos/s)',
					'  3) elapsed     — tempo decorrido desta execução (m:ss ou h:mm)',
					'  4) eta         — tempo estimado restante nesta velocidade',
					'  /',
					'  5) counts      — contagem de chunks: [0]=leituras bzip input, [1]=saída',
					'                  descomprimida, [2]=linhas XML (após LineSplitter)',
					'  /',
					'  6) sizes       — bytes acumulados nos mesmos 3 estágios (input bz2, xml',
					'                  descomprimido, xml em linhas)',
					'  7) bzF.B       — bz<membro_bzip>.<bloco_no_membro> (índices 0-based de',
					'                  progresso no stream concatenated bzip2)',
					'  /',
					'  8) xmlTree     — árvore resumida de tags OSM contadas até agora',
					'  9) geoSignals  — sinais de geocoding: n=nodes (ll=com lat+lon),',
					'                  w=ways, addr=/st=/hn= tags addr:*, name, place, hwy',
					'                  Prefixo GEO (maiúsculo) = material provável',
					'                  (pontos com coordenada + endereço/nome/place)',
					' 10) layout      — ordem lat/lon no stream: seq=fração de saltos <0.01°,',
					'                  meanJump em graus (não km), blocks/samples no mapa',
					'',
					'Soft-stop (Ctrl+C): tenta parar no fim de um membro bzip ou com XML perto',
					'da raiz; no máximo ~' +
						Math.round(softStopMaxMs / 1000) +
						's. Segundo Ctrl+C: hard-stop / saída forçada.',
					''
				].join('\n')
			);
			// Header on stdout so it sits directly above the \r progress line
			console.log(
				[
					'percent',
					'speed',
					'elapsed',
					'eta',
					'/',
					'counts(in,out,lines)',
					'/',
					'sizes(in,out,lines)',
					'bzFile.Block',
					'/',
					'xmlTree',
					'/',
					'geoSignals',
					'/',
					'layout'
				].join('  ')
			);
		}

		function streamPosMeta() {
			var fo = fopt || {};
			return {
				bzFile: bzIndexFile,
				bzBlock: bzIndexBlock,
				fileOffset: fo.fileOffset || bzNextFileOffset || 0,
				byteOffset: fo.byteOffset || 0,
				byteOffsetStart: fo.byteOffsetStart || 0,
				chunkPos: chunkPos.slice()
			};
		}

		function printXMLStats() {
			if (quiet) return;
			printProgressLegend();
			var time = (Date.now() - tstart) * 0.001;
			var posInput = chunkPos[0];
			var percent = Number((100 * posInput) / fstat.size);
			var runInput = posInput - previousChunkPos[0];
			var speed = time > 0 ? runInput / time : 0;
			var remain =
				speed > 0 ? (fstat.size - posInput) / speed : Number.POSITIVE_INFINITY;
			var remainStr =
				isFinite(remain) && remain >= 0 ? printTime(remain) : '--:--';
			process.stdout.write(
				'\r' +
					percent.toFixed(3) +
					' ' +
					dataSize(speed) +
					'/s' +
					' ' +
					printTime(time) +
					' ' +
					remainStr +
					' / ' +
					chunkCount.map(formatCount).join(', ') +
					' / ' +
					chunkPos.map(dataSize).join(', ') +
					' bz' +
					bzIndexFile +
					'.' +
					bzIndexBlock +
					' /' +
					printXMLStructure(chunkXMLStatsRoot) +
					' / ' +
					formatGeocodeSignals(geoSignals) +
					' / ' +
					CoordLayout.formatCoordLayout(coordLayout) +
					'   '
			);
		}

		/**
		 * Decide whether to stop after the block/member just processed.
		 * Prefers: end of bzip member > XML near root > deadline forced stop.
		 */
		function shouldStopAfter(opt) {
			if (hardStopRequested) return true;
			if (!softStopRequested) return false;
			var timedOut = Date.now() >= softStopDeadline;
			var atMemberEnd = !!(opt && opt.streamCRC);
			var nearRoot = chunkXMLTags.length <= 1;
			if (atMemberEnd) return true;
			if (nearRoot) return true;
			if (timedOut) return true;
			return false;
		}

		var fstat;
		try {
			fstat = fs.statSync(inputPath);
		} catch (e) {
			return fail(e);
		}

		var tstart = Date.now();
		var totaltime = 0;
		var previousRuns = [];
		var fd = fs.openSync(inputPath, 'r');
		var strDecoder = new StreamingDecoder({ highWaterMark: 256 });
		var fopt;
		var stoppedEarly = false;
		var stopReason = null;

		function bzReadBuffer(bzRead) {
			setChunkPos(
				{ length: bzRead.filePos - bzRead.lastStats.filePos },
				0
			);
		}
		var bzRead = SeekBzip.fdReadFile(fd, 4096, 0, bzReadBuffer);
		var bzWrite = SeekBzip.readBlock.makeOutStream(outPrint, 1024 * 1024);
		function outPrint(buf) {
			setChunkPos(buf, 1);
			strDecoder.write(buf);
		}

		var bzStats = [];
		var bzIndexFile = 0;
		var bzIndexBlock = 0;
		var bzNextFileOffset = 0;
		var bzFile = bzInitFile();
		function bzInitFile() {
			return {
				blocks: [],
				crc: null,
				offsetStart: bzNextFileOffset,
				offsetEnd: bzNextFileOffset,
				offsetBitEnd: 0,
				input: 0,
				output: 0,
				level: 0
			};
		}
		function bzFinishBlock(opt) {
			// Defensive: legacy resume JSON may omit current.bzCurrentFile
			if (!bzFile || !Array.isArray(bzFile.blocks)) {
				bzFile = ensureBzFile(bzFile, bzNextFileOffset);
			}
			// Nodes closed during this block used bzIndexBlock as index; finalize before ++
			var finishedBlockIndex = bzIndexBlock;
			CoordLayout.finalizeBzBlockLayout(coordLayout, {
				bzFile: bzIndexFile,
				bzBlock: finishedBlockIndex,
				fileOffset: opt.fileOffset || bzNextFileOffset || 0,
				byteOffsetEnd: opt.byteOffset || 0,
				chunkPosIn: chunkPos[0],
				chunkPosOut: chunkPos[1]
			});
			bzIndexBlock++;
			bzFile.blocks.push({
				crc: opt.blockCRC,
				crcStream: opt.streamPartialCRC,
				offsetStart: opt.byteOffsetStart,
				offsetEnd: opt.byteOffset,
				offsetBit: opt.bitOffset,
				input: opt.bytesInput,
				output: opt.bytesOutput
			});
			bzFile.input += opt.bytesInput;
			bzFile.output += opt.bytesOutput;
		}
		function bzFinishFile(opt) {
			bzIndexBlock = 0;
			bzIndexFile++;
			bzNextFileOffset = opt.fileOffset;
			bzFile.crc = opt.streamCRC;
			bzFile.offsetEnd = bzNextFileOffset;
			bzFile.offsetBitEnd = opt.bitOffsetEnd;
			bzFile.level = opt.bzLevelFile;
			bzStats.push(bzFile);
			bzFile = bzInitFile();
		}

		var chunkXMLTags = [];
		var chunkXMLIndex = [];
		var chunkXMLStatsRoot = { tags: {} };
		var chunkXMLCurrent = 0;
		var chunkXMLStatsCurrent = chunkXMLStatsRoot;
		var chunkXMLLastChunks = new Array(20);
		var chunkXMLFirstChunks = [];
		var chunkXMLRemain;
		var xmlPendingName = null;
		var xmlPendingAttrs = null;

		function getXMLTagPosStats() {
			return {
				count: chunkCount.slice(),
				pos: chunkPos.slice(),
				bzFile: bzIndexFile,
				bzBlock: bzIndexBlock
			};
		}

		function xmlMergeAttrsStats(target, source, mapIgnore) {
			for (var k in source) {
				if (hop.call(source, k) && !(mapIgnore && mapIgnore[k])) {
					var stats = target[k];
					if (stats) {
						stats.last = chunkXMLCurrent;
						stats.lastVal = source[k];
					} else {
						target[k] = stats = {
							first: chunkXMLCurrent,
							firstVal: source[k],
							last: null,
							lastVal: null,
							count: 0
						};
					}
					stats.count += 1;
				}
			}
		}

		function rememberXmlEvent(snapshot) {
			chunkXMLLastChunks = chunkXMLLastChunks.slice(1, 10).concat([snapshot]);
			if (chunkXMLFirstChunks.length < 10) {
				chunkXMLFirstChunks.push(snapshot);
			}
		}

		function onXmlOpenTag(cname, attrs) {
			var cxsNext;
			var tag_k;
			var tag_k_map;
			attrs = attrs || {};
			rememberXmlEvent({ event: 'open', name: cname, attrs: attrs });
			noteGeocodeOpen(geoSignals, cname, attrs);

			if (cname === 'node') {
				pendingNode = {
					lat: attrs.lat,
					lon: attrs.lon,
					id: attrs.id,
					hasAddr: false,
					hasName: false,
					hasPlace: false
				};
			} else if (cname === 'tag' && pendingNode && chunkXMLTags.length) {
				// parent open name is last on stack before we push 'tag'
				var parentName = chunkXMLTags[chunkXMLTags.length - 1];
				if (parentName === 'node' && attrs.k) {
					var kk = String(attrs.k);
					if (kk.indexOf('addr:') === 0) pendingNode.hasAddr = true;
					else if (kk === 'name' || kk.indexOf('name:') === 0)
						pendingNode.hasName = true;
					else if (kk === 'place') pendingNode.hasPlace = true;
				}
			}

			// densify mapping when address-like material appears
			if (
				cname === 'tag' &&
				attrs.k &&
				(String(attrs.k).indexOf('addr:') === 0 ||
					attrs.k === 'name' ||
					attrs.k === 'place')
			) {
				CoordLayout.maybeEnableAddressMapping(
					coordLayout,
					geoSignals,
					streamPosMeta()
				);
			}

			chunkXMLTags.push(cname);
			chunkXMLIndex.push(chunkXMLCurrent);
			if (cname === 'tag' && hop.call(attrs, 'k')) {
				tag_k = attrs.k;
				tag_k_map = chunkXMLStatsCurrent.tag_k_map;
				if (!tag_k_map) {
					chunkXMLStatsCurrent.tag_k_map = tag_k_map = {};
				}
				cxsNext = tag_k_map[tag_k];
			} else {
				cxsNext = chunkXMLStatsCurrent.tags[cname];
			}
			if (!cxsNext) {
				cxsNext = {
					parent: void 0,
					tags: {},
					attrs: {},
					firstOpen: getXMLTagPosStats(),
					firstClose: null,
					firstIndex: chunkXMLCurrent,
					lastOpen: null,
					lastClose: null,
					lastIndex: null,
					count: 0
				};
				if (tag_k_map) tag_k_map[tag_k] = cxsNext;
				else chunkXMLStatsCurrent.tags[cname] = cxsNext;
			}
			cxsNext.parent = chunkXMLStatsCurrent;
			cxsNext.lastOpen = getXMLTagPosStats();
			cxsNext.lastIndex = chunkXMLCurrent;
			cxsNext.count += 1;
			xmlMergeAttrsStats(
				cxsNext.attrs,
				attrs,
				tag_k_map ? { k: true } : null
			);
			chunkXMLStatsCurrent = cxsNext;
			chunkXMLCurrent = 0;
		}

		/**
		 * Pop open-tag stack for cname. Resilient recovery for planet dumps:
		 * if the stack top does not match (e.g. after soft-stop/resume mid-stream),
		 * pop until we find cname, or ignore a spurious close — unless strictXmlStack.
		 */
		function onXmlCloseTag(cname) {
			rememberXmlEvent({ event: 'close', name: cname });

			if (!chunkXMLTags.length) {
				xmlStackMismatches++;
				var emptyMsg =
					'XML stack empty on close </' + cname + '> (mismatch #' +
					xmlStackMismatches +
					')';
				if (strictXmlStack) throw new Error(emptyMsg);
				if (!quiet) console.error('Aviso: ' + emptyMsg);
				printXMLStats();
				return;
			}

			var tag = chunkXMLTags[chunkXMLTags.length - 1];
			if (tag !== cname) {
				xmlStackMismatches++;
				// Try to find cname deeper in the stack (unclosed intermediates)
				var foundAt = -1;
				for (var si = chunkXMLTags.length - 1; si >= 0; si--) {
					if (chunkXMLTags[si] === cname) {
						foundAt = si;
						break;
					}
				}
				if (foundAt === -1) {
					var missMsg =
						'XML close </' +
						cname +
						'> sem open correspondente (topo era <' +
						tag +
						'>, stack=[' +
						chunkXMLTags.join('>') +
						']) mismatch #' +
						xmlStackMismatches;
					if (strictXmlStack) {
						throw new Error(
							'Error tracking XML tags: expected ' + tag + ' but got ' + cname
						);
					}
					if (!quiet) console.error('Aviso: ' + missMsg + ' — ignorando close');
					printXMLStats();
					return;
				}
				// Pop intermediates until cname
				if (!quiet) {
					console.error(
						'Aviso: XML stack dessincronizada (esperado </' +
							tag +
							'>, veio </' +
							cname +
							'>); fechando ' +
							(chunkXMLTags.length - 1 - foundAt) +
							' tag(s) intermediária(s) [' +
							chunkXMLTags.slice(foundAt + 1).join('>') +
							'] mismatch #' +
							xmlStackMismatches
					);
				}
				while (chunkXMLTags.length - 1 > foundAt) {
					chunkXMLTags.pop();
					chunkXMLIndex.pop();
					if (chunkXMLStatsCurrent && chunkXMLStatsCurrent.parent) {
						if (!chunkXMLStatsCurrent.firstClose) {
							chunkXMLStatsCurrent.firstClose = getXMLTagPosStats();
						}
						chunkXMLStatsCurrent.lastClose = getXMLTagPosStats();
						chunkXMLStatsCurrent = chunkXMLStatsCurrent.parent;
					}
				}
			}

			tag = chunkXMLTags.pop();
			var index = chunkXMLIndex.pop();
			chunkXMLCurrent = (index != null ? index : 0) + 1;

			// Coordinate layout: closed node with lat/lon
			if (cname === 'node' && pendingNode) {
				if (pendingNode.lat != null && pendingNode.lon != null) {
					CoordLayout.noteNodeCoord(
						coordLayout,
						pendingNode,
						streamPosMeta()
					);
				}
				pendingNode = null;
			} else if (cname === 'node') {
				pendingNode = null;
			}

			if (chunkXMLStatsCurrent) {
				if (!chunkXMLStatsCurrent.firstClose) {
					chunkXMLStatsCurrent.firstClose = getXMLTagPosStats();
				}
				chunkXMLStatsCurrent.lastClose = getXMLTagPosStats();
				var cxsNext = chunkXMLStatsCurrent.parent;
				if (cxsNext) {
					chunkXMLStatsCurrent = cxsNext;
				} else if (chunkXMLTags.length === 0) {
					// closed root — stay at stats root
					chunkXMLStatsCurrent = chunkXMLStatsRoot;
				} else {
					var lostMsg = 'Error tracking XML tags: parent got lost';
					if (strictXmlStack) throw new Error(lostMsg);
					if (!quiet) console.error('Aviso: ' + lostMsg);
					chunkXMLStatsCurrent = chunkXMLStatsRoot;
				}
			}
			printXMLStats();
		}

		function onXmlParserEvent(ev) {
			var tag = ev.tag;
			var attr = ev.attr;
			switch (ev.name) {
				case 'startTag':
					xmlPendingName = null;
					xmlPendingAttrs = {};
					break;
				case 'tagName':
					if (tag) xmlPendingName = tag.name;
					break;
				case 'tagAttribute':
					if (attr && attr.name != null) {
						xmlPendingAttrs = xmlPendingAttrs || {};
						xmlPendingAttrs[attr.name] =
							attr.value == null ? '' : String(attr.value);
					}
					break;
				case 'endTag':
					if (tag) {
						if (xmlPendingName == null && tag.name != null) {
							xmlPendingName = tag.name;
						}
						if (tag.close) {
							onXmlCloseTag(xmlPendingName);
						} else {
							onXmlOpenTag(xmlPendingName, xmlPendingAttrs || {});
							if (tag.selfClose) onXmlCloseTag(xmlPendingName);
						}
					}
					xmlPendingName = null;
					xmlPendingAttrs = null;
					break;
				case 'endStream':
					if (chunkXMLRemain) {
						throw new Error('XMLParser: unparsed remain already filled');
					}
					chunkXMLRemain = {
						buffer: ev.parser.buffer || '',
						pos: ev.parser.pos
					};
					break;
			}
		}

		var xmlParser = new XMLParser({ event: onXmlParserEvent });
		var xmlSink = new stream.Writable({
			highWaterMark: 256,
			write: function (chunk, encoding, callback) {
				try {
					var text =
						typeof chunk === 'string'
							? chunk
							: chunk.toString(
									encoding === 'buffer' ? 'utf8' : encoding || 'utf8'
								);
					xmlParser.write(text);
					callback();
				} catch (e) {
					callback(e);
				}
			},
			final: function (callback) {
				try {
					xmlParser.end('');
					callback();
				} catch (e) {
					callback(e);
				}
			}
		});

		var pipe = [
			{ s: strDecoder, n: 'strDecoder' },
			{ s: new LineSplitter({ highWaterMark: 256 }), n: 'lineSplitter' },
			{ s: streamPos(2), n: 'streamPos(2)' },
			{ s: xmlSink, n: 'xmlParser' }
		];
		for (var i = pipe.length - 1; i >= 0; i--) {
			if (i > 0) pipe[i - 1].s.pipe(pipe[i].s);
		}

		function buildResult(err) {
			var root = chunkXMLStatsRoot.tags || {};
			var geoSnap = snapshotGeocodeSignals(geoSignals);
			var layoutSnap = CoordLayout.snapshotCoordLayout(coordLayout);
			var result = {
				error: err || null,
				stoppedEarly: stoppedEarly,
				inputPath: inputPath,
				statsPath: statsPath,
				resultsPath: resultsPath,
				fileSize: fstat.size,
				chunkCount: chunkCount.slice(),
				chunkPos: chunkPos.slice(),
				bzIndexFile: bzIndexFile,
				bzIndexBlock: bzIndexBlock,
				xmlStackMismatches: xmlStackMismatches,
				geocodeSignals: geoSnap,
				coordLayout: layoutSnap,
				xml: {
					tags: chunkXMLTags.slice(),
					openCounts: {
						osm: root.osm ? root.osm.count : 0,
						node: root.node ? root.node.count : 0,
						way: root.way ? root.way.count : 0,
						relation: root.relation ? root.relation.count : 0,
						bounds: root.bounds ? root.bounds.count : 0,
						tag: root.tag ? root.tag.count : 0
					},
					// nested under osm in stats tree
					nodeCount: root.osm && root.osm.tags && root.osm.tags.node
						? root.osm.tags.node.count
						: 0,
					wayCount: root.osm && root.osm.tags && root.osm.tags.way
						? root.osm.tags.way.count
						: 0,
					root: cleanXMLTree(chunkXMLStatsRoot),
					geocodeSignals: geoSnap
				},
				elapsedMs: Date.now() - tstart
			};
			if (saveBzStatsSeparate) {
				result.bzStatsPath = resultsBzStatsPath;
			}
			// never embed bzStats in the serializable result shape for disk
			return result;
		}

		function writeFileAsync(filePath, data, cb) {
			fs.writeFile(filePath, data, cb);
		}

		function ensureDirFor(filePath, cb) {
			var dir = path.dirname(filePath);
			fs.mkdir(dir, { recursive: true }, function (err) {
				// ignore EEXIST-equivalent; mkdir recursive is fine on Node 10+
				if (err && err.code !== 'EEXIST') cb(err);
				else cb(null);
			});
		}

		/**
		 * Write bzip members/blocks to a sidecar JSON (and optional per-member files).
		 * No-op when saveBzStatsSeparate is false.
		 */
		function saveBzSidecar(filePath, cleanedBz, label, callback) {
			if (!saveBzStatsSeparate) {
				callback && callback(null, null);
				return;
			}
			var aggregateJson = JSON.stringify(cleanedBz);
			writeFileAsync(filePath, aggregateJson, function (aggErr) {
				if (aggErr) {
					if (!quiet) {
						console.error('Erro ao salvar ' + label, aggErr);
					}
					return callback && callback(aggErr);
				}
				if (!quiet) {
					console.error(
						label +
							' salvo: ' +
							filePath +
							' (' +
							cleanedBz.length +
							' membros)'
					);
				}
				if (!saveBzStatsPerMember || cleanedBz.length === 0) {
					return callback && callback(null, filePath);
				}
				var membersDir = filePath.replace(/\.json$/i, '');
				ensureDirFor(path.join(membersDir, '0.json'), function (dirErr) {
					if (dirErr) {
						if (!quiet) {
							console.error(
								'Erro criando pasta de ' + label + ' por membro',
								dirErr
							);
						}
						return callback && callback(dirErr);
					}
					var i = 0;
					function writeNext(err) {
						if (err) return callback && callback(err);
						if (i >= cleanedBz.length) {
							if (!quiet) {
								console.error(label + ' por membro em: ' + membersDir + '/');
							}
							return callback && callback(null, filePath);
						}
						var memberPath = path.join(
							membersDir,
							('0000' + i).slice(-4) + '.json'
						);
						var body = JSON.stringify(cleanedBz[i]);
						i++;
						writeFileAsync(memberPath, body, writeNext);
					}
					writeNext(null);
				});
			});
		}

		function saveProcessStats(callback) {
			var cleanedBz = cleanBzStats(bzStats);
			if (!saveStats) {
				// Main stats off: only write bzip sidecar if requested
				return saveBzSidecar(bzStatsPath, cleanedBz, 'stats-bzip', function (e) {
					callback && callback(e);
				});
			}
			var time = (Date.now() - tstart) * 0.001;
			var percent = Number(chunkPos[0] / fstat.size);
			var runChunkCount = subtractArrays(chunkCount, previousChunkCount);
			var runChunkPos = subtractArrays(chunkPos, previousChunkPos);
			var speed = runChunkPos[0] / time;
			// Never embed "bzip" — only optional pointer when sidecars are enabled
			var geoSnap = snapshotGeocodeSignals(geoSignals);
			var layoutSnap = CoordLayout.snapshotCoordLayout(coordLayout);
			var payload = {
				xml: {
					tags: chunkXMLTags,
					index: chunkXMLIndex,
					current: chunkXMLCurrent,
					root: cleanXMLTree(chunkXMLStatsRoot),
					first: chunkXMLFirstChunks,
					last: chunkXMLLastChunks,
					remain: chunkXMLRemain,
					geocodeSignals: geoSnap
				},
				geocodeSignals: geoSnap,
				coordLayout: layoutSnap,
				fopt: cleanFopt(fopt),
				runs: previousRuns.concat([
					{
						start: new Date(tstart).toISOString(),
						time: Math.round(time),
						timeStr: printTime(time),
						percent: percent,
						speed: Math.round(speed),
						speedStr: dataSize(speed),
						chunkCount: runChunkCount,
						chunkPos: runChunkPos,
						endingCount: chunkCount,
						endingPos: chunkPos,
						bzFile: bzIndexFile,
						bzBlock: bzIndexBlock
					}
				]),
				current: {
					percent: percent,
					time: totaltime + Math.round(time),
					timeStr: printTime(totaltime + time),
					chunkCount: chunkCount,
					chunkPos: chunkPos,
					bzFile: bzIndexFile,
					bzBlock: bzIndexBlock,
					bzNextFileOffset: bzNextFileOffset,
					bzCurrentFile: bzFile
				}
			};
			if (saveBzStatsSeparate) {
				payload.bzipPath = bzStatsPath;
			}
			// else: discard cleanedBz for disk (kept only in memory until process ends)
			fs.writeFile(statsPath, JSON.stringify(payload), function (err) {
				if (err) {
					if (!quiet) {
						console.error('Erro ao salvar o arquivo de estatísticas', err);
					}
					return callback && callback(err);
				}
				if (!quiet) {
					console.error('Arquivo de estatísticas salvo com sucesso: ' + statsPath);
				}
				saveBzSidecar(bzStatsPath, cleanedBz, 'stats-bzip', function (bzErr) {
					callback && callback(bzErr || null);
				});
			});
		}

		function loadBzStatsFromDisk(stats, callback) {
			// Inline first (default mode / legacy); then sidecar if bzipPath or separate mode
			var inline = stats && stats.bzip;
			if (inline && inline.length) {
				return callback(null, inline);
			}
			var fromPath =
				(stats && stats.bzipPath) ||
				(saveBzStatsSeparate ? bzStatsPath : null) ||
				defaultBzStatsPath(statsPath);
			if (!fromPath) {
				return callback(null, inline || []);
			}
			fs.readFile(fromPath, { encoding: 'utf8' }, function (err, data) {
				if (err) {
					if (err.code === 'ENOENT') return callback(null, inline || []);
					return callback(err);
				}
				try {
					callback(null, JSON.parse(data));
				} catch (parseErr) {
					callback(parseErr);
				}
			});
		}

		function readProcessStats(callback) {
			if (!resume) return callback(null, null);
			fs.readFile(statsPath, { encoding: 'utf8' }, function (err, data) {
				if (err && err.code !== 'ENOENT') return callback(err);
				if (!data) return callback(null, null);
				var stats;
				try {
					stats = JSON.parse(data);
				} catch (parseErr) {
					return callback(parseErr);
				}
				loadBzStatsFromDisk(stats, function (bzErr, bzipData) {
					if (bzErr) return callback(bzErr);
					stats.bzip = bzipData || [];
					callback(null, stats);
				});
			});
		}

		function continueProcess(err, stats) {
			if (err) return bzEnd(err);
			if (stats) {
				try {
					if (!stats.current) {
						throw new Error(
							'Stats file missing "current" section: ' + statsPath
						);
					}
					chunkXMLStatsRoot = polluteXMLTree(
						(stats.xml && stats.xml.root) || { tags: {} }
					);
					chunkXMLTags = (stats.xml && stats.xml.tags) || [];
					chunkXMLIndex = (stats.xml && stats.xml.index) || [];
					chunkXMLCurrent =
						stats.xml && stats.xml.current != null ? stats.xml.current : 0;
					chunkXMLStatsCurrent = chunkXMLStatsRoot;
					for (var ti = 0; ti < chunkXMLTags.length; ti++) {
						var nextTag = chunkXMLTags[ti];
						var nextNode =
							chunkXMLStatsCurrent.tags &&
							chunkXMLStatsCurrent.tags[nextTag];
						if (!nextNode) {
							if (!quiet) {
								console.error(
									'Aviso: não achei tag "' +
										nextTag +
										'" na árvore de stats ao retomar; reiniciando ponteiro XML na raiz'
								);
							}
							chunkXMLStatsCurrent = chunkXMLStatsRoot;
							chunkXMLTags = [];
							chunkXMLIndex = [];
							chunkXMLCurrent = 0;
							break;
						}
						chunkXMLStatsCurrent = nextNode;
					}
					// Restore geocode signals (or recompute from tree for older stats)
					if (stats.geocodeSignals || (stats.xml && stats.xml.geocodeSignals)) {
						var savedGeo = stats.geocodeSignals || stats.xml.geocodeSignals;
						geoSignals = createGeocodeSignals();
						var gk;
						for (gk in savedGeo) {
							if (!hop.call(savedGeo, gk) || gk === 'hints') continue;
							if (gk === 'addrByKey' && savedGeo.addrByKey) {
								geoSignals.addrByKey = Object.assign(
									{},
									savedGeo.addrByKey
								);
							} else if (typeof savedGeo[gk] === 'number') {
								geoSignals[gk] = savedGeo[gk];
							}
						}
					} else {
						geoSignals = recomputeGeocodeSignalsFromTree(
							chunkXMLStatsRoot
						);
					}
					if (stats.coordLayout) {
						coordLayout = CoordLayout.restoreCoordLayout(
							stats.coordLayout,
							{
								sampleSize: options.coordSampleSize,
								jumpSmallDeg: options.jumpSmallDeg
							}
						);
					} else {
						coordLayout = CoordLayout.createCoordLayout({
							sampleSize: options.coordSampleSize,
							jumpSmallDeg: options.jumpSmallDeg
						});
					}
					pendingNode = null;
					bzStats = polluteBzStats(stats.bzip || []);
					fopt = polluteFopt(stats.fopt);
					previousRuns = stats.runs || [];
					chunkCount = (stats.current.chunkCount || [0, 0, 0]).slice();
					chunkPos = (stats.current.chunkPos || [0, 0, 0]).slice();
					previousChunkCount = chunkCount.slice();
					previousChunkPos = chunkPos.slice();
					bzIndexFile = stats.current.bzFile || 0;
					bzIndexBlock = stats.current.bzBlock || 0;
					// Legacy stats (2018) omit bzNextFileOffset / bzCurrentFile
					bzNextFileOffset =
						stats.current.bzNextFileOffset != null
							? stats.current.bzNextFileOffset
							: (stats.fopt && stats.fopt.fileOffset) || 0;
					bzFile = polluteBzCurrentFile(
						stats.current.bzCurrentFile,
						bzNextFileOffset
					);
					if (typeof stats.current.time === 'number') {
						totaltime = stats.current.time;
					}
					if (stats.xml && stats.xml.remain && stats.xml.remain.buffer) {
						xmlParser.write(stats.xml.remain.buffer);
					}
					if (!quiet) {
						console.error(
							'Retomando de stats: bz membro ' +
								bzIndexFile +
								' bloco ' +
								bzIndexBlock +
								' offset ' +
								bzNextFileOffset +
								' (' +
								dataSize(chunkPos[0] || 0) +
								' já lidos) | ' +
								formatGeocodeSignals(geoSignals)
						);
					}
				} catch (resumeErr) {
					return bzEnd(resumeErr);
				}
			}
			bzProcess();
		}

		function schedule(fn) {
			if (syncSchedule) process.nextTick(fn);
			else setTimeout(fn, 0);
		}

		function bzProcess() {
			try {
				if (hardStopRequested && ending) return;
				fopt = SeekBzip.readBlock(bzRead, bzWrite, fopt);
				bzWrite.flush();
				if (fopt.streamCRC) {
					bzFinishFile(fopt);
					if (
						checkpointEveryFiles != null &&
						fopt.fileCount % checkpointEveryFiles === 0 &&
						fopt.fileOffset < fstat.size
					) {
						// Periodic checkpoint when more concatenated members remain
						requestSoftStop(
							'checkpoint a cada ' + checkpointEveryFiles + ' membros bzip'
						);
					}
				} else if (fopt.blockCRC) {
					// Guard: never crash on missing bzFile (legacy resume)
					if (!bzFile || !bzFile.blocks) {
						bzFile = ensureBzFile(bzFile, bzNextFileOffset);
					}
					bzFinishBlock(fopt);
				}

				var atEof = fopt.fileOffset >= fstat.size;
				var stopNow = !atEof && shouldStopAfter(fopt);
				if (stopNow) {
					stoppedEarly = true;
					if (fopt.streamCRC) stopReason = 'bzip-member-end';
					else if (chunkXMLTags.length <= 1) stopReason = 'xml-near-root';
					else if (Date.now() >= softStopDeadline) stopReason = 'soft-stop-timeout';
					else if (hardStopRequested) stopReason = 'hard-stop';
					else stopReason = 'soft-stop';
				}
				var more = !stopNow && !atEof;
				var nextFn = more ? bzProcess : bzEnd;
				schedule(nextFn);
			} catch (err) {
				bzEnd(err);
			}
		}

		var ending = false;
		function bzEnd(err) {
			if (ending) return;
			ending = true;
			if (err) {
				if (err.errorCode === SeekBzip.Err.NOT_BZIP_DATA) {
					try {
						searchHexPi(fopt);
					} catch (_) {}
				}
				if (!quiet) {
					console.error(util.inspect(fopt, { depth: 0 }));
					console.error('Teve um erro', err);
				}
				try {
					fs.closeSync(fd);
				} catch (_) {}
				return fail(err);
			}
			if (!quiet) {
				process.stdout.write('\n');
				if (stoppedEarly) {
					console.error(
						'Parou antes do final' +
							(stopReason ? ' (' + stopReason + ')' : '') +
							'. Stats salvos para retomar.'
					);
				} else {
					console.error('Você chegou no final, parabéns!');
				}
			}
			try {
				fs.closeSync(fd);
			} catch (_) {}

			xmlSink.once('finish', function () {
				saveProcessStats(function (saveErr) {
					var cleanedBz = cleanBzStats(bzStats);
					var result = buildResult(saveErr || null);
					result.stoppedEarly = stoppedEarly;
					result.stopReason = stopReason;
					if (saveErr) return fail(saveErr);
					saveResults(
						quiet,
						resultsPath,
						resultsBzStatsPath,
						result,
						cleanedBz,
						saveBzStatsSeparate,
						function (resultErr) {
							if (resultErr) fail(resultErr);
							else {
								// Always on the returned object for API callers;
								// never written into stats/results unless --save-bz-stats
								result.bzStats = cleanedBz;
								ok(result);
							}
						}
					);
				});
			});
			xmlSink.once('error', fail);
			strDecoder.end();
		}

		xmlSink.on('error', function (e) {
			bzEnd(e);
		});

		if (!quiet) {
			console.error('Arquivo:  ' + inputPath);
			console.error('Tamanho:  ' + dataSize(fstat.size));
			console.error('Stats:    ' + statsPath);
			console.error('Results:  ' + resultsPath);
			if (saveBzStatsSeparate) {
				console.error('Stats-bzip:   ' + bzStatsPath + ' (--save-bz-stats)');
				console.error('Results-bzip: ' + resultsBzStatsPath);
				if (saveBzStatsPerMember) {
					console.error(
						'bz por membro: ' + bzStatsPath.replace(/\.json$/i, '') + '/'
					);
				}
			} else {
				console.error(
					'bzip/bzStats: descartados ao salvar (use --save-bz-stats para *-bzip.json)'
				);
			}
			printProgressLegend();
		}

		readProcessStats(continueProcess);
	});
}

function parseCliArgs(argv) {
	argv = argv || process.argv.slice(2);
	var flags = {
		saveBzStatsSeparate: false,
		saveBzStatsPerMember: false
	};
	var positional = [];
	for (var i = 0; i < argv.length; i++) {
		var a = argv[i];
		if (a === '--save-bz-stats') {
			flags.saveBzStatsSeparate = true;
		} else if (a === '--save-bz-stats-per-member') {
			flags.saveBzStatsSeparate = true;
			flags.saveBzStatsPerMember = true;
		} else if (a === '--no-save-bz-stats') {
			flags.saveBzStatsSeparate = false;
			flags.saveBzStatsPerMember = false;
		} else if (a.indexOf('-') === 0) {
			// unknown flag: ignore for forward-compat
		} else {
			positional.push(a);
		}
	}
	var envSave = process.env.OSM_SAVE_BZ_STATS;
	if (envSave === '1' || envSave === 'true' || envSave === 'yes') {
		flags.saveBzStatsSeparate = true;
	}
	if (
		process.env.OSM_SAVE_BZ_STATS_PER_MEMBER === '1' ||
		process.env.OSM_SAVE_BZ_STATS_PER_MEMBER === 'true'
	) {
		flags.saveBzStatsSeparate = true;
		flags.saveBzStatsPerMember = true;
	}
	return { flags: flags, positional: positional };
}

function defaultCliPaths(argv) {
	var parsed = parseCliArgs(argv);
	var pos = parsed.positional;
	var fromEnv = process.env.OSM_INPUT;
	var input =
		pos[0] ||
		fromEnv ||
		path.resolve(__dirname, '../planet-latest.osm.bz2');
	var stats =
		pos[1] ||
		process.env.OSM_STATS ||
		input.replace(/\.bz2$/i, '') + '-stats.json';
	var results =
		pos[2] ||
		process.env.OSM_RESULTS ||
		input.replace(/\.bz2$/i, '') + '-results.json';
	var bzStats =
		process.env.OSM_BZ_STATS || defaultBzStatsPath(stats);
	var resultsBz =
		process.env.OSM_RESULTS_BZ_STATS || defaultBzStatsPath(results);
	return {
		inputPath: input,
		statsPath: stats,
		resultsPath: results,
		bzStatsPath: bzStats,
		resultsBzStatsPath: resultsBz,
		saveBzStatsSeparate: parsed.flags.saveBzStatsSeparate,
		saveBzStatsPerMember: parsed.flags.saveBzStatsPerMember
	};
}

/**
 * Write results.json (never embeds bzStats).
 * When saveSeparate, also write results-bzip.json; otherwise discard bz data.
 */
function saveResults(
	quiet,
	resultsPath,
	resultsBzPath,
	result,
	cleanedBz,
	saveSeparate,
	callback
) {
	var main = Object.assign({}, result);
	delete main.bzStats;
	if (saveSeparate) {
		main.bzStatsPath = resultsBzPath;
	} else {
		delete main.bzStatsPath;
	}

	fs.writeFile(resultsPath, JSON.stringify(main), function (err) {
		if (err) {
			if (!quiet) {
				console.error('Erro ao salvar o arquivo de resultados', err);
			}
			return callback && callback(err);
		}
		if (!quiet) {
			console.error('Arquivo de resultados salvo com sucesso: ' + resultsPath);
		}
		if (!saveSeparate) {
			return callback && callback(null);
		}
		fs.writeFile(resultsBzPath, JSON.stringify(cleanedBz || []), function (bzErr) {
			if (bzErr) {
				if (!quiet) {
					console.error('Erro ao salvar results-bzip', bzErr);
				}
				return callback && callback(bzErr);
			}
			if (!quiet) {
				console.error(
					'results-bzip salvo: ' +
						resultsBzPath +
						' (' +
						(cleanedBz ? cleanedBz.length : 0) +
						' membros)'
				);
			}
			callback && callback(null);
		});
	});
}

function main() {
	var paths = defaultCliPaths();
	var control = null;
	var sigintCount = 0;

	process.on('SIGINT', function () {
		sigintCount++;
		if (!control) {
			console.error('\nAinda inicializando; tente de novo em instantes.\n');
			return;
		}
		if (sigintCount === 1) {
			control.softStop('Ctrl+C');
			return;
		}
		// Second Ctrl+C: force hard stop; third: immediate exit
		if (sigintCount === 2) {
			control.hardStop();
			return;
		}
		console.error('\nSaindo imediatamente (3º Ctrl+C).\n');
		process.exit(130);
	});

	runProcess({
		inputPath: paths.inputPath,
		statsPath: paths.statsPath,
		resultsPath: paths.resultsPath,
		bzStatsPath: paths.bzStatsPath,
		resultsBzStatsPath: paths.resultsBzStatsPath,
		saveBzStatsSeparate: paths.saveBzStatsSeparate,
		saveBzStatsPerMember: paths.saveBzStatsPerMember,
		quiet: false,
		resume: true,
		saveStats: true,
		checkpointEveryFiles: 300,
		softStopMaxMs: DEFAULT_SOFT_STOP_MS,
		onControl: function (ctl) {
			control = ctl;
		}
	})
		.then(function (result) {
			if (result.stoppedEarly) process.exitCode = 0;
		})
		.catch(function (err) {
			console.error(err);
			process.exitCode = 1;
		});
}

module.exports = {
	runProcess: runProcess,
	defaultCliPaths: defaultCliPaths,
	parseCliArgs: parseCliArgs,
	defaultBzStatsPath: defaultBzStatsPath,
	formatCount: formatCount,
	printTime: printTime,
	printCRC: printCRC,
	readCRC: readCRC,
	ensureBzFile: ensureBzFile,
	polluteBzCurrentFile: polluteBzCurrentFile,
	createGeocodeSignals: createGeocodeSignals,
	noteGeocodeOpen: noteGeocodeOpen,
	geocodeHints: geocodeHints,
	snapshotGeocodeSignals: snapshotGeocodeSignals,
	formatGeocodeSignals: formatGeocodeSignals,
	recomputeGeocodeSignalsFromTree: recomputeGeocodeSignalsFromTree,
	GeoSig: GeoSig,
	CoordLayout: CoordLayout,
	DEFAULT_SOFT_STOP_MS: DEFAULT_SOFT_STOP_MS
};

if (require.main === module) {
	main();
}
