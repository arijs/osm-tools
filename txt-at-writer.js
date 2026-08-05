'use strict';

/**
 * DNE-style delimited writers: fields joined with '@', UTF-8, no header.
 *
 * Flat mode (default): outDir/OSM_LOGRADOURO_SP.TXT
 * Shard mode (--shard-lines=N):
 *   outDir/OSM_LOGRADOURO_SP/{N}-linhas/000001.txt
 *   outDir/OSM_LOGRADOURO_SP/MANIFEST.json
 */

var fs = require('fs');
var path = require('path');

var DELIM = '@';

function sanitizeField(value) {
	if (value == null) return '';
	var s = String(value);
	s = s.replace(/[\r\n]+/g, ' ').replace(/@/g, ' ');
	return s;
}

function formatRow(fields) {
	var parts = [];
	for (var i = 0; i < fields.length; i++) {
		parts.push(sanitizeField(fields[i]));
	}
	return parts.join(DELIM) + '\n';
}

function padShard(n) {
	var s = String(n);
	while (s.length < 6) s = '0' + s;
	return s;
}

/**
 * @param {string} outDir
 * @param {object} [options]
 * @param {boolean} [options.append=false]
 * @param {number} [options.shardLines=0] 0 = flat single file
 * @param {string[]|null} [options.shardOnly] if set, only these baseName prefixes shard
 *   (match: baseName === key || baseName.indexOf(key + '_') === 0 || baseName === key)
 * @param {string} [options.encoding='utf8']
 * @param {string} [options.sourcePbf]
 */
function createTxtAtWriter(outDir, options) {
	options = options || {};
	fs.mkdirSync(outDir, { recursive: true });
	var encoding = options.encoding || 'utf8';
	var flags = options.append ? 'a' : 'w';
	var shardLines = options.shardLines > 0 ? (options.shardLines | 0) : 0;
	var shardOnly = options.shardOnly || null;
	var sourcePbf = options.sourcePbf || '';

	/** @type {Object.<string, object>} per baseName state */
	var states = Object.create(null);
	var counts = Object.create(null);
	var rejected = 0;

	function shouldShard(baseName) {
		if (shardLines <= 0) return false;
		if (!shardOnly || !shardOnly.length) return true;
		for (var i = 0; i < shardOnly.length; i++) {
			var k = shardOnly[i];
			if (baseName === k) return true;
			if (baseName.indexOf(k + '_') === 0) return true;
			// allow "logradouro" matching OSM_LOGRADOURO_SP
			if (k.indexOf('OSM_') !== 0) {
				var upper = k.toUpperCase();
				if (baseName.indexOf('OSM_' + upper) === 0) return true;
			}
		}
		return false;
	}

	function ensureState(baseName) {
		if (states[baseName]) return states[baseName];
		var useShard = shouldShard(baseName);
		var st = {
			baseName: baseName,
			useShard: useShard,
			stream: null,
			shardIndex: 0,
			linesInShard: 0,
			totalLines: 0,
			shardMeta: [],
			shardDirName: useShard ? shardLines + '-linhas' : null,
			shardRoot: useShard ? path.join(outDir, baseName) : null,
			shardPath: null
		};
		if (useShard) {
			st.shardPath = path.join(st.shardRoot, st.shardDirName);
			fs.mkdirSync(st.shardPath, { recursive: true });
		}
		states[baseName] = st;
		counts[baseName] = 0;
		return st;
	}

	function openNextShard(st) {
		if (st.stream) {
			// close sync-ish: end without waiting (flush() waits all)
			st.stream.end();
			st.stream = null;
		}
		st.shardIndex++;
		st.linesInShard = 0;
		var fileName = padShard(st.shardIndex) + '.txt';
		var filePath = path.join(st.shardPath, fileName);
		st.stream = fs.createWriteStream(filePath, { flags: 'w', encoding: encoding });
		st.currentFile = fileName;
		st.shardMeta.push({
			file: fileName,
			lines: 0,
			line_from: st.totalLines + 1,
			line_to: st.totalLines
		});
	}

	function streamFor(baseName) {
		var st = ensureState(baseName);
		if (st.useShard) {
			if (!st.stream) openNextShard(st);
			return st.stream;
		}
		if (!st.stream) {
			var filePath = path.join(outDir, baseName + '.TXT');
			st.stream = fs.createWriteStream(filePath, {
				flags: flags,
				encoding: encoding
			});
		}
		return st.stream;
	}

	function write(baseName, fields) {
		var st = ensureState(baseName);
		if (st.useShard) {
			if (!st.stream || st.linesInShard >= shardLines) {
				if (st.stream && st.linesInShard >= shardLines) {
					// finalize meta for previous
					var prev = st.shardMeta[st.shardMeta.length - 1];
					if (prev) {
						prev.lines = st.linesInShard;
						prev.line_to = st.totalLines;
					}
				}
				openNextShard(st);
			}
		}
		var line = formatRow(fields);
		streamFor(baseName).write(line);
		st.totalLines++;
		st.linesInShard++;
		counts[baseName] = st.totalLines;
		if (st.useShard && st.shardMeta.length) {
			var cur = st.shardMeta[st.shardMeta.length - 1];
			cur.lines = st.linesInShard;
			cur.line_to = st.totalLines;
		}
	}

	function writeManifest(st, complete) {
		if (!st.useShard) return;
		var manifest = {
			dataset_key: st.baseName,
			shard_lines: shardLines,
			shard_dir: st.shardDirName,
			encoding: 'utf-8',
			delimiter: DELIM,
			complete: !!complete,
			total_lines: st.totalLines,
			shard_count: st.shardMeta.length,
			shards: st.shardMeta.slice(),
			created_at: new Date().toISOString(),
			source_pbf: sourcePbf
		};
		var manPath = path.join(st.shardRoot, 'MANIFEST.json');
		fs.writeFileSync(manPath, JSON.stringify(manifest, null, 2), 'utf8');
	}

	function flush(cb) {
		var names = Object.keys(states);
		if (!names.length) {
			if (cb) cb(null);
			return Promise.resolve();
		}
		return new Promise(function (resolve, reject) {
			var left = names.length;
			var errOut = null;
			names.forEach(function (name) {
				var st = states[name];
				function afterClose(err) {
					if (err && !errOut) errOut = err;
					if (st.useShard) {
						try {
							writeManifest(st, true);
						} catch (e) {
							if (!errOut) errOut = e;
						}
					}
					left--;
					if (left === 0) {
						if (errOut) {
							if (cb) cb(errOut);
							reject(errOut);
						} else {
							if (cb) cb(null);
							resolve();
						}
					}
				}
				if (st.stream) {
					st.stream.end(afterClose);
					st.stream = null;
				} else {
					afterClose(null);
				}
			});
		});
	}

	function closeSync() {
		var names = Object.keys(states);
		for (var i = 0; i < names.length; i++) {
			var st = states[names[i]];
			try {
				if (st.stream) st.stream.end();
			} catch (_) {}
			if (st.useShard) {
				try {
					writeManifest(st, true);
				} catch (_) {}
			}
		}
	}

	/** Snapshot of shard info for checkpoint */
	function getShardSnapshot() {
		var out = {};
		var names = Object.keys(states);
		for (var i = 0; i < names.length; i++) {
			var st = states[names[i]];
			if (!st.useShard) continue;
			out[st.baseName] = {
				shard_lines: shardLines,
				total_lines: st.totalLines,
				shard_count: st.shardMeta.length,
				shards: st.shardMeta.slice()
			};
		}
		return out;
	}

	return {
		write: write,
		flush: flush,
		closeSync: closeSync,
		counts: counts,
		get rejected() {
			return rejected;
		},
		outDir: outDir,
		shardLines: shardLines,
		getShardSnapshot: getShardSnapshot
	};
}

/**
 * Remove previous extract artifacts (flat TXT and shard dirs).
 */
function wipeOsmOutputs(outDir) {
	if (!fs.existsSync(outDir)) return;
	var existing = fs.readdirSync(outDir);
	for (var i = 0; i < existing.length; i++) {
		var name = existing[i];
		var full = path.join(outDir, name);
		if (/^OSM_.*\.TXT$/i.test(name)) {
			try {
				fs.unlinkSync(full);
			} catch (_) {}
		} else if (/^OSM_/i.test(name)) {
			try {
				fs.rmSync(full, { recursive: true, force: true });
			} catch (_) {
				// Node < 14.14: fallback
				try {
					rmDirRecursive(full);
				} catch (__) {}
			}
		}
	}
}

function rmDirRecursive(dir) {
	if (!fs.existsSync(dir)) return;
	var entries = fs.readdirSync(dir);
	for (var i = 0; i < entries.length; i++) {
		var p = path.join(dir, entries[i]);
		if (fs.statSync(p).isDirectory()) rmDirRecursive(p);
		else fs.unlinkSync(p);
	}
	fs.rmdirSync(dir);
}

/**
 * Resolve dataset base name to ordered list of readable files.
 *
 * Preferência (sem concatenar):
 * 1. Pasta de shards: `outDir/BASE/` com MANIFEST.json ou `*-linhas/*.txt`
 * 2. Arquivo flat: `outDir/BASE.TXT` (case-insensitive em Windows)
 *
 * @param {string} outDir
 * @param {string} baseName e.g. OSM_LOGRADOURO_SP, OSM_BAIRRO, OSM_ADDR_POINT_SP
 * @returns {{ mode: 'shard'|'flat'|'missing', paths: string[], totalLines: number|null, manifest: object|null, root: string|null }}
 */
function resolveDatasetPaths(outDir, baseName) {
	var shardRoot = path.join(outDir, baseName);
	if (fs.existsSync(shardRoot) && fs.statSync(shardRoot).isDirectory()) {
		var manPath = path.join(shardRoot, 'MANIFEST.json');
		var paths = [];
		var totalLines = null;
		var manifest = null;
		if (fs.existsSync(manPath)) {
			try {
				manifest = JSON.parse(fs.readFileSync(manPath, 'utf8'));
			} catch (_) {
				manifest = null;
			}
		}
		if (manifest && Array.isArray(manifest.shards) && manifest.shards.length) {
			var shardDir = path.join(
				shardRoot,
				manifest.shard_dir || String(manifest.shard_lines || 0) + '-linhas'
			);
			for (var i = 0; i < manifest.shards.length; i++) {
				var rel = manifest.shards[i].file;
				if (!rel) continue;
				var fp = path.join(shardDir, rel);
				if (fs.existsSync(fp)) paths.push(fp);
			}
			if (typeof manifest.total_lines === 'number') totalLines = manifest.total_lines;
		}
		if (!paths.length) {
			// fallback: qualquer *.txt sob a pasta, ordem lexicográfica
			paths = listTxtRecursiveSorted(shardRoot);
		}
		if (paths.length) {
			return {
				mode: 'shard',
				paths: paths,
				totalLines: totalLines,
				manifest: manifest,
				root: shardRoot
			};
		}
	}

	// flat: BASE.TXT (exact) then case variants
	var candidates = [
		path.join(outDir, baseName + '.TXT'),
		path.join(outDir, baseName + '.txt')
	];
	for (var c = 0; c < candidates.length; c++) {
		if (fs.existsSync(candidates[c]) && fs.statSync(candidates[c]).isFile()) {
			return {
				mode: 'flat',
				paths: [candidates[c]],
				totalLines: null,
				manifest: null,
				root: candidates[c]
			};
		}
	}

	// Windows: scan dir for case-insensitive match
	if (fs.existsSync(outDir)) {
		try {
			var names = fs.readdirSync(outDir);
			var want = (baseName + '.TXT').toLowerCase();
			for (var n = 0; n < names.length; n++) {
				if (names[n].toLowerCase() === want) {
					var full = path.join(outDir, names[n]);
					if (fs.statSync(full).isFile()) {
						return {
							mode: 'flat',
							paths: [full],
							totalLines: null,
							manifest: null,
							root: full
						};
					}
				}
			}
		} catch (_) {}
	}

	return {
		mode: 'missing',
		paths: [],
		totalLines: null,
		manifest: null,
		root: null
	};
}

function listTxtRecursiveSorted(dir) {
	var out = [];
	function walk(d) {
		var entries;
		try {
			entries = fs.readdirSync(d);
		} catch (_) {
			return;
		}
		entries.sort();
		for (var i = 0; i < entries.length; i++) {
			if (entries[i] === 'MANIFEST.json') continue;
			var p = path.join(d, entries[i]);
			var st;
			try {
				st = fs.statSync(p);
			} catch (_) {
				continue;
			}
			if (st.isDirectory()) walk(p);
			else if (/\.txt$/i.test(entries[i])) out.push(p);
		}
	}
	walk(dir);
	out.sort();
	return out;
}

/**
 * Dataset exists as flat file and/or shard directory.
 */
function datasetExists(outDir, baseName) {
	return resolveDatasetPaths(outDir, baseName).mode !== 'missing';
}

module.exports = {
	DELIM: DELIM,
	sanitizeField: sanitizeField,
	formatRow: formatRow,
	padShard: padShard,
	createTxtAtWriter: createTxtAtWriter,
	wipeOsmOutputs: wipeOsmOutputs,
	resolveDatasetPaths: resolveDatasetPaths,
	datasetExists: datasetExists,
	listTxtRecursiveSorted: listTxtRecursiveSorted
};
