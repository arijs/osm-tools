'use strict';

/**
 * DNE-style delimited writers: fields joined with '@', UTF-8, no header.
 */

var fs = require('fs');
var path = require('path');

var DELIM = '@';

function sanitizeField(value) {
	if (value == null) return '';
	var s = String(value);
	// DNE-like files do not quote; strip CR/LF and '@' to keep one field per cell
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

/**
 * Multi-file writer keyed by logical name (e.g. OSM_LOGRADOURO_SP).
 */
function createTxtAtWriter(outDir, options) {
	options = options || {};
	fs.mkdirSync(outDir, { recursive: true });
	var streams = Object.create(null);
	var counts = Object.create(null);
	var rejected = 0;
	var encoding = options.encoding || 'utf8';
	var flags = options.append ? 'a' : 'w';

	function streamFor(baseName) {
		if (!streams[baseName]) {
			var filePath = path.join(outDir, baseName + '.TXT');
			streams[baseName] = fs.createWriteStream(filePath, {
				flags: flags,
				encoding: encoding
			});
			counts[baseName] = counts[baseName] || 0;
		}
		return streams[baseName];
	}

	function write(baseName, fields) {
		// reject if any field still contains delim after sanitize? sanitize strips @
		var line = formatRow(fields);
		streamFor(baseName).write(line);
		counts[baseName] = (counts[baseName] || 0) + 1;
	}

	function flush(cb) {
		var names = Object.keys(streams);
		if (!names.length) {
			if (cb) cb(null);
			return Promise.resolve();
		}
		return new Promise(function (resolve, reject) {
			var left = names.length;
			var errOut = null;
			names.forEach(function (name) {
				streams[name].end(function (err) {
					if (err && !errOut) errOut = err;
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
				});
			});
		});
	}

	function closeSync() {
		// best-effort for tests
		var names = Object.keys(streams);
		for (var i = 0; i < names.length; i++) {
			try {
				streams[names[i]].end();
			} catch (_) {}
		}
	}

	return {
		write: write,
		flush: flush,
		closeSync: closeSync,
		counts: counts,
		get rejected() {
			return rejected;
		},
		outDir: outDir
	};
}

module.exports = {
	DELIM: DELIM,
	sanitizeField: sanitizeField,
	formatRow: formatRow,
	createTxtAtWriter: createTxtAtWriter
};
