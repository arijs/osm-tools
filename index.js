'use strict';

/**
 * ⚠️  DRAFT / INCOMPLETE — do not use for real work.
 *
 * This file is a leftover experiment. It is fundamentally incomplete:
 *
 *  - Had a syntax error (`chunkPos[0] += @TODO ?`) — placeholder never finished
 *  - Calls `SeekBzip.readBlock(fd, fopt, onData, 8192)` with the wrong API
 *    (real signature is readBlock(sread, swrite, opt); see index0.js)
 *  - References `unbzip2()` without requiring `unbzip2-stream`
 *  - `bzInitFile()` returns void but was assigned as if it returned a file object
 *  - Dual half-wired pipelines (seek-bzip path never started; unbzip2 path caps at
 *    maxZipChunks and does not finish stats the way index0 does)
 *
 * The working processor is **index0.js**:
 *
 *   node index0.js path/to/file.osm.bz2 [stats.json]
 *   const { runProcess } = require('./index0');
 *
 * This module only throws when executed as a script so the breakage is obvious.
 */

if (require.main === module) {
	console.error(
		'index.js is an incomplete draft and cannot run.\n' +
			'Use:  node index0.js <file.osm.bz2> [stats.json]\n' +
			'Or:   const { runProcess } = require("./index0");'
	);
	process.exitCode = 1;
}

module.exports = {
	incomplete: true,
	message:
		'index.js is a broken draft. Use index0.js / runProcess instead.'
};
