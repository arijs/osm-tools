'use strict';

/**
 * Streaming OSM PBF blob reader (fileformat layer).
 * Yields { type, data, fileOffset, blobIndex, headerSize, blobSize }.
 */

var fs = require('fs');
var zlib = require('zlib');
var fileformat = require('./fileformat.proto.js');

function readUInt32BE(buf, offset) {
	return buf.readUInt32BE(offset);
}

/**
 * Read next blob from fd starting at fileOffset.
 * @returns {object|null} null at EOF
 */
function readNextBlob(fd, fileOffset, fileSize) {
	if (fileOffset + 4 > fileSize) return null;

	var sizeBuf = Buffer.alloc(4);
	var n = fs.readSync(fd, sizeBuf, 0, 4, fileOffset);
	if (n < 4) return null;

	var headerLen = readUInt32BE(sizeBuf, 0);
	if (headerLen <= 0 || headerLen > 64 * 1024) {
		throw new Error(
			'Invalid BlobHeader size ' + headerLen + ' at offset ' + fileOffset
		);
	}
	var headerStart = fileOffset + 4;
	if (headerStart + headerLen > fileSize) {
		throw new Error('Truncated BlobHeader at offset ' + fileOffset);
	}

	var headerBuf = Buffer.alloc(headerLen);
	fs.readSync(fd, headerBuf, 0, headerLen, headerStart);
	var header = fileformat.readBlobHeader(headerBuf);
	var dataSize = header.datasize | 0;
	if (dataSize < 0 || dataSize > 32 * 1024 * 1024) {
		throw new Error(
			'Invalid Blob datasize ' + dataSize + ' type=' + header.type
		);
	}

	var blobStart = headerStart + headerLen;
	if (blobStart + dataSize > fileSize) {
		throw new Error('Truncated Blob at offset ' + blobStart);
	}
	var blobBuf = Buffer.alloc(dataSize);
	fs.readSync(fd, blobBuf, 0, dataSize, blobStart);
	var blob = fileformat.readBlob(blobBuf);

	var data;
	if (blob.raw && blob.raw.length) {
		data = Buffer.from(blob.raw);
	} else if (blob.zlib_data && blob.zlib_data.length) {
		data = zlib.inflateSync(Buffer.from(blob.zlib_data));
	} else if (blob.lz4_data || blob.zstd_data || blob.lzma_data) {
		throw new Error(
			'Unsupported blob compression for type ' +
				header.type +
				' (only raw/zlib supported)'
		);
	} else {
		throw new Error('Empty blob data for type ' + header.type);
	}

	var nextOffset = blobStart + dataSize;
	return {
		type: header.type,
		data: data,
		fileOffset: fileOffset,
		nextOffset: nextOffset,
		headerSize: headerLen,
		blobSize: dataSize,
		rawSize: blob.raw_size || data.length
	};
}

/**
 * Open path and iterate all blobs (síncrono — bloqueia o event loop).
 * @param {string} filePath
 * @param {object} [options]
 * @param {number} [options.startOffset=0]
 * @param {function} onBlob - (blobInfo) => void | 'stop'
 * @returns {{ blobsRead: number, nextOffset: number, stopped: boolean }}
 */
function forEachBlob(filePath, options, onBlob) {
	if (typeof options === 'function') {
		onBlob = options;
		options = {};
	}
	options = options || {};
	var startOffset = options.startOffset || 0;
	var st = fs.statSync(filePath);
	var fd = fs.openSync(filePath, 'r');
	var offset = startOffset;
	var blobIndex = options.startBlobIndex || 0;
	var stopped = false;
	try {
		for (;;) {
			var info = readNextBlob(fd, offset, st.size);
			if (!info) break;
			info.blobIndex = blobIndex;
			info.fileSize = st.size;
			var ret = onBlob(info);
			blobIndex++;
			offset = info.nextOffset;
			if (ret === 'stop') {
				stopped = true;
				break;
			}
		}
	} finally {
		fs.closeSync(fd);
	}
	return {
		blobsRead: blobIndex - (options.startBlobIndex || 0),
		nextOffset: offset,
		blobIndex: blobIndex,
		stopped: stopped,
		fileSize: st.size
	};
}

/**
 * Like forEachBlob, but yields to the event loop every `yieldEvery` blobs
 * (default 1) so SIGINT/Ctrl+C handlers can run mid-scan.
 *
 * Crucial for long extracts/waves on multi-GB PBFs: the sync loop never
 * processes signals until it finishes.
 *
 * @param {string} filePath
 * @param {object} [options]
 * @param {number} [options.startOffset=0]
 * @param {number} [options.startBlobIndex=0]
 * @param {number} [options.yieldEvery=1]
 * @param {function} onBlob - (blobInfo) => void | 'stop' | Promise
 * @returns {Promise<{ blobsRead, nextOffset, blobIndex, stopped, fileSize }>}
 */
function forEachBlobAsync(filePath, options, onBlob) {
	if (typeof options === 'function') {
		onBlob = options;
		options = {};
	}
	options = options || {};
	var startOffset = options.startOffset || 0;
	var startBlobIndex = options.startBlobIndex || 0;
	var yieldEvery = options.yieldEvery == null ? 1 : options.yieldEvery | 0;
	if (yieldEvery < 0) yieldEvery = 0;

	return new Promise(function (resolve, reject) {
		var st;
		var fd;
		try {
			st = fs.statSync(filePath);
			fd = fs.openSync(filePath, 'r');
		} catch (e) {
			return reject(e);
		}
		var offset = startOffset;
		var blobIndex = startBlobIndex;
		var stopped = false;
		var sinceYield = 0;
		var closed = false;

		function closeFd() {
			if (closed) return;
			closed = true;
			try {
				fs.closeSync(fd);
			} catch (e) {
				/* ignore */
			}
		}

		function done() {
			closeFd();
			resolve({
				blobsRead: blobIndex - startBlobIndex,
				nextOffset: offset,
				blobIndex: blobIndex,
				stopped: stopped,
				fileSize: st.size
			});
		}

		function fail(err) {
			closeFd();
			reject(err);
		}

		function step() {
			try {
				for (;;) {
					var info = readNextBlob(fd, offset, st.size);
					if (!info) {
						done();
						return;
					}
					info.blobIndex = blobIndex;
					info.fileSize = st.size;
					var ret = onBlob(info);
					// Support async onBlob without forcing all callers to async
					if (ret && typeof ret.then === 'function') {
						ret.then(function (asyncRet) {
							blobIndex++;
							offset = info.nextOffset;
							if (asyncRet === 'stop') {
								stopped = true;
								done();
								return;
							}
							setImmediate(step);
						}, fail);
						return;
					}
					blobIndex++;
					offset = info.nextOffset;
					if (ret === 'stop') {
						stopped = true;
						done();
						return;
					}
					sinceYield++;
					if (yieldEvery > 0 && sinceYield >= yieldEvery) {
						sinceYield = 0;
						setImmediate(step);
						return;
					}
				}
			} catch (err) {
				fail(err);
			}
		}

		step();
	});
}

module.exports = {
	readNextBlob: readNextBlob,
	forEachBlob: forEachBlob,
	forEachBlobAsync: forEachBlobAsync
};
