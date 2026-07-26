'use strict';

var fs = require('fs');
var path = require('path');
var stream = require('stream');
var SeekBzip = require('@arijs/seek-bzip');
var { XMLParser } = require('@arijs/stream-xml-parser');
var StreamingDecoder = require('../streaming-decoder');

var fixturesDir = path.join(__dirname, 'fixtures');

function fixturePath(name) {
	return path.join(fixturesDir, name);
}

/**
 * Feed a string through XMLParser and collect simplified open/close/text/cdata events.
 * Mirrors how index.js / index0.js interpret native parser events.
 */
function parseXmlString(xml) {
	return new Promise(function (resolve, reject) {
		var events = [];
		var pendingName = null;
		var pendingAttrs = null;

		function pushOpen(name, attrs) {
			events.push({ event: 'open', name: name, attrs: attrs || {} });
		}
		function pushClose(name) {
			events.push({ event: 'close', name: name });
		}

		var parser = new XMLParser({
			event: function (ev) {
				var tag = ev.tag;
				var attr = ev.attr;
				try {
					switch (ev.name) {
						case 'startTag':
							pendingName = null;
							pendingAttrs = {};
							break;
						case 'tagName':
							if (tag) pendingName = tag.name;
							break;
						case 'tagAttribute':
							if (attr && attr.name != null) {
								pendingAttrs = pendingAttrs || {};
								pendingAttrs[attr.name] =
									attr.value == null ? '' : String(attr.value);
							}
							break;
						case 'endTag':
							if (tag) {
								if (pendingName == null && tag.name != null) {
									pendingName = tag.name;
								}
								if (tag.close) {
									pushClose(pendingName);
								} else {
									pushOpen(pendingName, pendingAttrs || {});
									if (tag.selfClose) pushClose(pendingName);
								}
							}
							pendingName = null;
							pendingAttrs = null;
							break;
						case 'text':
							if (ev.text) {
								events.push({ event: 'text', text: ev.text });
							}
							break;
						case 'endCdata':
							events.push({
								event: 'cdata',
								cdata: (tag && tag.textCdata) || ''
							});
							break;
						case 'endInstruction':
							events.push({
								event: 'instruction',
								text: (tag && tag.text) || ''
							});
							break;
						case 'endStream':
							resolve(events);
							break;
					}
				} catch (e) {
					reject(e);
				}
			}
		});

		try {
			parser.end(xml);
		} catch (e) {
			reject(e);
		}
	});
}

/**
 * Stream text chunks into XMLParser (as index sinks do).
 */
function parseXmlChunks(parts) {
	return new Promise(function (resolve, reject) {
		var events = [];
		var pendingName = null;
		var pendingAttrs = null;
		var ended = false;

		var parser = new XMLParser({
			event: function (ev) {
				var tag = ev.tag;
				var attr = ev.attr;
				try {
					switch (ev.name) {
						case 'startTag':
							pendingName = null;
							pendingAttrs = {};
							break;
						case 'tagName':
							if (tag) pendingName = tag.name;
							break;
						case 'tagAttribute':
							if (attr && attr.name != null) {
								pendingAttrs = pendingAttrs || {};
								pendingAttrs[attr.name] =
									attr.value == null ? '' : String(attr.value);
							}
							break;
						case 'endTag':
							if (tag) {
								if (pendingName == null && tag.name != null) {
									pendingName = tag.name;
								}
								if (tag.close) {
									events.push({ event: 'close', name: pendingName });
								} else {
									events.push({
										event: 'open',
										name: pendingName,
										attrs: pendingAttrs || {}
									});
									if (tag.selfClose) {
										events.push({ event: 'close', name: pendingName });
									}
								}
							}
							pendingName = null;
							pendingAttrs = null;
							break;
						case 'endStream':
							if (!ended) {
								ended = true;
								resolve(events);
							}
							break;
					}
				} catch (e) {
					reject(e);
				}
			}
		});

		var sink = new stream.Writable({
			write: function (chunk, encoding, callback) {
				try {
					parser.write(
						typeof chunk === 'string' ? chunk : chunk.toString('utf8')
					);
					callback();
				} catch (e) {
					callback(e);
				}
			},
			final: function (callback) {
				try {
					parser.end('');
					callback();
				} catch (e) {
					callback(e);
				}
			}
		});
		sink.on('error', reject);

		var i = 0;
		function writeNext() {
			if (i >= parts.length) {
				sink.end();
				return;
			}
			var ok = sink.write(parts[i++]);
			if (ok === false) sink.once('drain', writeNext);
			else setImmediate(writeNext);
		}
		writeNext();
	});
}

/**
 * Decode an entire .bz2 file with @arijs/seek-bzip.decode.
 */
function decodeBz2File(filePath) {
	var compressed = fs.readFileSync(filePath);
	var decoded = SeekBzip.decode(compressed);
	return Buffer.isBuffer(decoded) ? decoded : Buffer.from(decoded);
}

/**
 * Walk every bzip2 block with SeekBzip.readBlock, concatenating output.
 */
function decodeBz2ByBlocks(filePath) {
	var fd = fs.openSync(filePath, 'r');
	var chunks = [];
	var blocks = [];
	var files = 0;
	var fopt = null;
	var bzRead = SeekBzip.fdReadFile(fd, 4096, 0);
	var bzWrite = SeekBzip.readBlock.makeOutStream(function (buf) {
		if (buf && buf.length) chunks.push(Buffer.from(buf));
	}, 64 * 1024);

	try {
		for (;;) {
			fopt = SeekBzip.readBlock(bzRead, bzWrite, fopt);
			if (fopt.streamCRC) {
				files += 1;
				bzWrite.flush && bzWrite.flush();
				var size = fs.fstatSync(fd).size;
				if (fopt.fileOffset >= size) break;
			} else if (fopt.blockCRC) {
				blocks.push({
					input: fopt.bytesInput,
					output: fopt.bytesOutput,
					offset: fopt.byteOffset,
					crc: fopt.blockCRC
				});
				bzWrite.flush && bzWrite.flush();
			} else {
				break;
			}
		}
		bzWrite.flush && bzWrite.flush();
	} finally {
		fs.closeSync(fd);
	}

	return {
		text: Buffer.concat(chunks).toString('utf8'),
		blocks: blocks,
		files: files || 1
	};
}

/**
 * Full pipeline: .bz2 → seek-bzip → StreamingDecoder → XMLParser
 */
function parseOsmBz2(filePath) {
	return new Promise(function (resolve, reject) {
		var openTags = {};
		var firstOpen = null;
		var pendingName = null;
		var pendingAttrs = null;
		var fd = fs.openSync(filePath, 'r');
		var decoder = new StreamingDecoder({ highWaterMark: 256 });
		var done = false;
		var fopt = null;
		var bzRead = SeekBzip.fdReadFile(fd, 4096, 0);

		function finish(err) {
			if (done) return;
			done = true;
			try {
				fs.closeSync(fd);
			} catch (_) {}
			if (err) reject(err);
			else {
				resolve({
					openTags: openTags,
					nodeCount: openTags.node || 0,
					wayCount: openTags.way || 0,
					relationCount: openTags.relation || 0,
					tagCount: openTags.tag || 0,
					firstOpen: firstOpen
				});
			}
		}

		var parser = new XMLParser({
			event: function (ev) {
				var tag = ev.tag;
				var attr = ev.attr;
				try {
					switch (ev.name) {
						case 'startTag':
							pendingName = null;
							pendingAttrs = {};
							break;
						case 'tagName':
							if (tag) pendingName = tag.name;
							break;
						case 'tagAttribute':
							if (attr && attr.name != null) {
								pendingAttrs = pendingAttrs || {};
								pendingAttrs[attr.name] =
									attr.value == null ? '' : String(attr.value);
							}
							break;
						case 'endTag':
							if (tag) {
								if (pendingName == null && tag.name != null) {
									pendingName = tag.name;
								}
								if (!tag.close) {
									openTags[pendingName] = (openTags[pendingName] || 0) + 1;
									if (!firstOpen) {
										firstOpen = {
											name: pendingName,
											attrs: pendingAttrs || {}
										};
									}
								}
							}
							pendingName = null;
							pendingAttrs = null;
							break;
						case 'endStream':
							finish(null);
							break;
					}
				} catch (e) {
					finish(e);
				}
			}
		});

		decoder.on('data', function (chunk) {
			parser.write(typeof chunk === 'string' ? chunk : chunk.toString('utf8'));
		});
		decoder.on('end', function () {
			try {
				parser.end('');
			} catch (e) {
				finish(e);
			}
		});
		decoder.on('error', finish);

		var bzWrite = SeekBzip.readBlock.makeOutStream(function (buf) {
			if (buf && buf.length) decoder.write(buf);
		}, 64 * 1024);

		try {
			for (;;) {
				fopt = SeekBzip.readBlock(bzRead, bzWrite, fopt);
				if (fopt.streamCRC) {
					bzWrite.flush && bzWrite.flush();
					var size = fs.fstatSync(fd).size;
					if (fopt.fileOffset >= size) break;
				} else if (fopt.blockCRC) {
					bzWrite.flush && bzWrite.flush();
				} else {
					break;
				}
			}
			bzWrite.flush && bzWrite.flush();
			decoder.end();
		} catch (e) {
			finish(e);
		}
	});
}

function countOpen(events, name) {
	var n = 0;
	for (var i = 0; i < events.length; i++) {
		if (events[i].event === 'open' && events[i].name === name) n++;
	}
	return n;
}

function countClose(events, name) {
	var n = 0;
	for (var i = 0; i < events.length; i++) {
		if (events[i].event === 'close' && events[i].name === name) n++;
	}
	return n;
}

module.exports = {
	fixturesDir: fixturesDir,
	fixturePath: fixturePath,
	parseXmlString: parseXmlString,
	parseXmlChunks: parseXmlChunks,
	decodeBz2File: decodeBz2File,
	decodeBz2ByBlocks: decodeBz2ByBlocks,
	parseOsmBz2: parseOsmBz2,
	countOpen: countOpen,
	countClose: countClose,
	XMLParser: XMLParser,
	SeekBzip: SeekBzip
};
