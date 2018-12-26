var fs = require('fs');
var path = require('path');
var util = require('util');
var stream = require('stream');
var SeekBzip = require('@arijs/seek-bzip');
// var SeekBzip = require('./seek-bzip');
var dataSize = require('./datasize');
var StreamingDecoder = require('./streaming-decoder');
var LineSplitter = require('./line-splitter');
var ParseXML = require('./parse-xml');

var hop = Object.prototype.hasOwnProperty;

function formatBytes(x) {
	return Number(x).toFixed(0);
}

function printCRC(crc) {
	return 'number' === typeof crc
		? ('00000000'+crc.toString(16)).substr(-8, 8)
		: crc;
}
function readCRC(crc) {
	return 'string' === typeof crc && crc.length <= 8
		? parseInt(crc, 16)
		: crc;
}

function printTime(s) {
	var m = s/60;
	var h = m/60;
	var d = h/24;
	s = Math.floor(s % 60).toFixed(0);
	m = Math.floor(m % 60).toFixed(0);
	h = Math.floor(h % 24).toFixed(0);
	return (h < 1) ? ('  '+m).substr(-2)+';'+('00'+s).substr(-2) :
		(d <= 1) ? ('  '+h).substr(-2)+':'+('00'+m).substr(-2) :
		(d >= 100) ? (' '+d.toFixed(0)).substr(-4)+'d' :
		d.toFixed(d >= 10 ? 1 : 2)+'d';
}

function subtractArrays(a1, a2) {
	var b = [], c = a1.length;
	for (var i = 0; i < c; i++) {
		b[i] = (a1[i] || 0) - (a2[i] || 0);
	}
	return b;
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
		transform: function(chunk, encoding, callback) {
			setChunkPos(chunk, i);
			callback(null, chunk);
		}
	});
}

function createStreamFork(s1, s2) {
	return new stream.Writable({
		highWaterMark: 256,
		write: function(chunk, encoding, callback) {
			s1.write(chunk, encoding);
			s2.write(chunk, encoding);
			callback();
		},
		final: function() {
			s1.end();
			s2.end();
		}
	});
}

function printXMLStructure(cxsNext, tagName) {
	var ct = cxsNext.tags;
	var tags = [];
	for (var k in ct) {
		if (hop.call(ct, k)) {
			tags.push(printXMLStructure(ct[k], k));
		}
	}
	tags = tags.join(',');
	if (tagName) tags = tagName+' '+cxsNext.count+(tags?'('+tags+')':'');
	return tags;
}

function printXMLStats() {
	var time = (Date.now() - tstart) * 0.001;
	var posInput = chunkPos[0];
	var percent = Number(posInput / fstat.size);
	var runInput = posInput - previousChunkPos[0];
	var speed = runInput / time;
	var remain = (fstat.size - posInput) / speed;
	process.stdout.write(
		// '/encoding: '+encoding+
		'\r'+percent.toFixed(5)+' '+dataSize(speed)+'/s'+
		' '+printTime(time)+' '+printTime(remain)+
		' / '+chunkCount.join(', ')+
		' / '+chunkPos.map(dataSize).join(', ')+
		' bz'+bzIndexFile+'.'+bzIndexBlock+
		// ' / ('+chunkXMLTags.length+') '+chunkXMLTags.join('>')+
		' /'+printXMLStructure(chunkXMLStatsRoot)
		// ' /factor '+Number(chunkPos[0]/chunkPos[1]).toFixed(3)
	);
}

// var fname = '../planet-181217.osm';
var fname = '../planet-latest.osm';
var fpath = path.resolve(__dirname, fname+'.bz2');
var fwpath = path.resolve(__dirname, fname);
var fspath = path.resolve(__dirname, fname+'-stats.json');
var fstat = fs.statSync(fpath);
var tstart = Date.now();
var totaltime = 0;
var previousRuns = [];
var fd = fs.openSync(fpath, 'r');
// var fwrite = fs.createWriteStream(fwpath);
var strDecoder = new StreamingDecoder({ highWaterMark: 256 });
// strDec.on('readable', function() {
// 	var data;
// 	while (data = this.read()) {
// 		// console.log(data.toString('utf8'));
// 	}
// });
var proximoDesce = false;
var fopt;
var bzRead = SeekBzip.fdReadFile(fd, 4096, 0, bzReadBuffer);
var bzWrite = SeekBzip.readBlock.makeOutStream(outPrint, 1024 * 1024);
function bzReadBuffer(bzRead) {
	setChunkPos({length:bzRead.filePos - bzRead.lastStats.filePos}, 0);
}
function outPrint(buf) {
	setChunkPos(buf, 1);
	strDecoder.write(buf);
	// fwrite.write(buf);
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
function bzFinishBlock(fopt) {
	bzIndexBlock++;
	bzFile.blocks.push({
		crc: fopt.blockCRC,
		crcStream: fopt.streamPartialCRC,
		offsetStart: fopt.byteOffsetStart,
		offsetEnd: fopt.byteOffset,
		offsetBit: fopt.bitOffset,
		input: fopt.bytesInput,
		output: fopt.bytesOutput
	});
	bzFile.input += fopt.bytesInput;
	bzFile.output += fopt.bytesOutput;
}
function bzFinishFile(fopt) {
	bzIndexBlock = 0;
	bzIndexFile++;
	bzNextFileOffset = fopt.fileOffset;
	bzFile.crc = fopt.streamCRC;
	bzFile.offsetEnd = bzNextFileOffset;
	bzFile.offsetBitEnd = fopt.bitOffsetEnd;
	bzFile.level = fopt.bzLevelFile;
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
function getXMLTagPosStats() {
	return {
		count: chunkCount.slice(),
		pos: chunkPos.slice(),
		bzFile: bzIndexFile,
		bzBlock: bzIndexBlock
	};
}

function xmlMergeAttrsStats(target, source) {
	for (var k in source) {
		if (hop.call(source, k)) {
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

function createStreamXMLPos() {
	return new stream.Transform({
		highWaterMark: 0,
		objectMode: true,
		transform: function(chunk, encoding, callback) {
			var error = null;
			var cname = chunk.name;
			var cxsNext;
			chunkXMLLastChunks = chunkXMLLastChunks.slice(1,10).concat([chunk]);
			if (chunkXMLFirstChunks.length < 10) {
				chunkXMLFirstChunks.push(chunk);
			}
			switch (chunk.type) {
				case ParseXML.OPEN_TAG:
					chunkXMLTags.push(cname);//+'['+chunkXMLCurrent+']');
					chunkXMLIndex.push(chunkXMLCurrent);
					cxsNext = chunkXMLStatsCurrent.tags[cname];
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
						chunkXMLStatsCurrent.tags[cname] = cxsNext;
					}
					cxsNext.parent = chunkXMLStatsCurrent;
					cxsNext.lastOpen = getXMLTagPosStats();
					cxsNext.lastIndex = chunkXMLCurrent;
					cxsNext.count += 1;
					xmlMergeAttrsStats(cxsNext.attrs, chunk.attrs);
					chunkXMLStatsCurrent = cxsNext;
					chunkXMLCurrent = 0;
					break;
				case ParseXML.CLOSE_TAG:
					var tag = chunkXMLTags.pop();
					var index = chunkXMLIndex.pop();
					if (tag !== cname) {
						error = new Error('Error tracking XML tags: expected '+tag+' but got '+cname);
					}
					chunkXMLCurrent = index + 1;
					if (!chunkXMLStatsCurrent.firstClose) {
						chunkXMLStatsCurrent.firstClose = getXMLTagPosStats();
					}
					chunkXMLStatsCurrent.lastClose = getXMLTagPosStats();
					cxsNext = chunkXMLStatsCurrent.parent;
					if (cxsNext) {
						// cxsNext.parent = void 0;
						chunkXMLStatsCurrent = cxsNext;
					} else {
						console.error(chunkXMLStatsRoot.tags);
						console.error(chunkXMLLastChunks);
						if (chunkXMLStatsCurrent === chunkXMLStatsRoot) {
							console.error('cxsCurrent is root');
						}
						error = new Error('Error tracking XML tags: parent got lost');
					}
					break;
			}
			callback(error, chunk);
		},
		flush: function(callback) {
			this.push(null);
			callback();
		}
	});
}

function createStreamXMLCloseTag() {
	return new stream.Writable({
		highWaterMark: 256,
		objectMode: true,
		write: function(chunk, encoding, callback) {
			var error;
			switch (chunk.type) {
				case ParseXML.ERROR:
					// console.error('ERROR', String(chunk.error)+'\n');
					this.emit('error', chunk.error);
					error = chunk.error;
					// no break on purpose
				case ParseXML.CLOSE_TAG:
					printXMLStats();
					// console.log(chunk);
					// process.stdout.write(chunk.toString());
					break;
				case ParseXML.UNPARSED_REMAIN:
					if (chunkXMLRemain) {
						console.error(chunkXMLRemain);
						console.error(chunk);
						error = new Error('ParseXML: unparsed remain already filled');
						this.emit('error', error);
					}
					chunkXMLRemain = chunk;
			}
			callback(error);
		}
	});
}

function saveProcessStats(callback) {
	var time = (Date.now() - tstart) * 0.001;
	var percent = Number(chunkPos[0]/fstat.size);
	var runChunkCount = subtractArrays(chunkCount, previousChunkCount);
	var runChunkPos = subtractArrays(chunkPos, previousChunkPos);
	var speed = runChunkPos[0] / time;
	fs.writeFile(fspath, JSON.stringify({
		xml: {
			tags: chunkXMLTags,
			index: chunkXMLIndex,
			current: chunkXMLCurrent,
			root: cleanXMLTree(chunkXMLStatsRoot),
			first: chunkXMLFirstChunks,
			last: chunkXMLLastChunks,
			remain: chunkXMLRemain
		},
		bzip: cleanBzStats(bzStats),
		fopt: cleanFopt(fopt),
		runs: previousRuns.concat([{
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
		}]),
		current: {
			percent: percent,
			time: totaltime + Math.round(time),
			timeStr: printTime(totaltime + time),
			chunkCount: chunkCount,
			chunkPos: chunkPos,
			bzFile: bzIndexFile,
			bzBlock: bzIndexBlock
		}
	}), function(err) {
		if (err) {
			console.error('Erro ao salvar o arquivo de estatísticas', err);
		} else {
			console.error('Arquivo de estatísticas salvo com sucesso');
		}
		callback && callback(err);
	});
}
function readProcessStats(callback) {
	fs.readFile(fspath, {encoding:'utf8'}, function(err, data) {
		if (err && err.code !== 'ENOENT') {
			callback(err);
		} else {
			data = data && JSON.parse(data);
			callback(null, data);
		}
	});
}
function continueProcess(err, stats) {
	if (err) return bzEnd(err);
	if (stats) {
		chunkXMLStatsRoot = polluteXMLTree(stats.xml.root);
		chunkXMLTags = stats.xml.tags;
		chunkXMLIndex = stats.xml.index;
		chunkXMLCurrent = stats.xml.current;
		chunkXMLStatsCurrent = chunkXMLStatsRoot;
		for (var i = 0; i < chunkXMLTags.length; i++) {
			chunkXMLStatsCurrent = chunkXMLStatsCurrent.tags[chunkXMLTags[i]];
		}
		bzStats = polluteBzStats(stats.bzip);
		fopt = polluteFopt(stats.fopt);
		previousRuns = stats.runs;
		chunkCount = stats.current.chunkCount;
		chunkPos = stats.current.chunkPos;
		previousChunkCount = chunkCount.slice();
		previousChunkPos = chunkPos.slice();
		bzIndexFile = stats.current.bzFile;
		bzIndexBlock = stats.current.bzBlock;
		pipe[3].s.write(stats.xml.remain.buffer);
	}
	bzProcess();
}

var pipe = [
	{s: strDecoder, n: 'strDecoder'},
	{s: new LineSplitter({ highWaterMark: 256 }), n: 'lineSplitter'},
	{s: streamPos(2), n: 'streamPos(2)'},
	{s: new ParseXML({ highWaterMark: 256 }), n: 'parseXML'},
	{s: createStreamXMLPos(), n: 'streamXMLPos'},
	{s: createStreamXMLCloseTag(), n: 'streamXMLCloseTag'}
];
for (var i = pipe.length-1; i >= 0; i--) {
	(function(pipe, i, pipeBefore) {
		// útil para debugar
		// var n = 'pipe '+i+' '+pipe.n;
		// pipe.s.on('error', function(err) {
		// 	console.error('Error '+n, err);
		// }).on('finish', function() {
		// 	console.error('Finish '+n);
		// }).on('end', function() {
		// 	console.error('End '+n);
		// });
		if (pipeBefore) pipeBefore.s.pipe(pipe.s);
	})(pipe[i], i, i > 0 ? pipe[i-1] : null);
}
pipe[3].s.on('error', bzEnd);
pipe[5].s.on('error', bzEnd);
pipe[5].s.on('finish', saveProcessStats);

console.error(dataSize(fstat.size));

process.on('SIGINT', function() {
	proximoDesce = true;
	console.error('\n\nParando no próximo ponto...\n');
});

readProcessStats(continueProcess);

function bzProcess() {
	try {
		fopt = SeekBzip.readBlock(bzRead, bzWrite, fopt);
		bzWrite.flush();
		// console.error(detailsBlock(fopt, formatBytes));
		if (fopt.streamCRC) {
			bzFinishFile(fopt);
			if (0 == (fopt.fileCount % 10)) {
				proximoDesce = true;
			}
		} else if (fopt.blockCRC) {
			bzFinishBlock(fopt);
		}

		var nextFn = ( !proximoDesce && (fopt.fileOffset < fstat.size) )
			? bzProcess
			: bzEnd;

		if (false && fopt.fileCount % 10) {
			process.nextTick(nextFn);
		} else {
			setTimeout(nextFn, 0);
		}
	} catch (err) {
		bzEnd(err);
	}
}

function bzEnd(err) {
	if (err) {
		if (err.errorCode === SeekBzip.Err.NOT_BZIP_DATA) {
			searchHexPi(fopt);
		}
		console.error(util.inspect(fopt, {depth: 0}));
		console.error('Teve um erro', err);
	}
	console.error(
		proximoDesce
		? 'Parou antes do final'
		: 'Você chegou no final, parabéns!'
	);
	fs.closeSync(fd);
	// fwrite.end();
	strDecoder.end();
}

function cleanXMLTree(tree) {
	var t = tree.tags;
	var ctags = {};
	for (var k in t) {
		if (hop.call(t, k)) {
			ctags[k] = cleanXMLTree(t[k]);
		}
	}
	return {
		tags: ctags,
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
	tree = parent ? {
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
	} : { tags: ctags };
	for (var k in t) {
		if (hop.call(t, k)) {
			ctags[k] = polluteXMLTree(t[k], tree);
		}
	}
	return tree;
}
function cleanFopt(fopt) {
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
	var fc = bzStats.length;
	var cfs = [];
	for (var i = 0; i < fc; i++) {
		var fi = bzStats[i];
		var fbl = fi.blocks;
		var fbc = fbl.length;
		var cbs = [];
		for (var j = 0; j < fbc; j++) {
			var fbi = fbl[j];
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
	var fc = bzStats.length;
	var cfs = [];
	for (var i = 0; i < fc; i++) {
		var fi = bzStats[i];
		var fbl = fi.blocks;
		var fbc = fbl.length;
		var cbs = [];
		for (var j = 0; j < fbc; j++) {
			var fbi = fbl[j];
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
		onBitStart: function(bit) {
			console.error('---- bit '+bit+' ----',{fp:bzRead.filePos,p:bzRead.pos,e:bzRead.end});
			console.error(bzRead.lastSeek);
		},
		onFound: function(res, bit) {
			console.error('. '.concat(res.c,' ',res.n,' ',res.ix,' ',res.s1,'-',res.s2));
		}
	});
	for (var i = 0; i < m.length; i++) {
		var mil = m[i].length;
		console.error(': bit '+i+(mil ? ' - '+mil+' matches' : ''));
		for (var j = 0; j < mil; j++) {
			console.error(m[i][j]);
		}
	}
	bzRead.fillBufferDisabled = false;
}
