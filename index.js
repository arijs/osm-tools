var fs = require('fs');
var path = require('path');
var stream = require('stream');
var unbzip2 = require('unbzip2-stream');
var dataSize = require('./datasize');
var StreamingDecoder = require('./streaming-decoder');
var LineSplitter = require('./line-splitter');
var ParseXML = require('./parse-xml');

var chunkCount = [0, 0, 0];
var chunkPos = [0, 0, 0];
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
var chunkXMLTags = [];
var chunkXMLIndex = [];
var chunkXMLStatsRoot = { tags: {} };
var chunkXMLCurrent = 0;
var chunkXMLStatsCurrent = chunkXMLStatsRoot;
var chunkXMLLastChunks = new Array(20);
function getXMLTagPosStats() {
	return {
		count: chunkCount.slice(),
		pos: chunkPos.slice()
	};
}
function streamXMLPos() {
	return new stream.Transform({
		highWaterMark: 0,
		objectMode: true,
		transform: function(chunk, encoding, callback) {
			var error = null;
			var cname = chunk.name;
			var cxsNext;
			chunkXMLLastChunks = chunkXMLLastChunks.slice(1,19).concat([chunk]);
			switch (chunk.type) {
				case ParseXML.OPEN_TAG:
					chunkXMLTags.push(cname+'['+chunkXMLCurrent+']');
					chunkXMLIndex.push(chunkXMLCurrent);
					cxsNext = chunkXMLStatsCurrent.tags[cname];
					if (!cxsNext) {
						cxsNext = {
							parent: void 0,
							tags: {},
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
					chunkXMLStatsCurrent = cxsNext;
					chunkXMLCurrent = 0;
					break;
				case ParseXML.CLOSE_TAG:
					var tag = chunkXMLTags.pop();
					var index = chunkXMLIndex.pop();
					chunkXMLCurrent = index + 1;
					if (tag !== (cname+'['+index+']')) {
						error = new Error('Error tracking XML tags: expected '+tag+' but got '+cname);
					}
					if (!chunkXMLStatsCurrent.firstClose) {
						chunkXMLStatsCurrent.firstClose = getXMLTagPosStats();
					}
					chunkXMLStatsCurrent.lastClose = getXMLTagPosStats();
					cxsNext = chunkXMLStatsCurrent.parent;
					if (cxsNext) {
						// cxsNext.parent = void 0;
						chunkXMLStatsCurrent = cxsNext;
					} else {
						console.log(chunkXMLStatsRoot.tags);
						console.log(chunkXMLLastChunks);
						if (chunkXMLStatsCurrent === chunkXMLStatsRoot) {
							console.log('cxsCurrent is root');
						}
						error = new Error('Error tracking XML tags: parent got lost');
					}
					break;
			}
			callback(error, chunk);
		}
	});
}
function printTime(s) {
	var m = s/60;
	var h = m/60;
	var d = h/24;
	s = Math.floor(s % 60).toFixed(0);
	m = Math.floor(m % 60).toFixed(0);
	h = Math.floor(h % 24).toFixed(0);
	return (h < 1) ? ('  '+m).substr(-2)+':'+('00'+s).substr(-2) :
		(d <= 1) ? ('  '+h).substr(-2)+':'+('00'+m).substr(-2) :
		(d >= 100) ? (' '+d.toFixed(0)).substr(-4)+'d' :
		d.toFixed(d >= 10 ? 1 : 2)+'d';
}
function printXMLStats() {
	var time = (Date.now() - tstart) * 0.001;
	var percent = Number(chunkPos[0]/fstat.size);
	var speed = chunkPos[0] / time;
	var remain = (fstat.size - chunkPos[0]) / speed;
	process.stdout.write(
		// '/encoding: '+encoding+
		'\r'+percent.toFixed(5)+' '+dataSize(speed)+'/s'+
		' '+printTime(time)+' '+printTime(remain)+
		' / '+chunkCount.join(', ')+
		' / '+chunkPos.slice(0,2).map(dataSize).join(', ')+
		// ' / ('+chunkXMLTags.length+') '+chunkXMLTags.join('>')+
		' /'+printXMLStructure(chunkXMLStatsRoot)
		// ' /factor '+Number(chunkPos[0]/chunkPos[1]).toFixed(3)
	);
}
var hop = Object.prototype.hasOwnProperty;
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

var myWritable = new stream.Writable({
	highWaterMark: 0,
	objectMode: true,
	write(chunk, encoding, callback) {
		switch (chunk.type) {
			case ParseXML.ERROR:
				console.log('ERROR', String(chunk.error)+'\n');
				// no break on purpose
			case ParseXML.CLOSE_TAG:
				printXMLStats();
				// console.log(chunk);
				// process.stdout.write(chunk.toString());
				break;
		}
		callback();
	}
});

var fpath = path.resolve(__dirname, '../planet-latest.osm.bz2');
var fstat = fs.statSync(fpath);
var tstart = Date.now();

console.log(dataSize(fstat.size));

var ptTarget = new stream.PassThrough({ highWaterMark: 0 });
var ptTarget2 = ptTarget
// .pipe(streamPos(1))
.pipe(new StreamingDecoder({ highWaterMark: 0 }))
.pipe(streamPos(1))
.pipe(new LineSplitter({ highWaterMark: 0 }))
.pipe(streamPos(2))
.pipe(new ParseXML({ highWaterMark: 0 }))
.pipe(streamXMLPos())
.pipe(myWritable)
.on('finish', function() {
	console.log('process finished');
});

var maxZipChunks = 50000;
var zipChunkCount = [];
var readStream = fs.createReadStream(fpath);
var unbzip2Stream = unbzip2();
var ptSourcePos = streamPos(0);
var ptSource = new stream.PassThrough({ highWaterMark: 0 });
readStream
.pipe(ptSourcePos)
.pipe(unbzip2Stream)
.pipe(ptSource);
ptSource.on('data', function(chunk) {
	var chunk;
	// while (null != (chunk = ptSource.read())) {
	if (chunk && chunk.length) {
		if (maxZipChunks) ptTarget.write(chunk);
		zipChunkCount.push(chunk.length);
	}
	if (maxZipChunks && zipChunkCount.length == maxZipChunks) {
		console.log('\nsource read '+zipChunkCount.length);
		ptTarget.end();
		ptSourcePos.unpipe(unbzip2Stream);
		readStream.destroy();
		maxZipChunks = 0;
	}
});
ptSource.on('end', function() {
	console.log('ptSource end');
});
ptSource.on('finish', function() {
	console.log('ptSource finish');
});
