var fs = require('fs');
var path = require('path');
var util = require('util');
var SeekBzip = require('./seek-bzip');
var dataSize = require('./datasize');
var StreamingDecoder = require('./streaming-decoder');

function printCRC(crc) {
	return ('00000000'+crc.toString(16)).substr(-8, 8);
}

var fpath = path.resolve(__dirname, '../planet-latest.osm.bz2');
var fstat = fs.statSync(fpath);
var fd = fs.openSync(fpath, 'r');
var tstart = Date.now();
var strDec = new StreamingDecoder({ highWaterMark: 0 });
strDec.on('readable', function() {
	var data;
	while (data = this.read()) {
		console.log(data.toString('utf8'));
	}
});
var proximoDesce = false;
var fopt;
var bzRead = SeekBzip.fdReadStream(fd, 16384, 0);
var bzWrite = SeekBzip.readBlock.makeOutStream(outPrint, 16384);
function outPrint(buf) {
	// if (buf.length <= 1024) {
	// 	console.log(buf.toString('utf8'));
	// }
	strDec.write(buf);
}

console.error(dataSize(fstat.size));

process.on('SIGINT', function() {
	proximoDesce = true;
	console.error('\n\nParando no próximo ponto...\n\n');
});

bzProcess();

function formatBytes(x) {
	return Number(x).toFixed(0);
}
function detailsBlock(fopt, fSize) {
	fSize || (fSize = formatBytes);
	if (fopt.streamCRC) {
		return (
			'  '+fopt.fileCount+
			' file '+fSize(fopt.fileOffset)+
			' b.'+fopt.bitOffsetEnd+
			' in '+fSize(fopt.bytesInput)+
			' crc '+printCRC(fopt.streamCRC)
		);
	} else if (fopt.blockCRC) {
		return (
			fopt.fileCount+'.'+fopt.blockCount+
			' block '+fSize(fopt.byteOffset)+
			' b.'+fopt.bitOffset+
			' in '+fSize(fopt.bytesInput)+
			' out '+fSize(fopt.bytesOutput)+
			' crc '+printCRC(fopt.blockCRC)
		);
		fopt.swrite.flush();
	} else {
		return ('no block and no stream ' + util.inspect(fopt, {depth: 0}));
	}
}

function bzProcess() {
	try {
		fopt = SeekBzip.readBlock(bzRead, bzWrite, fopt);
		console.error(detailsBlock(fopt, formatBytes));//dataSize));
		var nextFn = ( !proximoDesce && (fopt.fileOffset < fstat.size) )
			? bzProcess
			: bzEnd;
		if (fopt.fileCount % 5) {
			process.nextTick(nextFn);
		} else {
			setTimeout(nextFn, 0);
		}
	} catch (err) {
		bzEnd(err);
	}
}

function searchHexString(hexString, bz, bzRead) {
	var len = hexString.length;
	var lbytes = Math.ceil(len*0.5);
	var startBit = bz.reader.bitOffset;
	var filePos = bzRead.filePos;
	var bzPos = bzRead.pos;
	var bzEnd = bzRead.end;
	var bzStart = filePos - bzEnd;
	var attempts = [];
	for (var bit = 0; bit < 8; bit++) {
		var bitAtt = [];
		bz.reader.seek(bzStart, bit);
		var remain = bzRead.end - (bit ? 1 : 0);
		var getSubstr = function() {
			var n = Math.min(lbytes, remain);
			var s = n > 0 ? bz.reader.readBytes(n).toString('hex') : '';
			remain -= n > 0 ? n : 0;
			return s;
		}
		var fstart = remain;
		var start1 = 0;
		var prev;
		var next = getSubstr();
		var spos = lbytes;
		var cut = 0;
		var start2 = fstart - remain;
		do {
			prev = next;
			next = getSubstr();
			var ix = String(prev+next).indexOf(hexString);
			if (ix != -1) {
				bitAtt.push({
					ix: ix,
					p: prev,
					n: next,
					c: cut,
					s1: start1,
					s2: start2,
					fs: fstart,
					r: remain
				});
			}
			cut++;
			start1 = start2;
			start2 = fstart - remain;
		} while (remain > 0);
		attempts.push(bitAtt);
	}
	bz.reader.seek(filePos - bzEnd + bzPos, startBit);
	return attempts;
}

function bzEnd(err) {
	fs.closeSync(fd);
	strDec.end();
	if (err) {
		if (err.errorCode === SeekBzip.Err.NOT_BZIP_DATA) {
			var pi = '314159265359';
			console.error(searchHexString(pi, fopt.bz, bzRead));
			// var rbuf = bzRead.buffer.toString('hex');
			// var index = rbuf.indexOf(pi);
			// var sub = (index == -1) ? null : [
			// 	rbuf.substring(Math.max(0, index-48), index),
			// 	rbuf.substring(index, index+12),
			// 	rbuf.substring(index+12, index+48),
			// ];
			// console.error({
			// 	index: index,
			// 	sub: sub,
			// 	filePos: bzRead.filePos,
			// 	pos: bzRead.pos,
			// 	end: bzRead.pos,
			// 	blen: bzRead.buffer.length,
			// 	lsFilePos: bzRead.lastStats.filePos,
			// 	lsPos: bzRead.lastStats.pos,
			// 	lsEnd: bzRead.lastStats.end
			// });
		}
		console.error(util.inspect(fopt, {depth: 0}));
		console.error('Teve um erro', err);
	}
	console.error(
		proximoDesce
		? 'Parou antes do final'
		: 'Você chegou no final, parabéns!'
	);
}
