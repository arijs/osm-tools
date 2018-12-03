var fs = require('fs');
var path = require('path');
var SeekBzip = require('@arijs/seek-bzip');
var dataSize = require('./datasize');

function printCRC(crc) {
	return ('00000000'+crc.toString(16)).substr(-8, 8);
}

var fpath = path.resolve(__dirname, '../planet-latest.osm.bz2');
var fstat = fs.statSync(fpath);
var fd = fs.openSync(fpath, 'r');
var tstart = Date.now();
var fopt;
function outPrint(buf) {
	if (buf.length <= 1024) {
		console.log(buf.toString('utf8'));
	}
}

console.log(dataSize(fstat.size));

try {

do {
	fopt = SeekBzip.readBlock(fd, fopt, null, 131072);
	if (fopt.streamCRC) {
		console.log('  '+fopt.fileCount+' file '+(fopt.fileOffset)+' bit '+fopt.bitOffsetEnd+' in '+(fopt.bytesInput)+' crc '+printCRC(fopt.streamCRC));
	} else if (fopt.blockCRC) {
		console.log(fopt.fileCount+'.'+fopt.blockCount+' block '+(fopt.byteOffset)+' bit '+fopt.bitOffset+' in '+(fopt.bytesInput)+' out '+(fopt.bytesOutput)+' crc '+printCRC(fopt.blockCRC));
		fopt.swrite.flush();
	}
} while (fopt.fileOffset < fstat.size);

} finally {
	fs.closeSync(fd);
}
