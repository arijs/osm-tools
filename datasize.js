
var dataUnits = 'B KB MB GB TB PB'.split(' ');
function dataSize(bytecount) {
	var i = 0;
	while (bytecount > 1024 && i < 5) {
		bytecount /= 1024;
		i++;
	}
	return bytecount.toFixed(i)+' '+dataUnits[i];
}

module.exports = dataSize;
