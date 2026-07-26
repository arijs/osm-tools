'use strict';
var fs = require('fs');
var path = require('path');

var dir = process.argv[2] || 'H:\\osm';
var files = fs.readdirSync(dir).filter(function (f) {
	return /-stats\d+\.json$/i.test(f);
});

if (!files.length) {
	console.error('Nenhum arquivo *-stats[digitos].json em', dir);
	process.exit(1);
}

files.forEach(function (f) {
	var p = path.join(dir, f);
	var raw = fs.readFileSync(p, 'utf8');
	var data = JSON.parse(raw);
	var had = Object.prototype.hasOwnProperty.call(data, 'bzip');
	var bzipInfo = had
		? Array.isArray(data.bzip)
			? 'array len=' + data.bzip.length
			: typeof data.bzip
		: 'absent';
	if (had) {
		delete data.bzip;
		fs.writeFileSync(p, JSON.stringify(data));
	}
	var after = JSON.parse(fs.readFileSync(p, 'utf8'));
	console.log(f, {
		hadBzip: had,
		bzipInfo: bzipInfo,
		stillHasBzip: Object.prototype.hasOwnProperty.call(after, 'bzip'),
		sizeBefore: raw.length,
		sizeAfter: fs.statSync(p).size
	});
});
