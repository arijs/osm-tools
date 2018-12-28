var fs = require('fs');
var path = require('path');

function getFilePath(dir, base, ext) {
	return path.resolve(dir, base + (ext || ''));
}

var fname = process.argv[2];
var fext = path.extname(fname);
var fbase = path.basename(fname, fext);
var fdir = path.dirname(path.resolve(process.cwd(), fname));
var fpath = getFilePath(fdir, fbase, fext);

fs.readFile(fpath, {encoding: 'utf8'}, function(err, data) {
	if (err) throw err;
	data = JSON.parse(data);
	fs.writeFile(
		getFilePath(fdir, fbase+'-xml', fext),
		JSON.stringify(data.xml, null, '\t'),
		function(err) {
			if (err) {
				console.error('Erro salvando xml', err);
			} else {
				console.error('xml salvo com sucesso');
			}
		}
	);
	fs.writeFile(
		getFilePath(fdir, fbase+'-bzip', fext),
		JSON.stringify(data.bzip, null, '\t'),
		function(err) {
			if (err) {
				console.error('Erro salvando bzip', err);
			} else {
				console.error('bzip salvo com sucesso');
			}
		}
	);
	fs.writeFile(
		getFilePath(fdir, fbase+'-fopt', fext),
		JSON.stringify(data.fopt, null, '\t'),
		function(err) {
			if (err) {
				console.error('Erro salvando fopt', err);
			} else {
				console.error('fopt salvo com sucesso');
			}
		}
	);
	fs.writeFile(
		getFilePath(fdir, fbase+'-runs', fext),
		JSON.stringify(data.runs, null, '\t'),
		function(err) {
			if (err) {
				console.error('Erro salvando runs', err);
			} else {
				console.error('runs salvo com sucesso');
			}
		}
	);
	fs.writeFile(
		getFilePath(fdir, fbase+'-current', fext),
		JSON.stringify(data.current, null, '\t'),
		function(err) {
			if (err) {
				console.error('Erro salvando current', err);
			} else {
				console.error('current salvo com sucesso');
			}
		}
	);
});
