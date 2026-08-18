'use strict';

/**
 * Gera `uf-poly.json` — contornos de UF para resolver a UF de um ponto por
 * geometria real (ver `uf-poly.js`).
 *
 * Fonte: malha de UF do IBGE (API de malhas v3). O arquivo gerado é versionado
 * no repositório porque o runtime não pode depender de rede; este script só
 * roda quando a malha for atualizada.
 *
 *   node scripts/build-uf-poly.js                    # baixa do IBGE
 *   node scripts/build-uf-poly.js --in=malha.json    # a partir de um arquivo
 *   node scripts/build-uf-poly.js --eps=0.005        # tolerância Douglas-Peucker
 */

var fs = require('fs');
var path = require('path');
var https = require('https');

var IBGE_UF = require('../uf-br').IBGE_UF;

var URL =
	'https://servicodados.ibge.gov.br/api/v3/malhas/paises/BR' +
	'?formato=application/vnd.geo+json&qualidade=maxima&intrarregiao=UF';

var LICENCA =
	'IBGE — malhas territoriais, dado público de uso livre com citação da fonte';

function parseArgs(argv) {
	var out = {};
	argv.forEach(function (a) {
		var m = /^--([^=]+)(?:=(.*))?$/.exec(a);
		if (m) out[m[1]] = m[2] === undefined ? true : m[2];
	});
	return out;
}

function get(url, cb) {
	https
		.get(url, function (res) {
			if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
				res.resume();
				return get(res.headers.location, cb);
			}
			if (res.statusCode !== 200) {
				res.resume();
				return cb(new Error('HTTP ' + res.statusCode + ' em ' + url));
			}
			var buf = '';
			res.setEncoding('utf8');
			res.on('data', function (c) { buf += c; });
			res.on('end', function () { cb(null, buf); });
		})
		.on('error', cb);
}

/**
 * Douglas-Peucker em graus (iterativo — os anéis do IBGE chegam a 3.7k pontos e
 * a versão recursiva estoura a pilha em máquina pequena).
 */
function simplify(pts, eps) {
	if (pts.length < 3) return pts;
	var keep = new Uint8Array(pts.length);
	keep[0] = 1;
	keep[pts.length - 1] = 1;
	var stack = [[0, pts.length - 1]];
	while (stack.length) {
		var seg = stack.pop();
		var i = seg[0];
		var j = seg[1];
		var ax = pts[i][0];
		var ay = pts[i][1];
		var dx = pts[j][0] - ax;
		var dy = pts[j][1] - ay;
		var dd = dx * dx + dy * dy;
		var best = -1;
		var bestD = eps;
		for (var k = i + 1; k < j; k++) {
			var px = pts[k][0] - ax;
			var py = pts[k][1] - ay;
			var d;
			if (dd === 0) {
				d = Math.sqrt(px * px + py * py);
			} else {
				var t = (px * dx + py * dy) / dd;
				if (t < 0) t = 0;
				else if (t > 1) t = 1;
				var ex = px - t * dx;
				var ey = py - t * dy;
				d = Math.sqrt(ex * ex + ey * ey);
			}
			if (d > bestD) {
				bestD = d;
				best = k;
			}
		}
		if (best > 0) {
			keep[best] = 1;
			stack.push([i, best]);
			stack.push([best, j]);
		}
	}
	var out = [];
	for (var m = 0; m < pts.length; m++) if (keep[m]) out.push(pts[m]);
	return out;
}

function ringsOf(feature) {
	var out = [];
	var coords = feature.geometry.coordinates;
	if (feature.geometry.type === 'Polygon') coords = [coords];
	coords.forEach(function (poly) {
		// buracos (anéis internos) entram junto: o teste par-ímpar em uf-poly.js
		// já os trata sem precisar saber quem é buraco de quem
		poly.forEach(function (ring) { out.push(ring); });
	});
	return out;
}

function build(geojson, eps, decimals) {
	var mult = Math.pow(10, decimals);
	var ufs = {};
	var total = 0;
	geojson.features.forEach(function (f) {
		var uf = IBGE_UF[String(f.properties.codarea).slice(0, 2)];
		if (!uf) throw new Error('codarea sem UF: ' + f.properties.codarea);
		var rings = [];
		ringsOf(f).forEach(function (ring) {
			var s = simplify(ring, eps);
			// anel degenerado depois da simplificação (ilhota) não define área
			if (s.length < 4) return;
			var flat = [];
			for (var i = 0; i < s.length; i++) {
				flat.push(Math.round(s[i][0] * mult) / mult, Math.round(s[i][1] * mult) / mult);
			}
			rings.push(flat);
			total += s.length;
		});
		rings.sort(function (a, b) { return b.length - a.length; });
		ufs[uf] = rings;
	});
	var ordered = {};
	Object.keys(ufs).sort().forEach(function (k) { ordered[k] = ufs[k]; });
	return { ufs: ordered, total: total };
}

function main() {
	var args = parseArgs(process.argv.slice(2));
	var eps = args.eps === undefined ? 0.005 : Number(args.eps);
	var decimals = args.decimals === undefined ? 4 : Number(args.decimals);
	var outPath = path.resolve(
		__dirname,
		'..',
		typeof args.out === 'string' ? args.out : 'uf-poly.json'
	);

	function emit(raw, origem) {
		var geojson = JSON.parse(raw);
		var built = build(geojson, eps, decimals);
		var doc = {
			fonte: origem,
			baixado_em: typeof args.data === 'string' ? args.data : hoje(),
			licenca: LICENCA,
			simplificacao: 'Douglas-Peucker eps=' + eps + '° (~' + Math.round(eps * 111000) + ' m), ' + decimals + ' casas',
			ufs: built.ufs
		};
		// uma UF por linha: o diff de uma atualização de malha fica legível
		var body = Object.keys(doc.ufs).map(function (uf) {
			return '\t\t' + JSON.stringify(uf) + ': ' + JSON.stringify(doc.ufs[uf]);
		});
		var head = ['fonte', 'baixado_em', 'licenca', 'simplificacao'].map(function (k) {
			return '\t' + JSON.stringify(k) + ': ' + JSON.stringify(doc[k]);
		});
		var text = '{\n' + head.join(',\n') + ',\n\t"ufs": {\n' + body.join(',\n') + '\n\t}\n}\n';
		fs.writeFileSync(outPath, text);
		console.log(
			outPath + ': ' + Object.keys(built.ufs).length + ' UFs, ' +
			built.total + ' pontos, ' + (text.length / 1024).toFixed(0) + ' KB'
		);
	}

	if (typeof args.in === 'string') {
		emit(fs.readFileSync(args.in, 'utf8'), 'arquivo local: ' + args.in);
		return;
	}
	get(URL, function (err, raw) {
		if (err) throw err;
		emit(raw, URL);
	});
}

function hoje() {
	var d = new Date();
	return (
		d.getFullYear() + '-' +
		String(d.getMonth() + 1).padStart(2, '0') + '-' +
		String(d.getDate()).padStart(2, '0')
	);
}

if (require.main === module) main();

module.exports = { simplify: simplify, build: build };
