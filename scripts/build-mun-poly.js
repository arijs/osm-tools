'use strict';

/**
 * Gera `mun-poly.json` — contornos de MUNICÍPIO, para validar se a coordenada
 * gravada numa linha do DNE cai dentro do município a que a linha pertence
 * (ver `mun-poly.js` e a verificação no fim do `dne-geo-join.js`).
 *
 * Irmão do `build-uf-poly.js`: mesma fonte (API de malhas v3 do IBGE), mesma
 * simplificação, mesmo arquivo versionado no repositório porque o runtime não
 * pode depender de rede. A diferença é o `intrarregiao=municipio` e o recorte.
 *
 * ─── por que baixar por UF ────────────────────────────────────────────────
 * A malha do Brasil inteiro em `qualidade=maxima` é uma resposta só, grande e
 * sem retomada. Por UF são 27 respostas de ~1 MB, cada uma verificável, e a que
 * falhar se repete sozinha.
 *
 * ─── por que o recorte por DNE ────────────────────────────────────────────
 * São 5.570 municípios, mas só **636** têm logradouro no DNE — o resto é CEP
 * único, sem rua, e nunca vira uma linha para validar. Guardar os 5.570 custa
 * ~4 MB no repositório para responder a pergunta que ninguém faz. Com `--dne`
 * o arquivo fica na ordem de grandeza do `uf-poly.json` (398 KB).
 * Município fora do recorte responde "não sei" (null), nunca "fora".
 *
 *   node scripts/build-mun-poly.js --dne=DIR              # 27 UFs, recorte DNE
 *   node scripts/build-mun-poly.js --dne=DIR --uf=RJ,SP   # só essas UFs
 *   node scripts/build-mun-poly.js --todos                # sem recorte (~4 MB)
 *   node scripts/build-mun-poly.js --in=malha-rj.json     # de um arquivo local
 *   node scripts/build-mun-poly.js --cache=DIR            # reusa a malha crua
 *   node scripts/build-mun-poly.js --eps=0.002 --decimals=4
 */

var fs = require('fs');
var path = require('path');
var https = require('https');

var IBGE_UF = require('../uf-br').IBGE_UF;
var simplify = require('./build-uf-poly').simplify;

/** `{UF}` é trocado pelo código IBGE de 2 dígitos do estado. */
var URL_UF =
	'https://servicodados.ibge.gov.br/api/v3/malhas/estados/{UF}' +
	'?formato=application/vnd.geo+json&qualidade=maxima&intrarregiao=municipio';

var LICENCA =
	'IBGE — malhas territoriais, dado público de uso livre com citação da fonte';

/** Reverso do IBGE_UF: 'RJ' → '33'. */
var UF_IBGE = {};
Object.keys(IBGE_UF).forEach(function (cod) { UF_IBGE[IBGE_UF[cod]] = cod; });

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

function ringsOf(feature) {
	var out = [];
	var coords = feature.geometry.coordinates;
	if (feature.geometry.type === 'Polygon') coords = [coords];
	coords.forEach(function (poly) {
		// buracos (anéis internos) entram junto: o par-ímpar do `mun-poly.js` já
		// os trata sem precisar saber quem é buraco de quem
		poly.forEach(function (ring) { out.push(ring); });
	});
	return out;
}

/**
 * Acumula as features de uma malha em `acc`, simplificadas e arredondadas.
 * @param {object} acc  destino, `codarea` → anéis achatados
 * @param {object} geojson  resposta do IBGE
 * @param {number} eps  tolerância Douglas-Peucker, em graus
 * @param {number} decimals  casas decimais gravadas
 * @param {object|null} recorte  se dado, só os `codarea` presentes entram
 * @returns {{ municipios: number, pontos: number, pulados: number }}
 */
function acumular(acc, geojson, eps, decimals, recorte) {
	var mult = Math.pow(10, decimals);
	var n = 0, pontos = 0, pulados = 0;
	geojson.features.forEach(function (f) {
		var cod = String(f.properties.codarea);
		if (!/^\d{7}$/.test(cod)) throw new Error('codarea inesperado: ' + cod);
		if (recorte && !recorte[cod]) { pulados++; return; }
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
			pontos += s.length;
		});
		if (!rings.length) return;
		// maior anel primeiro: o bbox-check do runtime corta antes no caso comum
		rings.sort(function (a, b) { return b.length - a.length; });
		acc[cod] = rings;
		n++;
	});
	return { municipios: n, pontos: pontos, pulados: pulados };
}

// ------------------------------------------------------------ recorte por DNE

/**
 * Códigos IBGE dos municípios que têm logradouro no DNE. Distrito herda o IBGE
 * do município de subordinação, como no `dne-geo-join.js`.
 * @param {string} dneDir
 * @param {string[]|null} ufs  se dado, só esses LOG_LOGRADOURO_{UF}.TXT
 * @returns {object} mapa codarea → true
 */
function municipiosDoDne(dneDir, ufs) {
	var loc = new Map();
	fs.readFileSync(path.join(dneDir, 'LOG_LOCALIDADE.TXT'), 'latin1')
		.split(/\r?\n/).forEach(function (l) {
			if (!l) return;
			var p = l.split('@');
			loc.set(p[0], { sub: p[6] || '', mun: p[8] || '' });
		});
	loc.forEach(function (l) {
		if (!l.mun && l.sub) {
			var pai = loc.get(l.sub);
			if (pai && pai.mun) l.mun = pai.mun;
		}
	});
	var out = {};
	fs.readdirSync(dneDir).forEach(function (f) {
		var m = /^LOG_LOGRADOURO_([A-Z]{2})\.TXT$/.exec(f);
		if (!m) return;
		if (ufs && ufs.indexOf(m[1]) < 0) return;
		fs.readFileSync(path.join(dneDir, f), 'latin1').split(/\r?\n/).forEach(function (l) {
			if (!l) return;
			var r = loc.get(l.split('@')[2]);
			if (r && r.mun) out[r.mun] = true;
		});
	});
	return out;
}

// ------------------------------------------------------------------ gravação

function escrever(outPath, doc) {
	// um município por linha: o diff de uma atualização de malha fica legível
	var body = Object.keys(doc.municipios).sort().map(function (cod) {
		return '\t\t' + JSON.stringify(cod) + ': ' + JSON.stringify(doc.municipios[cod]);
	});
	var head = ['fonte', 'baixado_em', 'licenca', 'simplificacao', 'recorte'].map(function (k) {
		return '\t' + JSON.stringify(k) + ': ' + JSON.stringify(doc[k]);
	});
	var text = '{\n' + head.join(',\n') + ',\n\t"municipios": {\n' + body.join(',\n') + '\n\t}\n}\n';
	fs.writeFileSync(outPath, text);
	return text.length;
}

function hoje() {
	var d = new Date();
	return (
		d.getFullYear() + '-' +
		String(d.getMonth() + 1).padStart(2, '0') + '-' +
		String(d.getDate()).padStart(2, '0')
	);
}

function main() {
	var args = parseArgs(process.argv.slice(2));
	var eps = args.eps === undefined ? 0.002 : Number(args.eps);
	var decimals = args.decimals === undefined ? 4 : Number(args.decimals);
	var outPath = path.resolve(
		__dirname, '..',
		typeof args.out === 'string' ? args.out : 'mun-poly.json'
	);
	var ufs = typeof args.uf === 'string'
		? args.uf.split(',').map(function (s) { return s.trim().toUpperCase(); }).filter(Boolean)
		: Object.keys(UF_IBGE).sort();
	ufs.forEach(function (uf) {
		if (!UF_IBGE[uf]) throw new Error('UF desconhecida: ' + uf);
	});

	var recorte = null, rotuloRecorte = 'todos os municípios da malha';
	if (typeof args.dne === 'string') {
		recorte = municipiosDoDne(args.dne, args.uf ? ufs : null);
		rotuloRecorte = Object.keys(recorte).length +
			' municípios com logradouro no DNE (LOG_LOGRADOURO_*.TXT)';
		console.log('[recorte] ' + rotuloRecorte);
	} else if (!args.todos) {
		console.log('[recorte] sem --dne: gravando todos os municípios da malha (~4 MB)');
	}

	var municipios = {};
	var totalPontos = 0, totalPulados = 0;

	function finalizar(fonte) {
		var bytes = escrever(outPath, {
			fonte: fonte,
			baixado_em: typeof args.data === 'string' ? args.data : hoje(),
			licenca: LICENCA,
			simplificacao:
				'Douglas-Peucker eps=' + eps + '° (~' + Math.round(eps * 111000) + ' m), ' +
				decimals + ' casas',
			recorte: rotuloRecorte,
			municipios: municipios
		});
		console.log(
			outPath + ': ' + Object.keys(municipios).length + ' municípios, ' +
			totalPontos + ' pontos, ' + (bytes / 1024).toFixed(0) + ' KB' +
			(totalPulados ? ' (' + totalPulados + ' fora do recorte)' : '')
		);
	}

	if (typeof args.in === 'string') {
		var r = acumular(
			municipios, JSON.parse(fs.readFileSync(args.in, 'utf8')), eps, decimals, recorte
		);
		totalPontos += r.pontos;
		totalPulados += r.pulados;
		return finalizar('arquivo local: ' + args.in);
	}

	var cacheDir = typeof args.cache === 'string' ? args.cache : null;
	if (cacheDir) fs.mkdirSync(cacheDir, { recursive: true });

	var i = 0;
	(function proxima() {
		if (i >= ufs.length) return finalizar(URL_UF);
		var uf = ufs[i++];
		var url = URL_UF.replace('{UF}', UF_IBGE[uf]);
		var cached = cacheDir ? path.join(cacheDir, 'malha-mun-' + uf + '.json') : null;

		function usar(raw, origem) {
			var res = acumular(municipios, JSON.parse(raw), eps, decimals, recorte);
			totalPontos += res.pontos;
			totalPulados += res.pulados;
			console.log(
				'[' + i + '/' + ufs.length + '] ' + uf + ': ' + res.municipios + ' municípios, ' +
				res.pontos + ' pontos, ' + (raw.length / 1024 / 1024).toFixed(1) + ' MB ' + origem
			);
			proxima();
		}

		if (cached && fs.existsSync(cached)) return usar(fs.readFileSync(cached, 'utf8'), '(cache)');
		get(url, function (err, raw) {
			if (err) throw err;
			if (cached) fs.writeFileSync(cached, raw);
			usar(raw, '');
		});
	})();
}

if (require.main === module) main();

module.exports = {
	acumular: acumular,
	municipiosDoDne: municipiosDoDne,
	UF_IBGE: UF_IBGE,
	URL_UF: URL_UF
};
