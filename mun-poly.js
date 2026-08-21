'use strict';

/**
 * "Esta coordenada cai dentro do município a que a linha diz pertencer?" —
 * point-in-polygon contra a malha municipal do IBGE (`mun-poly.json`).
 *
 * ─── por que ──────────────────────────────────────────────────────────────
 * O pior modo de falha do `dne-geo-join.js` é silencioso: a linha sai `ok`, com
 * coordenada, e o consumidor desenha no mapa com ar de acerto. A defesa de hoje
 * é a pegada por âncoras (células do OSM em volta do que casou sozinho), que é
 * uma aproximação do município feita com o dado que se quer validar. Ela deixa
 * passar ~10% — em RJ, 6.8 mil linhas `ok` com centróide fora do município.
 * O polígono do IBGE é independente do join, então enxerga esse resíduo.
 *
 * ─── por que sem grade, ao contrário do `uf-poly.js` ──────────────────────
 * Lá a pergunta é "qual UF?", uma busca entre 27 polígonos, e a grade evita a
 * varredura. Aqui **o município já vem dado** (coluna 14 do DNE, código IBGE de
 * 7 dígitos): não há busca, só o teste contra um polígono de ~130 pontos. Medido
 * no re-join de RJ — 70.797 consultas `ok`, com a distância da borda para as que
 * caem fora, em ~1,5 s. Grade aqui seria índice para uma busca que não existe.
 *
 * A carga é preguiçosa e o shape de cada município é montado na primeira
 * pergunta sobre ele: um join de UF toca algumas dezenas dos 636 municípios.
 *
 * ─── convenções ──────────────────────────────────────────────────────────
 * - `null` é "não sei" (município fora do recorte da malha, ou malha ausente) e
 *   nunca deve virar "fora" na contagem de quem chama;
 * - ponto EXATAMENTE sobre a divisa conta como **dentro**, igual ao `uf-poly.js`
 *   — a divisa é dos dois municípios que a compartilham;
 * - buraco (anel interno do MultiPolygon) se resolve por paridade: o ponto
 *   dentro dele cruza o anel externo e o interno, par, fora.
 */

var fs = require('fs');
var path = require('path');

var CAMINHO_PADRAO = path.join(__dirname, 'mun-poly.json');

/** Tolerância do "ponto sobre a divisa" (os dados têm 4 casas decimais). */
var EPS = 1e-7;

/** Grau de latitude em km (WGS84 médio) — o mesmo fator do `geo-cluster.js`. */
var KM_POR_GRAU = 111.19492664455873;

/** `null` enquanto ninguém perguntou; `false` quando a malha não existe. */
var estado = null;
var caminhoAtual = null;

function carregar(caminho) {
	var texto;
	try {
		texto = fs.readFileSync(caminho, 'utf8');
	} catch (e) {
		return false;
	}
	var dados = JSON.parse(texto);
	return { caminho: caminho, dados: dados, shapes: Object.create(null) };
}

function atual() {
	if (estado === null) {
		caminhoAtual = CAMINHO_PADRAO;
		estado = carregar(CAMINHO_PADRAO);
	}
	return estado;
}

/**
 * Troca a fonte da malha (teste, ou uma malha maior gerada fora do repositório).
 * Repetir o mesmo caminho não relê o arquivo — o `dne-geo-join.js` chama isto
 * uma vez por run, e a suíte roda vários runs no mesmo processo.
 * @param {string|null} caminho  `null` volta ao `mun-poly.json` do repositório
 * @returns {boolean} true se a malha carregou
 */
function usarArquivo(caminho) {
	var alvo = caminho === null || caminho === undefined ? CAMINHO_PADRAO : caminho;
	if (estado !== null && alvo === caminhoAtual) return estado !== false;
	caminhoAtual = alvo;
	estado = carregar(alvo);
	return estado !== false;
}

/** @returns {boolean} há malha carregada? */
function disponivel() {
	return atual() !== false;
}

/** Cabeçalho da malha (fonte, data, simplificação, recorte, nº de municípios). */
function meta() {
	var e = atual();
	if (!e) return null;
	return {
		fonte: e.dados.fonte,
		baixado_em: e.dados.baixado_em,
		simplificacao: e.dados.simplificacao,
		recorte: e.dados.recorte,
		municipios: Object.keys(e.dados.municipios).length
	};
}

// ------------------------------------------------------------------ geometria

function ringBox(r) {
	var xMin = Infinity, xMax = -Infinity, yMin = Infinity, yMax = -Infinity;
	for (var i = 0; i < r.length; i += 2) {
		if (r[i] < xMin) xMin = r[i];
		if (r[i] > xMax) xMax = r[i];
		if (r[i + 1] < yMin) yMin = r[i + 1];
		if (r[i + 1] > yMax) yMax = r[i + 1];
	}
	return { xMin: xMin, xMax: xMax, yMin: yMin, yMax: yMax };
}

/**
 * Anéis achatados (`[x0,y0,x1,y1,…]`, em `[lng,lat]`) → shape consultável.
 * @param {number[][]} rings
 */
function montarShape(rings) {
	var boxes = rings.map(ringBox);
	var box = { xMin: Infinity, xMax: -Infinity, yMin: Infinity, yMax: -Infinity };
	boxes.forEach(function (b) {
		if (b.xMin < box.xMin) box.xMin = b.xMin;
		if (b.xMax > box.xMax) box.xMax = b.xMax;
		if (b.yMin < box.yMin) box.yMin = b.yMin;
		if (b.yMax > box.yMax) box.yMax = b.yMax;
	});
	return { rings: rings, boxes: boxes, box: box };
}

function onSegment(ax, ay, bx, by, x, y) {
	if (x < (ax < bx ? ax : bx) - EPS || x > (ax > bx ? ax : bx) + EPS) return false;
	if (y < (ay < by ? ay : by) - EPS || y > (ay > by ? ay : by) + EPS) return false;
	var cross = (bx - ax) * (y - ay) - (by - ay) * (x - ax);
	return cross < EPS && cross > -EPS;
}

/**
 * Par-ímpar sobre todos os anéis, com o segmento contando como dentro.
 * @param {object} shape  de `montarShape`
 * @param {number} x  longitude
 * @param {number} y  latitude
 */
function contemPonto(shape, x, y) {
	if (x < shape.box.xMin || x > shape.box.xMax) return false;
	if (y < shape.box.yMin || y > shape.box.yMax) return false;
	var inside = false;
	for (var n = 0; n < shape.rings.length; n++) {
		var b = shape.boxes[n];
		if (x < b.xMin || x > b.xMax || y < b.yMin || y > b.yMax) continue;
		var r = shape.rings[n];
		for (var i = 0, j = r.length - 2; i < r.length; j = i, i += 2) {
			var xi = r[i], yi = r[i + 1], xj = r[j], yj = r[j + 1];
			if (onSegment(xi, yi, xj, yj, x, y)) return true;
			if ((yi > y) !== (yj > y) && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) {
				inside = !inside;
			}
		}
	}
	return inside;
}

/**
 * Distância (km) do ponto ao segmento de divisa mais próximo — sem sinal, serve
 * igual para dentro e fora. Equirretangular local: o erro é desprezível nas
 * dezenas de km que interessam, e a alternativa (haversine por segmento) custa
 * uma trigonometria por ponto do anel.
 * @param {object} shape  de `montarShape`
 * @param {number} x  longitude
 * @param {number} y  latitude
 */
function distanciaKm(shape, x, y) {
	var kx = KM_POR_GRAU * Math.cos((y * Math.PI) / 180);
	var ky = KM_POR_GRAU;
	var best = Infinity;
	for (var n = 0; n < shape.rings.length; n++) {
		var r = shape.rings[n];
		for (var i = 0, j = r.length - 2; i < r.length; j = i, i += 2) {
			var ax = (r[j] - x) * kx, ay = (r[j + 1] - y) * ky;
			var bx = (r[i] - x) * kx, by = (r[i + 1] - y) * ky;
			var dx = bx - ax, dy = by - ay;
			var dd = dx * dx + dy * dy;
			var d;
			if (dd === 0) {
				d = Math.sqrt(ax * ax + ay * ay);
			} else {
				var t = -(ax * dx + ay * dy) / dd;
				if (t < 0) t = 0;
				else if (t > 1) t = 1;
				var ex = ax + t * dx, ey = ay + t * dy;
				d = Math.sqrt(ex * ex + ey * ey);
			}
			if (d < best) best = d;
		}
	}
	return best;
}

// -------------------------------------------------------------------- consulta

function shapeDe(ibge) {
	var e = atual();
	if (!e) return null;
	var cod = String(ibge || '');
	var cache = e.shapes;
	if (cache[cod] !== undefined) return cache[cod];
	var rings = e.dados.municipios[cod];
	return (cache[cod] = rings ? montarShape(rings) : null);
}

/**
 * O ponto está dentro do município?
 * @param {string|number} ibge  código IBGE de 7 dígitos
 * @param {number} lat
 * @param {number} lng
 * @returns {boolean|null} `null` = município desconhecido ou coordenada inválida
 */
function dentroDoMunicipio(ibge, lat, lng) {
	if (lat == null || lng == null || !isFinite(lat) || !isFinite(lng)) return null;
	var shape = shapeDe(ibge);
	if (!shape) return null;
	return contemPonto(shape, lng, lat);
}

/**
 * Distância do ponto à divisa do município, em km (dentro ou fora).
 * @param {string|number} ibge  código IBGE de 7 dígitos
 * @param {number} lat
 * @param {number} lng
 * @returns {number|null} `null` = município desconhecido ou coordenada inválida
 */
function distanciaDaBordaKm(ibge, lat, lng) {
	if (lat == null || lng == null || !isFinite(lat) || !isFinite(lng)) return null;
	var shape = shapeDe(ibge);
	if (!shape) return null;
	return distanciaKm(shape, lng, lat);
}

module.exports = {
	dentroDoMunicipio: dentroDoMunicipio,
	distanciaDaBordaKm: distanciaDaBordaKm,
	disponivel: disponivel,
	meta: meta,
	usarArquivo: usarArquivo,
	CAMINHO_PADRAO: CAMINHO_PADRAO,
	// puros, para teste e para quem quiser montar shape de outra fonte
	montarShape: montarShape,
	contemPonto: contemPonto,
	distanciaKm: distanciaKm
};
