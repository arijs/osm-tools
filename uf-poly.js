'use strict';

/**
 * UF de um ponto por GEOMETRIA REAL (polígono da malha do IBGE), no lugar do
 * retângulo de `uf-br.js`.
 *
 * ─── por que ──────────────────────────────────────────────────────────────
 * Os retângulos de UF se sobrepõem muito: o de GO cobre todo o Triângulo e o
 * Noroeste de MG, o da BA cobre o norte de MG. Qualquer desempate entre caixas
 * (menor área, primeira que casa) erra cidade inteira — Patrocínio saía GO e
 * Montes Claros saía BA. Retângulo não separa MG de GO; polígono separa.
 *
 * ─── como fica O(1) ──────────────────────────────────────────────────────
 * O extract chama isto por feature, milhões de vezes. Uma grade de células de
 * GRAU_CELULA° é montada na primeira consulta:
 *
 *   célula inteiramente dentro de uma UF → a resposta está na célula (sem PIP);
 *   célula cortada por divisa/costa      → lista de candidatas, e só aí roda
 *                                          point-in-polygon nas candidatas;
 *   célula sem nenhuma UF (mar/fora)     → ''.
 *
 * Divisa é da União: um ponto EXATAMENTE sobre um segmento conta como dentro de
 * todas as UFs que o compartilham, e vence a primeira em ordem alfabética —
 * regra arbitrária, mas determinística, que é o que o pipeline (retomável e
 * comparado entre execuções) exige.
 *
 * Ponto fora de todos os polígonos (mar aberto, plataforma, fora do Brasil)
 * responde '' — quem chama decide o fallback; `uf-br.js` cai no retângulo, que
 * é o comportamento antigo, para que nada que hoje tem rótulo vire XX.
 */

var DATA = null;

/** Carga tardia: `scripts/build-uf-poly.js` regenera o .json e não pode exigir
 * que a versão anterior dele exista para rodar. */
function dados() {
	if (!DATA) DATA = require('./uf-poly.json');
	return DATA;
}

/**
 * 0,25° ≈ 27 km. Célula menor deixa menos consulta caindo no caso caro (PIP),
 * mas a montagem testa o centro de cada célula e o custo sobe com o quadrado.
 * Os números medidos estão em test/uf-poly.test.js.
 */
var GRAU_CELULA = 0.25;

/** Tolerância do "ponto sobre a divisa" (os dados têm 4 casas decimais). */
var EPS = 1e-7;

var grid = null;

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

function onSegment(ax, ay, bx, by, x, y) {
	if (x < (ax < bx ? ax : bx) - EPS || x > (ax > bx ? ax : bx) + EPS) return false;
	if (y < (ay < by ? ay : by) - EPS || y > (ay > by ? ay : by) + EPS) return false;
	var cross = (bx - ax) * (y - ay) - (by - ay) * (x - ax);
	return cross < EPS && cross > -EPS;
}

/**
 * Par-ímpar sobre TODOS os anéis da UF — o buraco se resolve sozinho: o ponto
 * dentro dele cruza o anel externo e o interno, paridade par, fora.
 */
function ufContains(shape, x, y) {
	var inside = false;
	for (var n = 0; n < shape.rings.length; n++) {
		var box = shape.boxes[n];
		if (x < box.xMin || x > box.xMax || y < box.yMin || y > box.yMax) continue;
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

function buildGrid() {
	var D = dados();
	var ufs = Object.keys(D.ufs).sort();
	var shapes = {};
	var xMin = Infinity, xMax = -Infinity, yMin = Infinity, yMax = -Infinity;

	ufs.forEach(function (uf) {
		var rings = D.ufs[uf];
		var boxes = rings.map(ringBox);
		var box = { xMin: Infinity, xMax: -Infinity, yMin: Infinity, yMax: -Infinity };
		boxes.forEach(function (b) {
			if (b.xMin < box.xMin) box.xMin = b.xMin;
			if (b.xMax > box.xMax) box.xMax = b.xMax;
			if (b.yMin < box.yMin) box.yMin = b.yMin;
			if (b.yMax > box.yMax) box.yMax = b.yMax;
		});
		shapes[uf] = { rings: rings, boxes: boxes, box: box };
		if (box.xMin < xMin) xMin = box.xMin;
		if (box.xMax > xMax) xMax = box.xMax;
		if (box.yMin < yMin) yMin = box.yMin;
		if (box.yMax > yMax) yMax = box.yMax;
	});

	var nx = Math.ceil((xMax - xMin) / GRAU_CELULA) + 1;
	var ny = Math.ceil((yMax - yMin) / GRAU_CELULA) + 1;
	var cells = new Array(nx * ny);
	var ambiguas = 0;

	function markBox(x0, y0, x1, y1, uf) {
		var cx0 = Math.floor(((x0 < x1 ? x0 : x1) - xMin) / GRAU_CELULA);
		var cx1 = Math.floor(((x0 > x1 ? x0 : x1) - xMin) / GRAU_CELULA);
		var cy0 = Math.floor(((y0 < y1 ? y0 : y1) - yMin) / GRAU_CELULA);
		var cy1 = Math.floor(((y0 > y1 ? y0 : y1) - yMin) / GRAU_CELULA);
		for (var cy = cy0; cy <= cy1; cy++) {
			if (cy < 0 || cy >= ny) continue;
			for (var cx = cx0; cx <= cx1; cx++) {
				if (cx < 0 || cx >= nx) continue;
				var k = cy * nx + cx;
				var cur = cells[k];
				if (cur === undefined) cells[k] = [uf];
				else if (cur.indexOf(uf) < 0) cur.push(uf);
			}
		}
	}

	// células cortadas por divisa/costa: cada segmento é andado de meia célula em
	// meia célula e o bbox de cada passo marca as células tocadas — superset, e é
	// o lado seguro do erro (marcar demais custa um PIP, marcar de menos erra)
	var passo = GRAU_CELULA / 2;
	ufs.forEach(function (uf) {
		D.ufs[uf].forEach(function (r) {
			for (var i = 0, j = r.length - 2; i < r.length; j = i, i += 2) {
				var ax = r[j], ay = r[j + 1];
				var dx = r[i] - ax, dy = r[i + 1] - ay;
				var adx = dx < 0 ? -dx : dx, ady = dy < 0 ? -dy : dy;
				var passos = Math.ceil((adx > ady ? adx : ady) / passo) || 1;
				var px = ax, py = ay;
				for (var s = 1; s <= passos; s++) {
					var qx = ax + (dx * s) / passos, qy = ay + (dy * s) / passos;
					markBox(px, py, qx, qy, uf);
					px = qx;
					py = qy;
				}
			}
		});
	});

	// as demais são homogêneas: uma consulta no centro vale pela célula inteira
	for (var cy2 = 0; cy2 < ny; cy2++) {
		var y = yMin + (cy2 + 0.5) * GRAU_CELULA;
		for (var cx2 = 0; cx2 < nx; cx2++) {
			var k2 = cy2 * nx + cx2;
			if (cells[k2] !== undefined) {
				ambiguas++;
				continue;
			}
			var x = xMin + (cx2 + 0.5) * GRAU_CELULA;
			var achou = '';
			for (var u = 0; u < ufs.length; u++) {
				var sh = shapes[ufs[u]];
				if (x < sh.box.xMin || x > sh.box.xMax || y < sh.box.yMin || y > sh.box.yMax) continue;
				if (ufContains(sh, x, y)) {
					achou = ufs[u];
					break;
				}
			}
			cells[k2] = achou;
		}
	}

	return {
		ufs: ufs, shapes: shapes, xMin: xMin, yMin: yMin,
		nx: nx, ny: ny, cells: cells, ambiguas: ambiguas
	};
}

function ensureGrid() {
	if (!grid) grid = buildGrid();
	return grid;
}

var memoLat = NaN, memoLon = NaN, memoUf = '';

/**
 * UF do ponto pelo polígono, ou '' se cair fora de todas.
 * @param {number} lat
 * @param {number} lon
 * @returns {string} sigla ou ''
 */
function ufFromPointPoly(lat, lon) {
	if (lat == null || lon == null || !isFinite(lat) || !isFinite(lon)) return '';
	// o extract pergunta o mesmo ponto duas vezes seguidas (rótulo e filtro)
	if (lat === memoLat && lon === memoLon) return memoUf;
	var g = ensureGrid();
	var cx = Math.floor((lon - g.xMin) / GRAU_CELULA);
	var cy = Math.floor((lat - g.yMin) / GRAU_CELULA);
	var res = '';
	if (cx >= 0 && cx < g.nx && cy >= 0 && cy < g.ny) {
		var cell = g.cells[cy * g.nx + cx];
		if (typeof cell === 'string') {
			res = cell;
		} else {
			for (var i = 0; i < cell.length; i++) {
				if (ufContains(g.shapes[cell[i]], lon, lat)) {
					res = cell[i];
					break;
				}
			}
		}
	}
	memoLat = lat;
	memoLon = lon;
	memoUf = res;
	return res;
}

/** Diagnóstico da grade (teste de desempenho e sanidade). */
function gridStats() {
	var g = ensureGrid();
	return {
		celula: GRAU_CELULA,
		nx: g.nx,
		ny: g.ny,
		celulas: g.nx * g.ny,
		ambiguas: g.ambiguas,
		ufs: g.ufs.length,
		fonte: dados().fonte,
		baixado_em: dados().baixado_em
	};
}

module.exports = {
	ufFromPointPoly: ufFromPointPoly,
	gridStats: gridStats,
	GRAU_CELULA: GRAU_CELULA
};
