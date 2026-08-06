'use strict';

/**
 * Polyline em inteiros — a geometria de uma way dentro de um campo do TXT `@`.
 *
 * Ver docs/geo/geometria-via-destaque.md §Codificação.
 *
 * Formato: pontos separados por `;`, cada um `lat,lng` em UNIDADES DE 1e-6 GRAU
 * (inteiros). O primeiro ponto é absoluto; os seguintes são DELTAS do anterior.
 *
 *     -23552000,-46632000;-1000,-1000;2000,500
 *
 * Três decisões que valem explicação:
 *
 * **Por que não o polyline codificado do Google.** O alfabeto dele vai de 63 a
 * 126 na tabela ASCII, e 64 é `@` — o delimitador destes arquivos. O
 * `txt-at-writer` sanitiza `@` para espaço, então a geometria seria corrompida
 * em silêncio, sem erro nenhum, e só apareceria como traçado torto no mapa.
 * O alfabeto daqui é dígito, `-`, `,` e `;`: nada que o sanitizador toque.
 *
 * **Por que delta.** Os pontos de uma via são vizinhos: o absoluto custa ~19
 * caracteres e cada delta custa 2 a 4. Numa way de 10 nós é a diferença entre
 * ~190 e ~60 bytes, e o arquivo inteiro de SP entre ~150 MB e ~50 MB.
 *
 * **Por que 1e-6 (≈ 0,11 m) e não 1e-5 (≈ 1,1 m).** 1,1 m dá 4 a 5 pixels no
 * zoom 18 do mapa — o traçado ficaria escadinha visível em curva. Uma casa
 * decimal a mais custa um dígito por delta e resolve.
 *
 * Nada aqui simplifica a geometria (Douglas-Peucker e afins): o que entra é o
 * que o OSM tem, e a única perda é o arredondamento. Simplificar é decisão de
 * quem desenha, e pode ser feita depois sem regerar o artefato.
 */

/** Unidades por grau. `1e6` → resolução de ~0,11 m. */
var PRECISAO = 1e6;

var SEP_PONTO = ';';
var SEP_COORD = ',';

/**
 * Codifica `[[lat, lng], …]`. Pontos consecutivos que colapsam no mesmo inteiro
 * após o arredondamento são descartados — não é simplificação, é remoção de
 * repetição que não desenha nada. Ponto repetido NÃO consecutivo fica (é assim
 * que uma way fechada — praça, parque — mantém o anel fechado).
 *
 * @param {Array<Array<number>>} pontos
 * @param {number} [precisao]
 * @returns {string} vazio quando não há ponto nenhum
 */
function encodePolyline(pontos, precisao) {
	precisao = precisao || PRECISAO;
	if (!pontos || !pontos.length) return '';
	var partes = [];
	var prevLat = 0;
	var prevLng = 0;
	for (var i = 0; i < pontos.length; i++) {
		var p = pontos[i];
		if (!p || p.length < 2) continue;
		var lat = Math.round(p[0] * precisao);
		var lng = Math.round(p[1] * precisao);
		if (!isFinite(lat) || !isFinite(lng)) continue;
		if (partes.length === 0) {
			partes.push(lat + SEP_COORD + lng);
		} else {
			if (lat === prevLat && lng === prevLng) continue;
			partes.push(lat - prevLat + SEP_COORD + (lng - prevLng));
		}
		prevLat = lat;
		prevLng = lng;
	}
	return partes.join(SEP_PONTO);
}

/**
 * Decodifica de volta para `[[lat, lng], …]` em graus.
 *
 * Lança em entrada malformada em vez de devolver lista curta: geometria pela
 * metade desenha uma via que existe e está errada, que é pior do que não
 * desenhar. Quem carrega em volume decide se aborta ou se conta e segue.
 *
 * @param {string} txt
 * @param {number} [precisao]
 * @returns {Array<Array<number>>}
 */
function decodePolyline(txt, precisao) {
	precisao = precisao || PRECISAO;
	if (txt === null || txt === undefined) return [];
	var s = String(txt).trim();
	if (!s) return [];
	var partes = s.split(SEP_PONTO);
	var out = [];
	var lat = 0;
	var lng = 0;
	for (var i = 0; i < partes.length; i++) {
		var par = partes[i].split(SEP_COORD);
		if (par.length !== 2) {
			throw new Error('polyline malformada no ponto ' + (i + 1) + ': ' + partes[i]);
		}
		var a = Number(par[0]);
		var b = Number(par[1]);
		if (!isFinite(a) || !isFinite(b) || par[0] === '' || par[1] === '') {
			throw new Error('polyline com coordenada não numérica no ponto ' + (i + 1));
		}
		if (i === 0) {
			lat = a;
			lng = b;
		} else {
			lat += a;
			lng += b;
		}
		out.push([lat / precisao, lng / precisao]);
	}
	return out;
}

/** Quantos pontos a string carrega, sem alocar o array. */
function countPolyline(txt) {
	if (!txt) return 0;
	var s = String(txt);
	if (!s) return 0;
	var n = 1;
	for (var i = 0; i < s.length; i++) {
		if (s.charCodeAt(i) === 59 /* ; */) n++;
	}
	return n;
}

module.exports = {
	PRECISAO: PRECISAO,
	SEP_PONTO: SEP_PONTO,
	SEP_COORD: SEP_COORD,
	encodePolyline: encodePolyline,
	decodePolyline: decodePolyline,
	countPolyline: countPolyline
};
