'use strict';

/**
 * Chaves de nome para casar logradouro OSM ↔ DNE.
 *
 * Entrada esperada: string já passada por `name-norm.js` (minúscula, sem acento,
 * só [a-z0-9 ]). As funções aqui não normalizam de novo.
 *
 * Ver docs/geo/dne-geo-join.md §Fase 4.
 */

/** Tipos de logradouro que aparecem como prefixo do nome, nos dois lados. */
var TIPOS = {
	rua: 1, avenida: 1, travessa: 1, viela: 1, praca: 1, alameda: 1, estrada: 1,
	rodovia: 1, via: 1, passagem: 1, largo: 1, viaduto: 1, ponte: 1, acesso: 1,
	servidao: 1, beco: 1, marginal: 1, ladeira: 1, complexo: 1, anel: 1,
	corredor: 1, caminho: 1, trevo: 1, elevado: 1, galeria: 1, area: 1, trecho: 1,
	ciclovia: 1, calcada: 1, passarela: 1, terminal: 1, pedestre: 1, partilhada: 1,
	lateral: 1, vila: 1, parque: 1, jardim: 1, condominio: 1, loteamento: 1,
	residencial: 1, quadra: 1, conjunto: 1, boulevard: 1, retorno: 1, alca: 1
};

/** Conectores que sobram na frente depois de tirar o tipo ("da moenda"). */
var LIGA = { de: 1, da: 1, do: 1, das: 1, dos: 1, e: 1, a: 1, o: 1, em: 1, no: 1, na: 1 };

/** `TLO_TX` do DNE que designam área, não linha. */
var TLO_AREA = {
	praca: 1, largo: 1, parque: 1, jardim: 1, vila: 1, area: 1
};

/** `kind` do extract que é área. */
var KIND_AREA = { square: 1, park: 1 };

/**
 * Núcleo do nome: tira o tipo de logradouro e conectores da frente.
 * "travessa da moenda" → "moenda"; casa com "rua da moenda" do OSM.
 * Nunca devolve vazio — nome que é só tipo ("travessa") volta inteiro.
 */
function coreName(norm) {
	if (!norm) return '';
	var t = norm.split(' ');
	var i = 0;
	while (i < t.length - 1 && (TIPOS[t[i]] === 1 || LIGA[t[i]] === 1)) i++;
	return t.slice(i).join(' ');
}

/**
 * Chave fonética PT-BR. Colapsa as variações de grafia que o DNE e o OSM
 * divergem sistematicamente: Luiz/Luis, Sousa/Souza, Ayrton/Airton, Affonso/Afonso.
 *
 * Medido: 0,93 % de colisão entre 59 827 núcleos distintos, e as colisões são
 * pares que deveriam colapsar mesmo (docs/geo/melhoria-extracao-coordenadas.md §8.5).
 */
function phoneticKey(norm) {
	if (!norm) return '';
	var out = [];
	var words = norm.split(' ');
	for (var i = 0; i < words.length; i++) {
		var x = words[i];
		if (!x) continue;
		x = x.replace(/ph/g, 'f');
		x = x.replace(/ch/g, 'x');           // ch e x são o mesmo som: Xavier ≡ Chavier
		// `~` é sentinela: a entrada normalizada só tem [a-z0-9 ], então não colide
		x = x.replace(/([ln])h/g, '$1~');    // protege lh/nh do corte do h mudo
		x = x.replace(/h/g, '');
		x = x.replace(/~/g, 'h');
		x = x.replace(/y/g, 'i');
		x = x.replace(/w/g, 'v');
		x = x.replace(/qu?/g, 'k');
		x = x.replace(/c([ei])/g, 's$1');
		x = x.replace(/ss/g, 's');
		x = x.replace(/sc([ei])/g, 's$1');
		x = x.replace(/c/g, 'k');
		x = x.replace(/g([ei])/g, 'j$1');
		x = x.replace(/z/g, 's');
		x = x.replace(/m$/g, 'n');
		x = x.replace(/(.)\1+/g, '$1');
		out.push(x);
	}
	return out.join(' ');
}

function isAreaTlo(tloNorm) {
	return TLO_AREA[tloNorm] === 1;
}

function isAreaKind(kind) {
	return KIND_AREA[kind] === 1;
}

module.exports = {
	coreName: coreName,
	phoneticKey: phoneticKey,
	isAreaTlo: isAreaTlo,
	isAreaKind: isAreaKind,
	TIPOS: TIPOS,
	TLO_AREA: TLO_AREA,
	KIND_AREA: KIND_AREA
};
