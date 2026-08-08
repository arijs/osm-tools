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

/**
 * Qualificadores do tipo composto no DNE (TLO_TX multi-palavra).
 * Só saem depois de um TIPOS — "estrada municipal X" → "X";
 * "municipal X" sozinho (sem tipo antes) permanece (nome, não tipo).
 * Ex.: TLO "Estrada Municipal" + LOG_NO "Professora …".
 */
var TIPO_MOD = {
	municipal: 1, estadual: 1, federal: 1, vicinal: 1
};

/** Conectores que sobram na frente depois de tirar o tipo ("da moenda"). */
var LIGA = { de: 1, da: 1, do: 1, das: 1, dos: 1, e: 1, a: 1, o: 1, em: 1, no: 1, na: 1 };

/**
 * Conectores no *meio* do núcleo — DNE e OSM divergem em inserir/omitir.
 * Ex.: "arlindo moraes costa" ↔ "arlindo moraes da costa".
 * Subconjunto de LIGA (sem artigos a/o e sem em/no/na — iniciais e ruído).
 */
var MID_LIGA = { de: 1, da: 1, do: 1, das: 1, dos: 1, e: 1 };

/**
 * Títulos / honrarias no início do núcleo (depois do tipo).
 * DNE costuma omitir; OSM grava por extenso ou abreviado.
 * Entrada já normalizada (sem acento). Não inclui santo/são — colidem com topônimos.
 */
var TITULOS = {
	doutor: 1, doutora: 1, dr: 1, dra: 1,
	professor: 1, professora: 1, prof: 1,
	engenheiro: 1, engenheira: 1, eng: 1,
	desembargador: 1, desembargadora: 1,
	senador: 1, senadora: 1,
	deputado: 1, deputada: 1,
	ministro: 1, ministra: 1,
	presidente: 1,
	padre: 1, frei: 1, monsenhor: 1,
	marechal: 1, general: 1, brigadeiro: 1,
	coronel: 1, major: 1, capitao: 1, tenente: 1, almirante: 1,
	comendador: 1, comendadora: 1,
	vereador: 1, vereadora: 1,
	prefeito: 1, prefeita: 1,
	governador: 1, governadora: 1
	// nobreza (barão, visconde, …) fica de fora: é parte do nome, não honraria
	// opcional — Barão de Piracicaba ≠ de Piracicaba
};

/** `TLO_TX` do DNE que designam área, não linha. */
var TLO_AREA = {
	praca: 1, largo: 1, parque: 1, jardim: 1, vila: 1, area: 1
};

/** `kind` do extract que é área. */
var KIND_AREA = { square: 1, park: 1 };

/**
 * Núcleo do nome: tira o tipo de logradouro, qualificadores do tipo e conectores
 * da frente. "travessa da moenda" → "moenda"; "estrada municipal X" → "X".
 * Nunca devolve vazio — nome que é só tipo ("travessa") volta inteiro.
 */
function coreName(norm) {
	if (!norm) return '';
	var t = norm.split(' ');
	var i = 0;
	var sawTipo = false;
	while (i < t.length - 1) {
		if (TIPOS[t[i]] === 1) {
			sawTipo = true;
			i++;
			continue;
		}
		if (LIGA[t[i]] === 1) {
			i++;
			continue;
		}
		// "municipal"/"estadual"/… só após um tipo real (TLO composto)
		if (sawTipo && TIPO_MOD[t[i]] === 1) {
			i++;
			continue;
		}
		break;
	}
	return t.slice(i).join(' ');
}

/**
 * Remove títulos do início do núcleo.
 * "doutor olimpio carr ribeiro" → "olimpio carr ribeiro".
 * Nunca devolve vazio — nome que é só título volta inteiro.
 * Devolve também os tokens removidos (auditoria).
 */
function stripTitulos(norm) {
	if (!norm) return { bare: '', removed: [] };
	var t = norm.split(' ');
	var i = 0;
	var removed = [];
	while (i < t.length - 1 && TITULOS[t[i]] === 1) {
		removed.push(t[i]);
		i++;
	}
	return { bare: t.slice(i).join(' '), removed: removed };
}

/** Núcleo sem tipo e sem títulos de honraria. */
function coreBare(norm) {
	return stripTitulos(coreName(norm)).bare;
}

/**
 * Remove conectores MID_LIGA em qualquer posição exceto a primeira palavra.
 * "arlindo moraes da costa" → "arlindo moraes costa".
 * Não devolve vazio.
 */
function stripMidLiga(norm) {
	if (!norm) return '';
	var t = norm.split(' ').filter(Boolean);
	if (t.length <= 1) return norm;
	var out = [t[0]];
	for (var i = 1; i < t.length; i++) {
		if (MID_LIGA[t[i]] === 1) continue;
		out.push(t[i]);
	}
	return out.length ? out.join(' ') : norm;
}

/** Núcleo sem tipo, sem título e sem conectores no meio. */
function midBare(norm) {
	return stripMidLiga(coreBare(norm));
}

/**
 * Distância de Levenshtein com early-exit se > maxDist.
 * maxDist omitido → distância completa.
 */
function levenshtein(a, b, maxDist) {
	if (a === b) return 0;
	var la = a.length;
	var lb = b.length;
	if (maxDist !== undefined && Math.abs(la - lb) > maxDist) return maxDist + 1;
	if (!la) return lb;
	if (!lb) return la;
	var prev = new Array(lb + 1);
	var cur = new Array(lb + 1);
	var j, i;
	for (j = 0; j <= lb; j++) prev[j] = j;
	for (i = 1; i <= la; i++) {
		cur[0] = i;
		var rowMin = i;
		for (j = 1; j <= lb; j++) {
			var cost = a.charCodeAt(i - 1) === b.charCodeAt(j - 1) ? 0 : 1;
			cur[j] = Math.min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + cost);
			if (cur[j] < rowMin) rowMin = cur[j];
		}
		if (maxDist !== undefined && rowMin > maxDist) return maxDist + 1;
		var tmp = prev;
		prev = cur;
		cur = tmp;
	}
	return prev[lb];
}

/** A própria string + deleções de 1 caractere (índice SymSpell K=1). */
function deletes1(s) {
	var out = [s];
	for (var i = 0; i < s.length; i++) out.push(s.slice(0, i) + s.slice(i + 1));
	return out;
}

/**
 * Distância máxima de fuzzy por comprimento do núcleo.
 * Só dist=1 e só a partir de 10 chars — medido: dist=2 ainda gera
 * Germani↔Germano / Luiz↔Luiza. Ver changelog 2026-08-07.
 */
function fuzzyMaxDist(len) {
	return len >= 10 ? 1 : 0;
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
	stripTitulos: stripTitulos,
	coreBare: coreBare,
	stripMidLiga: stripMidLiga,
	midBare: midBare,
	levenshtein: levenshtein,
	deletes1: deletes1,
	fuzzyMaxDist: fuzzyMaxDist,
	phoneticKey: phoneticKey,
	isAreaTlo: isAreaTlo,
	isAreaKind: isAreaKind,
	TIPOS: TIPOS,
	TIPO_MOD: TIPO_MOD,
	MID_LIGA: MID_LIGA,
	TITULOS: TITULOS,
	TLO_AREA: TLO_AREA,
	KIND_AREA: KIND_AREA
};
