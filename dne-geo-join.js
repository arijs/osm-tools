'use strict';

/**
 * Junta o DNE (LOG_*.TXT, latin1) com o extract OSM (OSM_*.TXT ou shards, utf8)
 * e emite DNE_GEO_LOGRADOURO_{UF}.TXT com geometria por logradouro.
 *
 * OSM aceita arquivo flat `OSM_LOGRADOURO_{UF}.TXT` **ou** pasta de shards
 * `OSM_LOGRADOURO_{UF}/{N}-linhas/000001.txt` (+ MANIFEST.json). Prefere shards
 * se a pasta existir. Idem para `OSM_ADDR_POINT_{UF}`.
 *
 * Especificação: docs/geo/dne-geo-join.md
 *
 * CLI:
 *   node dne-geo-join.js --dne=DIR --osm=DIR --out=DIR --uf=SP
 *     --cluster-cell=0.02   célula da clusterização (graus)
 *     --footprint-cell=0.01 célula do footprint municipal (graus)
 *     --max-extent-km=15    acima disso, com mais de um candidato, vira `ambiguo`
 *     --footprint-dilate=1  células de halo na pegada municipal
 *     --envelope-tol-km=1   recupera fora_do_footprint com 1 candidato a ≤N km da mancha
 *     --sem-envelope        desliga a recuperação por envelope
 *     --vizinho-cep5-tol-km=1  recupera fora_do_footprint por vizinhança CEP-5 (km)
 *     --vizinho-cep5-min=3    mínimo de vias ok no mesmo CEP-5 (ou bairro)
 *     --sem-vizinho-cep5    desliga a recuperação por vizinhança CEP-5
 *     --sem-exclusao-cluster desliga a exclusividade de cluster entre municípios
 *     --quiet
 */

var fs = require('fs');
var path = require('path');
var readline = require('readline');
var nameNorm = require('./name-norm').nameNorm;
var keys = require('./name-keys');
var geo = require('./geo-cluster');
var txtAt = require('./txt-at-writer');

var REGRAS = [
	'exato', 'area', 'name_alt', 'addr', 'nucleo', 'fonetico',
	'titulo', 'titulo_fonetico', 'vizinho_cep5'
];

// A "âncora local" antiga (centroide + raio = max dist) foi medida e removida:
// rendia ~36 linhas e o raio explodia com outliers. A fase 5e (vizinho CEP-5)
// é outra coisa: distância ao **vizinho mais próximo** já ok no mesmo CEP-5
// (fallback: bairro), só no resíduo `fora_do_footprint`, candidato único.
// Ver docs/geo/amostras-ambiguo-sp.md e docs/geo/dne-geo-join.md §Fase 5e.

function digitsCep5(cep) {
	var d = String(cep || '').replace(/\D/g, '');
	return d.length >= 5 ? d.slice(0, 5) : '';
}

/** Distância ao ponto mais próximo (km). Infinity se lista vazia. */
function nearestDistKm(lat, lng, pts) {
	var best = Infinity;
	for (var i = 0; i < pts.length; i++) {
		var d = geo.distKm(lat, lng, pts[i].lat, pts[i].lng);
		if (d < best) best = d;
	}
	return best;
}

function readLines(file, encoding, onLine) {
	return new Promise(function (resolve, reject) {
		var rl = readline.createInterface({
			input: fs.createReadStream(file, encoding ? { encoding: encoding } : undefined),
			crlfDelay: Infinity
		});
		rl.on('line', function (l) { if (l) onLine(l); });
		rl.on('close', resolve);
		rl.on('error', reject);
	});
}

/** Lê um ou mais arquivos em ordem (flat ou shards). */
async function readLinesMany(files, encoding, onLine) {
	for (var i = 0; i < files.length; i++) {
		await readLines(files[i], encoding, onLine);
	}
}

function num(v) {
	if (v === undefined || v === '') return null;
	var f = Number(v);
	return isFinite(f) ? f : null;
}

// ---------------------------------------------------------------- DNE (latin1)

async function loadLocalidades(dneDir) {
	var byId = new Map();
	await readLines(path.join(dneDir, 'LOG_LOCALIDADE.TXT'), 'latin1', function (l) {
		var p = l.split('@');
		byId.set(p[0], {
			loc_nu: p[0], uf: p[1], nome: p[2], sit: p[4], tipo: p[5],
			sub: p[6] || '', mun: p[8] || ''
		});
	});
	// distrito herda o IBGE do município de subordinação
	byId.forEach(function (l) {
		if (!l.mun && l.sub) {
			var pai = byId.get(l.sub);
			if (pai && pai.mun) l.mun = pai.mun;
		}
	});
	return byId;
}

async function loadBairros(dneDir, uf) {
	var byId = new Map();
	await readLines(path.join(dneDir, 'LOG_BAIRRO.TXT'), 'latin1', function (l) {
		var p = l.split('@');
		if (p[1] !== uf) return;
		byId.set(p[0], { bai_nu: p[0], uf: p[1], loc_nu: p[2], nome: p[3] });
	});
	return byId;
}

async function loadLogradouros(dneDir, uf) {
	var rows = [];
	await readLines(path.join(dneDir, 'LOG_LOGRADOURO_' + uf + '.TXT'), 'latin1', function (l) {
		var p = l.split('@');
		var tlo = p[8] || '';
		var semTipo = nameNorm(p[5]);
		// LOG_STA_TLO = 'N' → o tipo não faz parte do nome usado no endereço
		var comTipo = p[9] === 'N' ? semTipo : nameNorm(tlo + ' ' + p[5]);
		rows.push({
			log_nu: p[0], uf: p[1], loc_nu: p[2], bai_ini: p[3], bai_fim: p[4] || '',
			log_no: p[5], compl: p[6] || '', cep: p[7], tlo: tlo, sta: p[9] || '',
			abrev: p[10] || '',
			comTipo: comTipo, semTipo: semTipo,
			tloNorm: nameNorm(tlo),
			ehArea: keys.isAreaTlo(nameNorm(tlo)),
			cluster: null, regra: '', status: 'sem_nome_osm', nCand: 0
		});
	});
	return rows;
}

// ---------------------------------------------------------------- OSM (utf8)

/**
 * Resolve paths de OSM_LOGRADOURO_{UF} (shard ou flat).
 * @returns {{ mode: string, paths: string[], label: string }}
 */
function resolveOsmLogradouro(osmDir, uf) {
	var resolved = txtAt.resolveDatasetPaths(osmDir, 'OSM_LOGRADOURO_' + uf);
	var label =
		resolved.mode === 'shard'
			? 'OSM_LOGRADOURO_' + uf + '/ (' + resolved.paths.length + ' shards)'
			: resolved.mode === 'flat'
				? path.basename(resolved.paths[0])
				: 'OSM_LOGRADOURO_' + uf + '.TXT';
	return { mode: resolved.mode, paths: resolved.paths, label: label, resolved: resolved };
}

/** Lê OSM_LOGRADOURO_{UF} (flat ou shards) agrupando por name_norm (e name_alt_norm). */
async function loadOsmStreets(osmDir, uf, quiet) {
	var byName = new Map();     // name_norm -> [feat]
	var altOf = new Map();      // name_alt_norm -> Set(name_norm)
	var lines = 0, semGeom = 0;
	var res = resolveOsmLogradouro(osmDir, uf);
	if (!res.paths.length) {
		return { byName: byName, altOf: altOf, lines: 0, semGeom: 0, mode: 'missing', files: 0 };
	}
	if (!quiet && res.mode === 'shard') {
		process.stderr.write('  lendo ' + res.label + '\n');
	}
	await readLinesMany(res.paths, null, function (l) {
		var p = l.split('@');
		lines++;
		var lat = num(p[10]), lng = num(p[11]);
		if (lat === null || lng === null) { semGeom++; return; }
		var nn = p[2];
		if (!nn) return;
		var feat = {
			// `id` = osm_id da way. Não participa de cluster nem de casamento —
			// existe só para o cluster VENCEDOR poder dizer, na saída, de quais
			// ways ele é feito. É o que deixa o consumidor buscar o traçado por
			// id exato, em vez de recasar nome na hora de desenhar.
			id: p[0] || '',
			lat: lat, lng: lng,
			latMin: num(p[12]) === null ? lat : num(p[12]),
			latMax: num(p[13]) === null ? lat : num(p[13]),
			lngMin: num(p[14]) === null ? lng : num(p[14]),
			lngMax: num(p[15]) === null ? lng : num(p[15]),
			n: num(p[16]) || 1,
			kind: p[3] || ''
		};
		var arr = byName.get(nn);
		if (arr) arr.push(feat);
		else byName.set(nn, [feat]);

		var alt = p[18];
		if (alt) {
			var parts = alt.split(';');
			for (var i = 0; i < parts.length; i++) {
				var a = parts[i];
				if (!a || a === nn) continue;
				var s = altOf.get(a);
				if (s) s.add(nn);
				else altOf.set(a, new Set([nn]));
			}
		}
		if (!quiet && lines % 200000 === 0) {
			process.stderr.write('\r  osm ' + lines + ' linhas, ' + byName.size + ' nomes');
		}
	});
	if (!quiet) process.stderr.write('\r  osm ' + lines + ' linhas, ' + byName.size + ' nomes\n');
	return {
		byName: byName, altOf: altOf, lines: lines, semGeom: semGeom,
		mode: res.mode, files: res.paths.length
	};
}

/** Pontos addr:street (flat ou shards) — peso 1, só se não houver way com o mesmo name_norm. */
async function loadAddrPoints(osmDir, uf, byName) {
	var resolved = txtAt.resolveDatasetPaths(osmDir, 'OSM_ADDR_POINT_' + uf);
	if (!resolved.paths.length) return 0;
	var add = 0;
	await readLinesMany(resolved.paths, null, function (l) {
		var p = l.split('@');
		var lat = num(p[1]), lng = num(p[2]);
		var nn = p[4];
		if (lat === null || lng === null || !nn) return;
		if (byName.has(nn)) return; // way nomeada ganha do ponto de numeração
		var feat = {
			lat: lat, lng: lng, latMin: lat, latMax: lat, lngMin: lng, lngMax: lng,
			n: 1, kind: 'addr'
		};
		var arr = byName.get(nn);
		if (arr) arr.push(feat);
		else byName.set(nn, [feat]);
		add++;
	});
	return add;
}

// ---------------------------------------------------------------- clusters

function buildClusters(byName, clusterCell) {
	var clusters = [];          // {name, feats, agg, hasVia, hasArea}
	var byNameIdx = new Map();  // name_norm -> [idx]
	byName.forEach(function (feats, nn) {
		var groups = geo.clusterFeatures(feats, clusterCell);
		var idxs = [];
		for (var i = 0; i < groups.length; i++) {
			var agg = geo.aggregate(groups[i]);
			var hasArea = false, hasVia = false;
			for (var k = 0; k < agg.kinds.length; k++) {
				if (keys.isAreaKind(agg.kinds[k])) hasArea = true;
				else hasVia = true;
			}
			idxs.push(clusters.length);
			clusters.push({
				name: nn, feats: groups[i], agg: agg,
				hasVia: hasVia, hasArea: hasArea
			});
		}
		byNameIdx.set(nn, idxs);
	});
	return { clusters: clusters, byName: byNameIdx };
}

/**
 * Índices derivados sobre os nomes de cluster:
 * núcleo sem tipo, fonético, núcleo sem título, fonético sem título.
 */
function buildDerivedIndexes(byNameIdx) {
	var byCore = new Map();
	var byPhon = new Map();
	var byTitle = new Map();
	var byTitlePhon = new Map();
	function push(map, key, idxs) {
		if (!key) return;
		var a = map.get(key);
		if (a) { for (var i = 0; i < idxs.length; i++) a.push(idxs[i]); }
		else map.set(key, idxs.slice());
	}
	byNameIdx.forEach(function (idxs, nn) {
		var c = keys.coreName(nn);
		if (!c) return;
		push(byCore, c, idxs);
		push(byPhon, keys.phoneticKey(c), idxs);
		var bare = keys.stripTitulos(c).bare;
		push(byTitle, bare, idxs);
		push(byTitlePhon, keys.phoneticKey(bare), idxs);
	});
	return {
		byCore: byCore, byPhon: byPhon,
		byTitle: byTitle, byTitlePhon: byTitlePhon
	};
}

// ---------------------------------------------------------------- cascata

/**
 * Candidatos de um degrau da cascata, já filtrados pela guarda kind-aware.
 * Área (`square`/`park`) só serve para TLO_TX de área.
 */
function candidatesFor(row, idxs, clusters) {
	if (!idxs || !idxs.length) return [];
	var out = [];
	for (var i = 0; i < idxs.length; i++) {
		var c = clusters[idxs[i]];
		if (!row.ehArea && !c.hasVia) continue;  // Rua não casa com praça/parque puro
		out.push(idxs[i]);
	}
	return out;
}

function cascadeCandidates(row, idx, clusters) {
	var core = keys.coreName(row.comTipo);
	var bare = keys.stripTitulos(core).bare;
	var tries = [
		['exato', idx.byName.get(row.comTipo)],
		['exato', idx.byName.get(row.semTipo)],
		['name_alt', idx.altIdx.get(row.comTipo)],
		['name_alt', idx.altIdx.get(row.semTipo)],
		['nucleo', idx.byCore.get(core)],
		['fonetico', idx.byPhon.get(keys.phoneticKey(core))],
		// DNE sem título ↔ OSM com Doutor/Prof/… (ou o inverso)
		['titulo', idx.byTitle.get(bare)],
		['titulo_fonetico', idx.byTitlePhon.get(keys.phoneticKey(bare))]
	];
	for (var i = 0; i < tries.length; i++) {
		var cand = candidatesFor(row, tries[i][1], clusters);
		if (cand.length) return { regra: tries[i][0], cand: cand };
	}
	return { regra: '', cand: [] };
}

function main(opts) {
	return run(opts);
}

async function run(opts) {
	var uf = String(opts.uf || '').toUpperCase();
	var quiet = !!opts.quiet;
	var log = quiet ? function () {} : function (m) { process.stderr.write(m + '\n'); };
	var clusterCell = opts.clusterCell || geo.CELL_CLUSTER;
	var footprintCell = opts.footprintCell || geo.CELL_FOOTPRINT;
	var maxExtentKm = opts.maxExtentKm || 15;
	var dilate = opts.footprintDilate == null ? 1 : opts.footprintDilate;
	var envelopeTolKm = opts.envelopeTolKm == null ? 1 : opts.envelopeTolKm;
	var useEnvelope = opts.semEnvelope ? false : true;
	var useExclusao = opts.semExclusaoCluster ? false : true;
	var useVizinhoCep5 = opts.semVizinhoCep5 ? false : true;
	var vizinhoCep5TolKm = opts.vizinhoCep5TolKm == null ? 1 : opts.vizinhoCep5TolKm;
	var vizinhoCep5Min = opts.vizinhoCep5Min == null ? 3 : opts.vizinhoCep5Min;

	log('DNE  : ' + opts.dneDir);
	log('OSM  : ' + opts.osmDir);
	log('UF   : ' + uf);

	var osmRes = resolveOsmLogradouro(opts.osmDir, uf);
	var semExtract = osmRes.mode === 'missing';

	log('[1/6] DNE…');
	var localidades = await loadLocalidades(opts.dneDir);
	var bairros = await loadBairros(opts.dneDir, uf);
	var rows = await loadLogradouros(opts.dneDir, uf);
	log('      localidades=' + localidades.size + ' bairros=' + bairros.size + ' logradouros=' + rows.length);

	var osm = { byName: new Map(), altOf: new Map(), lines: 0, semGeom: 0, mode: 'missing', files: 0 };
	var addrAdded = 0;
	if (!semExtract) {
		log('[2/6] OSM…');
		osm = await loadOsmStreets(opts.osmDir, uf, quiet);
		addrAdded = await loadAddrPoints(opts.osmDir, uf, osm.byName);
		log('      modo=' + osm.mode + ' arquivos=' + osm.files +
			' addr:street sem way homônima: ' + addrAdded);
	} else {
		log('[2/6] OSM ausente (' + osmRes.label + ') — tudo sai sem_extract');
	}

	log('[3/6] clusters…');
	var built = buildClusters(osm.byName, clusterCell);
	var clusters = built.clusters;
	for (var ci = 0; ci < clusters.length; ci++) clusters[ci].id = ci;
	var derived = buildDerivedIndexes(built.byName);
	// name_alt → índices dos clusters do nome principal
	var altIdx = new Map();
	osm.altOf.forEach(function (nomes, alt) {
		var acc = [];
		nomes.forEach(function (nn) {
			var ix = built.byName.get(nn);
			if (ix) for (var i = 0; i < ix.length; i++) acc.push(ix[i]);
		});
		if (acc.length) altIdx.set(alt, acc);
	});
	var idx = {
		byName: built.byName, altIdx: altIdx,
		byCore: derived.byCore, byPhon: derived.byPhon,
		byTitle: derived.byTitle, byTitlePhon: derived.byTitlePhon
	};
	log('      clusters=' + clusters.length + ' nomes=' + built.byName.size +
		' nucleos=' + derived.byCore.size + ' foneticos=' + derived.byPhon.size +
		' titulos=' + derived.byTitle.size);

	// ---- Fase 2: âncoras (nome com 1 loc_nu no DNE e 1 cluster no OSM)
	log('[4/6] âncoras e footprint…');
	var locsDoNome = new Map();
	for (var i = 0; i < rows.length; i++) {
		var r = rows[i];
		var s = locsDoNome.get(r.comTipo);
		if (s) s.add(r.loc_nu);
		else locsDoNome.set(r.comTipo, new Set([r.loc_nu]));
	}
	var pontosPorLoc = new Map();
	var ancoras = 0;
	locsDoNome.forEach(function (locs, nome) {
		if (locs.size !== 1) return;
		var ix = built.byName.get(nome);
		if (!ix || ix.length !== 1) return;
		ancoras++;
		var loc = locs.values().next().value;
		var arr = pontosPorLoc.get(loc);
		if (!arr) pontosPorLoc.set(loc, arr = []);
		var feats = clusters[ix[0]].feats;
		for (var j = 0; j < feats.length; j++) arr.push(feats[j]);
	});

	var footprints = new Map();
	var centroLoc = new Map();   // diagnóstico: centro e raio das âncoras do município
	pontosPorLoc.forEach(function (pts, loc) {
		footprints.set(loc, geo.buildFootprint(pts, footprintCell, dilate));
		var sLat = 0, sLng = 0;
		for (var i = 0; i < pts.length; i++) { sLat += pts[i].lat; sLng += pts[i].lng; }
		var cLat = sLat / pts.length, cLng = sLng / pts.length;
		var raio = 0;
		for (var j = 0; j < pts.length; j++) {
			var d = geo.distKm(cLat, cLng, pts[j].lat, pts[j].lng);
			if (d > raio) raio = d;
		}
		centroLoc.set(loc, { lat: cLat, lng: cLng, raio: raio });
	});
	// distrito sem âncora própria usa a pegada do município de subordinação
	var herdados = 0;
	localidades.forEach(function (l) {
		if (footprints.has(l.loc_nu) || !l.sub) return;
		var pai = footprints.get(l.sub);
		if (pai) { footprints.set(l.loc_nu, pai); herdados++; }
	});
	log('      ancoras=' + ancoras + ' footprints=' + footprints.size + ' (herdados=' + herdados + ')');

	// ---- Fase 4/5: cascata + desempate, em duas voltas
	log('[5/6] casando…');
	var stats = { rodadas: [] };
	var porBairro = new Map();  // bai_nu -> {sLat,sLng,n}

	function resolver(volta) {
		var ok = 0, amb = 0, sem = 0;
		for (var i = 0; i < rows.length; i++) {
			var r = rows[i];
			if (r.status === 'ok') continue;
			if (semExtract) { r.status = 'sem_extract'; continue; }

			var got = cascadeCandidates(r, idx, clusters);
			if (!got.cand.length) { r.status = 'sem_nome_osm'; r.motivo = ''; sem++; continue; }

			var fp = footprints.get(r.loc_nu);
			var dentro = [];
			for (var c = 0; c < got.cand.length; c++) {
				if (geo.footprintOverlap(fp, clusters[got.cand[c]].feats) >= 0.5) dentro.push(got.cand[c]);
			}
			// sem footprint (município ralo): candidato único ainda vale
			if (!dentro.length && !fp && got.cand.length === 1) dentro = got.cand;
			if (!dentro.length) {
				r.status = 'ambiguo';
				r.motivo = fp ? 'fora_do_footprint' : 'sem_footprint';
				r.nCand = got.cand.length;
				// Quão longe o melhor candidato está da mancha do município? Perto =
				// buraco na pegada (recuperável); longe = rua de outra cidade.
				var cen = centroLoc.get(r.loc_nu);
				if (cen) {
					var perto = Infinity;
					for (var cc = 0; cc < got.cand.length; cc++) {
						var ag2 = clusters[got.cand[cc]].agg;
						var dd = geo.distKm(cen.lat, cen.lng, ag2.lat, ag2.lng) - cen.raio;
						if (dd < perto) perto = dd;
					}
					r.distKm = Math.max(0, perto);
				}
				amb++;
				continue;
			}

			r.nCand = dentro.length;
			var escolhido = dentro[0];
			if (dentro.length > 1) {
				var b = volta > 1 ? porBairro.get(r.bai_ini) : null;
				if (b && b.n > 0) {
					var bLat = b.sLat / b.n, bLng = b.sLng / b.n;
					var melhor = Infinity;
					for (var d = 0; d < dentro.length; d++) {
						var a = clusters[dentro[d]].agg;
						var dist = geo.distKm(bLat, bLng, a.lat, a.lng);
						if (dist < melhor) { melhor = dist; escolhido = dentro[d]; }
					}
				} else {
					var maiorPeso = -1, empate = false;
					for (var e = 0; e < dentro.length; e++) {
						var w = clusters[dentro[e]].agg.weight;
						if (w > maiorPeso) { maiorPeso = w; escolhido = dentro[e]; empate = false; }
						else if (w === maiorPeso) empate = true;
					}
					if (empate) { r.status = 'ambiguo'; r.motivo = 'empate_de_tamanho'; amb++; continue; }
				}
			}
			var alvo = clusters[escolhido];

			// Encadeamento do single-link: nome genérico ("Rua Dois") espalhado pela
			// cidade forma uma corrente de células que liga pontas a dezenas de km.
			// Medido em Guarulhos: 29 ways, 25 km, 19 clusters concorrentes.
			// Via longa legítima (rodovia) aparece como cluster ÚNICO — daí a 2ª condição.
			var ag = alvo.agg;
			var extKm = Math.max(
				(ag.latMax - ag.latMin) * 111,
				(ag.lngMax - ag.lngMin) * 102
			);
			if (extKm > maxExtentKm && dentro.length > 1) {
				r.status = 'ambiguo';
				r.motivo = 'extensao_' + Math.round(extKm) + 'km';
				amb++;
				continue;
			}

			r.cluster = alvo;
			r.motivo = '';
			var soAddr = ag.kinds.length === 1 && ag.kinds[0] === 'addr';
			r.regra = soAddr
				? 'addr'
				: (got.regra === 'exato' && r.ehArea && !alvo.hasVia ? 'area' : got.regra);
			r.status = 'ok';
			ok++;

			var bb = porBairro.get(r.bai_ini);
			if (!bb) porBairro.set(r.bai_ini, bb = { sLat: 0, sLng: 0, n: 0 });
			bb.sLat += r.cluster.agg.lat;
			bb.sLng += r.cluster.agg.lng;
			bb.n++;
		}
		stats.rodadas.push({ volta: volta, ok: ok, ambiguo: amb, sem_nome_osm: sem });
		return ok;
	}

	resolver(1);
	// 2ª volta: footprint cresceu com o que casou, e agora há centroide de bairro
	if (!semExtract) {
		var extra = new Map();
		for (var q = 0; q < rows.length; q++) {
			var rr = rows[q];
			if (rr.status !== 'ok' || !rr.cluster) continue;
			var arr2 = extra.get(rr.loc_nu);
			if (!arr2) extra.set(rr.loc_nu, arr2 = []);
			for (var z = 0; z < rr.cluster.feats.length; z++) arr2.push(rr.cluster.feats[z]);
		}
		extra.forEach(function (pts, loc) {
			var base = pontosPorLoc.get(loc) || [];
			footprints.set(loc, geo.buildFootprint(base.concat(pts), footprintCell, dilate));
		});
		for (var w2 = 0; w2 < rows.length; w2++) {
			if (rows[w2].status !== 'ok') rows[w2].status = 'pendente';
		}
		resolver(2);
		for (var w3 = 0; w3 < rows.length; w3++) {
			if (rows[w3].status === 'pendente') rows[w3].status = 'sem_nome_osm';
		}

		// ---- Fase 5c: buraco na pegada — candidato único perto da mancha
		// Não dilata o footprint (halo metropolitano mistura cidades vizinhas).
		// Entre os candidatos de nome, conta quantos caem a ≤ tol km da mancha
		// (centro−raio das âncoras). Se sobra exatamente 1 e a extensão é ok → aceita.
		var envelopeOk = 0;
		if (useEnvelope && envelopeTolKm > 0) {
			for (var e5 = 0; e5 < rows.length; e5++) {
				var re = rows[e5];
				if (re.status !== 'ambiguo' || re.motivo !== 'fora_do_footprint') continue;
				var cenE = centroLoc.get(re.loc_nu);
				if (!cenE) continue;
				var gotE = cascadeCandidates(re, idx, clusters);
				if (!gotE.cand.length) continue;
				var pertoE = [];
				for (var pe = 0; pe < gotE.cand.length; pe++) {
					var agP = clusters[gotE.cand[pe]].agg;
					var dP = Math.max(0, geo.distKm(cenE.lat, cenE.lng, agP.lat, agP.lng) - cenE.raio);
					if (dP <= envelopeTolKm) pertoE.push(gotE.cand[pe]);
				}
				if (pertoE.length !== 1) continue;
				var alvoE = clusters[pertoE[0]];
				var agE = alvoE.agg;
				var extE = Math.max(
					(agE.latMax - agE.latMin) * 111,
					(agE.lngMax - agE.lngMin) * 102
				);
				if (extE > maxExtentKm) continue;
				re.cluster = alvoE;
				re.motivo = '';
				re.regra = gotE.regra === 'exato' && re.ehArea && !alvoE.hasVia
					? 'area' : (gotE.regra || 'exato');
				re.status = 'ok';
				re.nCand = 1;
				re.via_envelope = true;
				envelopeOk++;
				var bbE = porBairro.get(re.bai_ini);
				if (!bbE) porBairro.set(re.bai_ini, bbE = { sLat: 0, sLng: 0, n: 0 });
				bbE.sLat += agE.lat;
				bbE.sLng += agE.lng;
				bbE.n++;
			}
			if (envelopeOk) log('      envelope: recuperou ' + envelopeOk + ' (tol=' + envelopeTolKm + ' km)');
		}
		stats.envelope_recuperados = envelopeOk;

		// ---- Fase 5e: vizinhança CEP-5 (ou bairro) no resíduo fora_do_footprint
		// Distância ao vizinho mais próximo já ok — nunca raio de centroide.
		// Aceita só se sobrar exatamente 1 candidato de nome a ≤ tol km de ≥min âncoras.
		var vizinhoOk = 0;
		var exemplosVizinho = [];
		if (useVizinhoCep5 && vizinhoCep5TolKm > 0 && vizinhoCep5Min > 0) {
			var byCep5Loc = new Map(); // loc_nu|cep5 -> [{lat,lng,log_nu,log_no}]
			var byBaiNu = new Map();   // bai_nu -> idem
			for (var vi = 0; vi < rows.length; vi++) {
				var rv = rows[vi];
				if (rv.status !== 'ok' || !rv.cluster) continue;
				var pt = {
					lat: rv.cluster.agg.lat,
					lng: rv.cluster.agg.lng,
					log_nu: rv.log_nu,
					log_no: rv.log_no
				};
				var c5 = digitsCep5(rv.cep);
				if (c5) {
					var ck = rv.loc_nu + '|' + c5;
					var ca = byCep5Loc.get(ck);
					if (ca) ca.push(pt); else byCep5Loc.set(ck, [pt]);
				}
				if (rv.bai_ini) {
					var ba = byBaiNu.get(rv.bai_ini);
					if (ba) ba.push(pt); else byBaiNu.set(rv.bai_ini, [pt]);
				}
			}
			for (var v5 = 0; v5 < rows.length; v5++) {
				var rV = rows[v5];
				if (rV.status !== 'ambiguo' || rV.motivo !== 'fora_do_footprint') continue;
				var gotV = cascadeCandidates(rV, idx, clusters);
				if (!gotV.cand.length) continue;
				var fonte = 'cep5';
				var anchors = null;
				var c5r = digitsCep5(rV.cep);
				if (c5r) anchors = byCep5Loc.get(rV.loc_nu + '|' + c5r) || null;
				if (!anchors || anchors.length < vizinhoCep5Min) {
					anchors = rV.bai_ini ? (byBaiNu.get(rV.bai_ini) || null) : null;
					fonte = 'bairro';
				}
				if (!anchors || anchors.length < vizinhoCep5Min) continue;
				var pertoV = [];
				for (var pv = 0; pv < gotV.cand.length; pv++) {
					var agV = clusters[gotV.cand[pv]].agg;
					var dV = nearestDistKm(agV.lat, agV.lng, anchors);
					if (dV <= vizinhoCep5TolKm) {
						pertoV.push({ idx: gotV.cand[pv], dist: dV });
					}
				}
				if (pertoV.length !== 1) continue;
				var alvoV = clusters[pertoV[0].idx];
				var agVV = alvoV.agg;
				var extV = Math.max(
					(agVV.latMax - agVV.latMin) * 111,
					(agVV.lngMax - agVV.lngMin) * 102
				);
				if (extV > maxExtentKm) continue;
				// top-3 vizinhas para auditoria
				var vizSorted = anchors.slice().sort(function (a, b) {
					return geo.distKm(agVV.lat, agVV.lng, a.lat, a.lng) -
						geo.distKm(agVV.lat, agVV.lng, b.lat, b.lng);
				});
				var vizTop = [];
				for (var vt = 0; vt < vizSorted.length && vt < 3; vt++) {
					vizTop.push({
						log_nu: vizSorted[vt].log_nu,
						log_no: vizSorted[vt].log_no,
						dist_km: Math.round(
							geo.distKm(agVV.lat, agVV.lng, vizSorted[vt].lat, vizSorted[vt].lng) * 1000
						) / 1000
					});
				}
				rV.cluster = alvoV;
				rV.motivo = '';
				rV.regra = 'vizinho_cep5';
				rV.status = 'ok';
				rV.nCand = gotV.cand.length;
				rV.via_vizinho_cep5 = true;
				rV.nome_regra = gotV.regra;
				rV.vizinho_fonte = fonte;
				rV.vizinho_n = anchors.length;
				rV.vizinho_dist = pertoV[0].dist;
				vizinhoOk++;
				if (exemplosVizinho.length < 30) {
					exemplosVizinho.push({
						log_nu: rV.log_nu,
						dne_nome: (rV.tlo + ' ' + rV.log_no).trim(),
						cep: rV.cep,
						cep5: c5r,
						localidade: (localidades.get(rV.loc_nu) || {}).nome || '',
						geo_regra: 'vizinho_cep5',
						nome_regra: gotV.regra,
						fonte: fonte,
						n_vizinhos: anchors.length,
						dist_vizinho_km: Math.round(pertoV[0].dist * 1000) / 1000,
						n_cand_nome: gotV.cand.length,
						vizinhos: vizTop
					});
				}
				var bbV = porBairro.get(rV.bai_ini);
				if (!bbV) porBairro.set(rV.bai_ini, bbV = { sLat: 0, sLng: 0, n: 0 });
				bbV.sLat += agVV.lat;
				bbV.sLng += agVV.lng;
				bbV.n++;
			}
			if (vizinhoOk) {
				log('      vizinho_cep5: recuperou ' + vizinhoOk +
					' (tol=' + vizinhoCep5TolKm + ' km, min=' + vizinhoCep5Min + ')');
			}
		}
		stats.vizinho_cep5_recuperados = vizinhoOk;
		stats.vizinho_cep5_exemplos = exemplosVizinho;

		// ---- Fase 5d: cluster usado por 2+ municípios → dono único
		// Uma via física é de uma cidade só. Preferência: loc_nu com mais linhas ok;
		// empate → âncora municipal mais próxima do centroide do cluster.
		var multiAntes = 0, revogados = 0, clustersMulti = 0;
		if (useExclusao) {
			var claim = new Map(); // clusterId -> Map(loc_nu -> count)
			for (var x = 0; x < rows.length; x++) {
				var rx = rows[x];
				if (rx.status !== 'ok' || !rx.cluster) continue;
				var cid = rx.cluster.id;
				var m = claim.get(cid);
				if (!m) claim.set(cid, m = new Map());
				m.set(rx.loc_nu, (m.get(rx.loc_nu) || 0) + 1);
			}
			var ownerOf = new Map(); // clusterId -> loc_nu
			claim.forEach(function (locs, cid) {
				if (locs.size < 2) {
					ownerOf.set(cid, locs.keys().next().value);
					return;
				}
				clustersMulti++;
				var bestLoc = null, bestN = -1, bestDist = Infinity;
				var aggC = clusters[cid].agg;
				locs.forEach(function (n, locNu) {
					var cen = centroLoc.get(locNu);
					var d = cen ? geo.distKm(cen.lat, cen.lng, aggC.lat, aggC.lng) : 1e9;
					if (n > bestN || (n === bestN && d < bestDist)) {
						bestN = n;
						bestDist = d;
						bestLoc = locNu;
					}
				});
				ownerOf.set(cid, bestLoc);
			});
			for (var y = 0; y < rows.length; y++) {
				var ry = rows[y];
				if (ry.status !== 'ok' || !ry.cluster) continue;
				var cl = claim.get(ry.cluster.id);
				if (cl && cl.size >= 2) multiAntes++;
			}
			for (var z3 = 0; z3 < rows.length; z3++) {
				var r3 = rows[z3];
				if (r3.status !== 'ok' || !r3.cluster) continue;
				var own3 = ownerOf.get(r3.cluster.id);
				if (own3 === undefined || own3 === r3.loc_nu) continue;
				var nLocs = claim.get(r3.cluster.id).size;
				r3.cluster = null;
				r3.regra = '';
				r3.status = 'ambiguo';
				r3.motivo = 'conflito_municipio';
				r3.nCand = nLocs;
				revogados++;
			}
			if (clustersMulti) {
				log('      exclusão multi-município: ' + clustersMulti +
					' clusters, ' + revogados + ' linhas revogadas (de ' + multiAntes + ' em disputa)');
			}
		}
		stats.clusters_multi_municipio = clustersMulti;
		stats.linhas_em_cluster_multi = multiAntes;
		stats.revogados_conflito_municipio = revogados;
	}

	// ---- Fase 6: emitir
	log('[6/6] gravando…');
	fs.mkdirSync(opts.outDir, { recursive: true });
	var writer = txtAt.createTxtAtWriter(opts.outDir, { shardLines: 0 });
	var logKey = 'DNE_GEO_LOGRADOURO_' + uf;
	var f7 = function (v) { return v === null || v === undefined ? '' : Number(v).toFixed(7); };

	/**
	 * Ways do cluster vencedor, ordenadas e sem repetição — a coluna que deixa o
	 * consumidor pegar o traçado por id exato (`OSM_LOGRADOURO_GEOM_{UF}`).
	 *
	 * Ponto de `addr:street` entra em cluster sem `id` (é nó de numeração, não
	 * way, e não tem traçado nenhum): fica de fora em vez de virar um id que não
	 * resolve do outro lado.
	 *
	 * Ordem numérica por determinismo — o mesmo insumo tem de dar byte a byte o
	 * mesmo arquivo, como já vale para `kinds`.
	 */
	var waysDoCluster = function (cluster) {
		if (!cluster || !cluster.feats) return '';
		var vistos = Object.create(null);
		var ids = [];
		for (var w = 0; w < cluster.feats.length; w++) {
			var id = cluster.feats[w].id;
			if (!id || vistos[id]) continue;
			vistos[id] = 1;
			ids.push(id);
		}
		ids.sort(function (a, b) { return Number(a) - Number(b); });
		return ids.join('+');
	};

	var porStatus = {}, porRegra = {}, bairroAgg = new Map();
	// Diagnóstico do resíduo: `ambiguo` mistura causas com tratamentos diferentes.
	var porMotivo = {}, exemplosAmbiguo = {}, distAmbiguo = {};
	// Amostra de matches novos (título) e do residual sem nome — auditoria / aprendizado.
	var exemplosTitulo = [];
	var exemplosSemNome = [];
	for (var m = 0; m < rows.length; m++) {
		var row = rows[m];
		var loc = localidades.get(row.loc_nu) || {};
		var bai = bairros.get(row.bai_ini);
		var a = row.cluster ? row.cluster.agg : null;
		writer.write(logKey, [
			row.log_nu, row.uf, row.loc_nu, row.bai_ini, row.bai_fim,
			row.log_no, row.compl, row.cep, row.tlo, row.sta, row.abrev,
			loc.nome || '', bai ? bai.nome : '', loc.mun || '',
			a ? f7(a.lat) : '', a ? f7(a.lng) : '',
			a ? f7(a.latMin) : '', a ? f7(a.latMax) : '',
			a ? f7(a.lngMin) : '', a ? f7(a.lngMax) : '',
			row.status, row.regra, a ? a.kinds.join('+') : '',
			a ? String(a.ways) : '0', String(row.nCand),
			waysDoCluster(row.cluster)
		]);
		porStatus[row.status] = (porStatus[row.status] || 0) + 1;
		if (row.regra) porRegra[row.regra] = (porRegra[row.regra] || 0) + 1;
		if (
			(row.regra === 'titulo' || row.regra === 'titulo_fonetico') &&
			exemplosTitulo.length < 30
		) {
			var coreDne = keys.coreName(row.comTipo);
			var stripD = keys.stripTitulos(coreDne);
			// cluster.name = name_norm OSM (sem display name no extract)
			var osmNorm = row.cluster ? (row.cluster.name || '') : '';
			var coreOsm = keys.coreName(osmNorm);
			var stripO = keys.stripTitulos(coreOsm);
			exemplosTitulo.push({
				log_nu: row.log_nu,
				dne_nome: (row.tlo + ' ' + row.log_no).trim(),
				osm_name_norm: osmNorm,
				localidade: loc.nome || '',
				bairro: bai ? bai.nome : '',
				cep: row.cep,
				geo_regra: row.regra,
				dne_nucleo: coreDne,
				osm_nucleo: coreOsm,
				nucleo_bare: stripD.bare,
				tokens_removidos_dne: stripD.removed,
				tokens_removidos_osm: stripO.removed,
				candidatos: row.nCand
			});
		}
		if (row.status === 'sem_nome_osm' && exemplosSemNome.length < 30) {
			var coreSn = keys.coreName(row.comTipo);
			var bareSn = keys.stripTitulos(coreSn);
			exemplosSemNome.push({
				log_nu: row.log_nu,
				nome: (row.tlo + ' ' + row.log_no).trim(),
				localidade: loc.nome || '',
				bairro: bai ? bai.nome : '',
				cep: row.cep,
				cep5: String(row.cep || '').replace(/\D/g, '').slice(0, 5),
				nucleo: coreSn,
				nucleo_bare: bareSn.bare,
				titulos_no_dne: bareSn.removed,
				tlo: row.tlo
			});
		}
		if (row.status === 'ambiguo') {
			var mv = (row.motivo || 'desconhecido').replace(/_\d+km$/, '_longa');
			porMotivo[mv] = (porMotivo[mv] || 0) + 1;
			if (row.distKm !== undefined) {
				var faixa = row.distKm <= 1 ? 'ate_1km'
					: row.distKm <= 5 ? 'de_1_a_5km'
						: row.distKm <= 20 ? 'de_5_a_20km' : 'mais_de_20km';
				distAmbiguo[faixa] = (distAmbiguo[faixa] || 0) + 1;
			}
			if (!exemplosAmbiguo[mv]) exemplosAmbiguo[mv] = [];
			if (exemplosAmbiguo[mv].length < 20) {
				var ex = {
					log_nu: row.log_nu,
					nome: (row.tlo + ' ' + row.log_no).trim(),
					localidade: loc.nome || '',
					bairro: bai ? bai.nome : '',
					cep: row.cep,
					candidatos: row.nCand,
					detalhe: row.motivo
				};
				if (row.distKm !== undefined) ex.km_da_mancha = Math.round(row.distKm * 100) / 100;
				exemplosAmbiguo[mv].push(ex);
			}
		}
		if (a && row.bai_ini) {
			var ba = bairroAgg.get(row.bai_ini);
			if (!ba) bairroAgg.set(row.bai_ini, ba = {
				latMin: a.latMin, latMax: a.latMax, lngMin: a.lngMin, lngMax: a.lngMax,
				sLat: 0, sLng: 0, n: 0
			});
			if (a.latMin < ba.latMin) ba.latMin = a.latMin;
			if (a.latMax > ba.latMax) ba.latMax = a.latMax;
			if (a.lngMin < ba.lngMin) ba.lngMin = a.lngMin;
			if (a.lngMax > ba.lngMax) ba.lngMax = a.lngMax;
			ba.sLat += a.lat; ba.sLng += a.lng; ba.n++;
		}
	}

	var baiKey = 'DNE_GEO_BAIRRO_' + uf;
	bairroAgg.forEach(function (ba, baiNu) {
		var b = bairros.get(baiNu);
		if (!b) return;
		var loc2 = localidades.get(b.loc_nu) || {};
		writer.write(baiKey, [
			baiNu, uf, b.loc_nu, b.nome, loc2.nome || '', loc2.mun || '',
			f7(ba.sLat / ba.n), f7(ba.sLng / ba.n),
			f7(ba.latMin), f7(ba.latMax), f7(ba.lngMin), f7(ba.lngMax),
			String(ba.n)
		]);
	});
	// flush(), não closeSync(): o closeSync só chama stream.end() sem esperar,
	// e o arquivo pode não estar completo quando run() retorna.
	await writer.flush();

	var relatorio = {
		uf: uf,
		gerado_por: 'dne-geo-join.js',
		dne_dir: opts.dneDir,
		osm_dir: opts.osmDir,
		cluster_cell: clusterCell,
		footprint_cell: footprintCell,
		linhas_dne: rows.length,
		osm: {
			linhas: osm.lines, sem_geom: osm.semGeom, nomes: built.byName.size,
			clusters: clusters.length, addr_extras: addrAdded,
			mode: osm.mode || (semExtract ? 'missing' : 'flat'),
			files: osm.files || 0
		},
		localidades: { total: localidades.size, com_footprint: footprints.size,
			herdados_de_subordinacao: herdados, ancoras: ancoras },
		geo_status: porStatus,
		geo_regra: porRegra,
		ambiguo_por_motivo: porMotivo,
		ambiguo_distancia_ate_a_mancha: distAmbiguo,
		ambiguo_exemplos: exemplosAmbiguo,
		titulo_exemplos: exemplosTitulo,
		sem_nome_osm_exemplos: exemplosSemNome,
		vizinho_cep5_exemplos: stats.vizinho_cep5_exemplos || [],
		rodadas: stats.rodadas,
		envelope_recuperados: stats.envelope_recuperados || 0,
		vizinho_cep5_recuperados: stats.vizinho_cep5_recuperados || 0,
		clusters_multi_municipio: stats.clusters_multi_municipio || 0,
		linhas_em_cluster_multi: stats.linhas_em_cluster_multi || 0,
		revogados_conflito_municipio: stats.revogados_conflito_municipio || 0,
		bairros_com_bbox: bairroAgg.size
	};
	fs.writeFileSync(
		path.join(opts.outDir, 'DNE_GEO_RELATORIO_' + uf + '.json'),
		JSON.stringify(relatorio, null, 2), 'utf8'
	);

	var okPct = rows.length ? ((porStatus.ok || 0) / rows.length * 100).toFixed(1) : '0.0';
	log('OK: ' + (porStatus.ok || 0) + '/' + rows.length + ' (' + okPct + '%)  ' +
		REGRAS.map(function (k) { return k + '=' + (porRegra[k] || 0); }).join(' '));
	return relatorio;
}

function parseCli(argv) {
	var o = {
		dneDir: null, osmDir: null, outDir: null, uf: null,
		clusterCell: 0, footprintCell: 0, quiet: false
	};
	for (var i = 0; i < argv.length; i++) {
		var a = argv[i];
		if (a.indexOf('--dne=') === 0) o.dneDir = a.slice(6);
		else if (a.indexOf('--osm=') === 0) o.osmDir = a.slice(6);
		else if (a.indexOf('--out=') === 0) o.outDir = a.slice(6);
		else if (a.indexOf('--uf=') === 0) o.uf = a.slice(5);
		else if (a.indexOf('--cluster-cell=') === 0) o.clusterCell = Number(a.slice(15));
		else if (a.indexOf('--footprint-cell=') === 0) o.footprintCell = Number(a.slice(17));
		else if (a.indexOf('--max-extent-km=') === 0) o.maxExtentKm = Number(a.slice(16));
		else if (a.indexOf('--footprint-dilate=') === 0) o.footprintDilate = Number(a.slice(19));
		else if (a.indexOf('--envelope-tol-km=') === 0) o.envelopeTolKm = Number(a.slice(18));
		else if (a === '--sem-envelope') o.semEnvelope = true;
		else if (a.indexOf('--vizinho-cep5-tol-km=') === 0) o.vizinhoCep5TolKm = Number(a.slice(22));
		else if (a.indexOf('--vizinho-cep5-min=') === 0) o.vizinhoCep5Min = Number(a.slice(19));
		else if (a === '--sem-vizinho-cep5') o.semVizinhoCep5 = true;
		else if (a === '--sem-exclusao-cluster') o.semExclusaoCluster = true;
		else if (a === '--quiet') o.quiet = true;
	}
	return o;
}

module.exports = {
	run: run,
	parseCli: parseCli,
	buildClusters: buildClusters,
	buildDerivedIndexes: buildDerivedIndexes,
	candidatesFor: candidatesFor,
	resolveOsmLogradouro: resolveOsmLogradouro,
	loadOsmStreets: loadOsmStreets,
	digitsCep5: digitsCep5,
	nearestDistKm: nearestDistKm
};

if (require.main === module) {
	var opts = parseCli(process.argv.slice(2));
	if (!opts.dneDir || !opts.osmDir || !opts.outDir || !opts.uf) {
		process.stderr.write(
			'uso: node dne-geo-join.js --dne=DIR --osm=DIR --out=DIR --uf=SP\n'
		);
		process.exit(1);
	}
	main(opts).catch(function (e) {
		process.stderr.write(String(e && e.stack || e) + '\n');
		process.exit(2);
	});
}
