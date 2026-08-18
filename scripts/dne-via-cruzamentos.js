#!/usr/bin/env node
'use strict';

/**
 * Cruzamentos / conexões entre ways DNE-casadas + densificação ≤ ~111 m.
 *
 * Insumos: DNE_GEO_LOGRADOURO_{UF} (col. 26 osm_way_ids) + OSM_LOGRADOURO_GEOM_{UF}.
 * Saídas: DNE_GEO_VIA_PONTO_{UF}.TXT, DNE_GEO_VIA_LIGACAO_{UF}.TXT, relatório JSON.
 *
 * Doc: docs/geo/via-cruzamentos-densificar.md
 *
 *   node scripts/dne-via-cruzamentos.js --dne-geo=G:\dne-geo-conectores-fuzzy --geom=G:\osm-geo-br-geom\sp --out=G:\dne-geo-via-sp --uf=SP
 */

var fs = require('fs');
var path = require('path');
var readline = require('readline');

var ROOT = path.resolve(__dirname, '..');
var txtAt = require(path.join(ROOT, 'txt-at-writer'));
var poly = require(path.join(ROOT, 'geo-polyline'));
var vg = require(path.join(ROOT, 'via-geom'));
var oneway = require(path.join(ROOT, 'osm-oneway'));

/**
 * Parseia --bbox=minLon,minLat,maxLon,maxLat (ordem OSM/Leaflet).
 * @returns {{ minLng: number, minLat: number, maxLng: number, maxLat: number }|null}
 */
function parseBbox(raw) {
	if (!raw) return null;
	var parts = String(raw).split(/[,;\s]+/).filter(Boolean);
	if (parts.length !== 4) {
		throw new Error('--bbox precisa de 4 números: minLon,minLat,maxLon,maxLat');
	}
	var minLng = Number(parts[0]);
	var minLat = Number(parts[1]);
	var maxLng = Number(parts[2]);
	var maxLat = Number(parts[3]);
	if (![minLng, minLat, maxLng, maxLat].every(isFinite)) {
		throw new Error('--bbox com valor não numérico');
	}
	if (minLng > maxLng || minLat > maxLat) {
		throw new Error('--bbox: min deve ser ≤ max (lon e lat)');
	}
	return { minLng: minLng, minLat: minLat, maxLng: maxLng, maxLat: maxLat };
}

/** Envelope da polyline intersecta o bbox? */
function wayIntersectsBbox(pts, bbox) {
	if (!bbox || !pts || !pts.length) return true;
	var minLat = Infinity;
	var maxLat = -Infinity;
	var minLng = Infinity;
	var maxLng = -Infinity;
	for (var i = 0; i < pts.length; i++) {
		var lat = pts[i][0];
		var lng = pts[i][1];
		if (lat < minLat) minLat = lat;
		if (lat > maxLat) maxLat = lat;
		if (lng < minLng) minLng = lng;
		if (lng > maxLng) maxLng = lng;
	}
	return !(
		maxLat < bbox.minLat ||
		minLat > bbox.maxLat ||
		maxLng < bbox.minLng ||
		minLng > bbox.maxLng
	);
}

/** Remove ways cujo envelope não intersecta o bbox. Devolve quantas saíram. */
function filterWaysByBbox(ways, bbox) {
	if (!bbox) return 0;
	var dropped = 0;
	ways.forEach(function (pts, osmId) {
		if (!wayIntersectsBbox(pts, bbox)) {
			ways.delete(osmId);
			dropped++;
		}
	});
	return dropped;
}

function parseCli(argv) {
	var o = {
		dneGeo: null,
		geom: null,
		out: null,
		uf: null,
		cell: 0.002,
		maxSegKm: vg.MAX_SEG_KM_DEFAULT,
		touchKm: vg.TOUCH_KM_DEFAULT,
		onlyOk: true,
		bbox: null,
		help: false
	};
	for (var i = 0; i < argv.length; i++) {
		var a = argv[i];
		if (a.indexOf('--dne-geo=') === 0) o.dneGeo = a.slice(10);
		else if (a.indexOf('--geom=') === 0) o.geom = a.slice(7);
		else if (a.indexOf('--out=') === 0) o.out = a.slice(6);
		else if (a.indexOf('--uf=') === 0) o.uf = a.slice(5).toUpperCase();
		else if (a.indexOf('--cell=') === 0) o.cell = Number(a.slice(7));
		else if (a.indexOf('--max-seg-km=') === 0) o.maxSegKm = Number(a.slice(13));
		else if (a.indexOf('--touch-km=') === 0) o.touchKm = Number(a.slice(11));
		else if (a.indexOf('--bbox=') === 0) o.bbox = parseBbox(a.slice(7));
		else if (a === '--only-ok') o.onlyOk = true;
		else if (a === '--no-only-ok') o.onlyOk = false;
		else if (a === '--help' || a === '-h') o.help = true;
	}
	return o;
}

function usage() {
	return [
		'Uso: node scripts/dne-via-cruzamentos.js --dne-geo=DIR --geom=DIR --out=DIR --uf=UF',
		'',
		'  --dne-geo=DIR     pasta com DNE_GEO_LOGRADOURO_{UF}.TXT (26 cols)',
		'  --geom=DIR        pasta com OSM_LOGRADOURO_GEOM_{UF} (flat ou shard)',
		'  --out=DIR         saída dos artefatos VIA_*',
		'  --uf=SP',
		'  --bbox=minLon,minLat,maxLon,maxLat   só ways cujo envelope intersecta',
		'  --cell=0.002      grau ≈ 220 m (grade espacial)',
		'  --max-seg-km=0.111',
		'  --touch-km=0.0015  tolerância T-junction / endpoints (~1,5 m)',
		'  --only-ok         (default) só geo_status=ok',
		'',
		'Ex. RMSP: --bbox=-47.20,-24.05,-46.30,-23.20',
		'',
		'Doc: docs/geo/via-cruzamentos-densificar.md'
	].join('\n');
}

function readLines(file, encoding, onLine) {
	return new Promise(function (resolve, reject) {
		var rl = readline.createInterface({
			input: fs.createReadStream(file, encoding ? { encoding: encoding } : undefined),
			crlfDelay: Infinity
		});
		rl.on('line', function (l) {
			if (l) onLine(l);
		});
		rl.on('close', resolve);
		rl.on('error', reject);
	});
}

async function readLinesMany(files, encoding, onLine) {
	for (var i = 0; i < files.length; i++) {
		await readLines(files[i], encoding, onLine);
	}
}

function fmtCoord(n) {
	// até 7 casas — alinhado ao restante do pipeline
	var s = String(Math.round(n * 1e7) / 1e7);
	return s;
}

/**
 * Coleta osm_id ↔ log_nu a partir do join DNE.
 * @returns {{ osmToLogs: Map<string,Set<string>>, logToOsms: Map<string,string[]>, rowsOk: number, rowsSkip: number, warnNoIds: number }}
 */
async function collectDneIds(dnePath, onlyOk) {
	var osmToLogs = new Map();
	var logToOsms = new Map();
	var rowsOk = 0;
	var rowsSkip = 0;
	var warnNoIds = 0;
	var colsShort = 0;

	await readLines(dnePath, 'utf8', function (l) {
		var p = l.split('@');
		if (p.length < 26) {
			colsShort++;
			rowsSkip++;
			return;
		}
		var status = p[20];
		if (onlyOk && status !== 'ok') {
			rowsSkip++;
			return;
		}
		var logNu = p[0];
		var idsRaw = p[25] || '';
		if (!idsRaw) {
			warnNoIds++;
			rowsSkip++;
			return;
		}
		var ids = idsRaw.split('+').filter(Boolean);
		if (!ids.length) {
			warnNoIds++;
			rowsSkip++;
			return;
		}
		rowsOk++;
		if (!logToOsms.has(logNu)) logToOsms.set(logNu, []);
		var arr = logToOsms.get(logNu);
		for (var i = 0; i < ids.length; i++) {
			var oid = ids[i];
			arr.push(oid);
			if (!osmToLogs.has(oid)) osmToLogs.set(oid, new Set());
			osmToLogs.get(oid).add(logNu);
		}
	});

	// dedupe log→osms
	logToOsms.forEach(function (arr, logNu) {
		var seen = new Set();
		var uniq = [];
		for (var i = 0; i < arr.length; i++) {
			if (seen.has(arr[i])) continue;
			seen.add(arr[i]);
			uniq.push(arr[i]);
		}
		logToOsms.set(logNu, uniq);
	});

	return {
		osmToLogs: osmToLogs,
		logToOsms: logToOsms,
		rowsOk: rowsOk,
		rowsSkip: rowsSkip,
		warnNoIds: warnNoIds,
		colsShort: colsShort
	};
}

/**
 * Carrega polylines só dos osm_id pedidos.
 * Aceita GEOM legado (`osm_id@polyline`) e novo (`osm_id@polyline@oneway`).
 * @returns {{ ways: Map<string, number[][]>, onewayByOsm: Map<string, number>, loaded: number, skipped: number }}
 */
async function loadGeomSubset(geomPaths, osmWanted) {
	var ways = new Map();
	var onewayByOsm = new Map();
	var loaded = 0;
	var skipped = 0;
	await readLinesMany(geomPaths, 'utf8', function (l) {
		var at = l.indexOf('@');
		if (at < 0) return;
		var oid = l.slice(0, at);
		if (!osmWanted.has(oid)) {
			skipped++;
			return;
		}
		if (ways.has(oid)) return;
		var parts = oneway.splitGeomPayload(l.slice(at + 1));
		try {
			var pts = poly.decodePolyline(parts.polyline);
			if (pts.length >= 2) {
				ways.set(oid, pts);
				onewayByOsm.set(oid, parts.oneway);
				loaded++;
			}
		} catch (_) {
			/* polyline inválida — ignora */
		}
	});
	return { ways: ways, onewayByOsm: onewayByOsm, loaded: loaded, skipped: skipped };
}

/**
 * Segmentos + grade.
 * seg = { osmId, si, a:[lat,lng], b:[lat,lng] }
 */
function buildSegmentGrid(ways, cell) {
	var segs = [];
	var grid = new Map();
	ways.forEach(function (pts, osmId) {
		for (var i = 0; i < pts.length - 1; i++) {
			var a = pts[i];
			var b = pts[i + 1];
			if (a[0] === b[0] && a[1] === b[1]) continue;
			var idx = segs.length;
			segs.push({ osmId: osmId, si: i, a: a, b: b });
			var keys = vg.segmentCellKeys(a, b, cell);
			for (var k = 0; k < keys.length; k++) {
				var key = keys[k];
				var bucket = grid.get(key);
				if (!bucket) {
					bucket = [];
					grid.set(key, bucket);
				}
				bucket.push(idx);
			}
		}
	});
	return { segs: segs, grid: grid };
}

/**
 * Descobre cruzamentos e conexões entre ways distintas.
 * Devolve:
 *   hits: Array<{ osmA, osmB, lat, lng, tipo }>  tipo = cruzamento|conexao
 *   extrasByOsm: Map osmId → [{ lat, lng, origem }]
 *
 * Evita Set global de pares de segmentos (estoura o teto ~16.7M do V8 no
 * extract BR — mesmo RangeError). Dedup de hits: Map externo + Set pequeno
 * por par de ways; pares de segmentos: stamp Int32 por varredura.
 *
 * Conexões: (1) qualquer vértice compartilhado entre ways, (2) T-junction
 * extremo→interior de segmento, (3) cruzamento segmento×segmento.
 */
function findConnectionsFull(ways, segs, grid, cell, touchKm) {
	var hits = [];
	/** @type {Map<string, Set<string>>} pairKey "osmLo|osmHi|tipo" → pointKeys */
	var seenHit = new Map();
	var extrasByOsm = new Map();

	function addExtra(osmId, lat, lng, origem) {
		var list = extrasByOsm.get(osmId);
		if (!list) {
			list = [];
			extrasByOsm.set(osmId, list);
		}
		list.push({ lat: lat, lng: lng, origem: origem });
	}

	function record(osmA, osmB, lat, lng, tipo) {
		if (osmA === osmB) return;
		var lo = osmA < osmB ? osmA : osmB;
		var hi = osmA < osmB ? osmB : osmA;
		var pk = vg.pointKey(lat, lng);
		var pairKey = lo + '|' + hi + '|' + tipo;
		var pts = seenHit.get(pairKey);
		if (!pts) {
			pts = new Set();
			seenHit.set(pairKey, pts);
		}
		if (pts.has(pk)) return;
		pts.add(pk);
		hits.push({ osmA: lo, osmB: hi, lat: lat, lng: lng, tipo: tipo });
		addExtra(lo, lat, lng, tipo);
		addExtra(hi, lat, lng, tipo);
	}

	// Vértice compartilhado (caso OSM típico: mesmo nó no meio das duas ways).
	// Cobre também extremo↔extremo; índice por pointKey 1e-6°, um registro por way.
	var vertIndex = new Map();
	ways.forEach(function (pts, osmId) {
		for (var vi = 0; vi < pts.length; vi++) {
			var pt = pts[vi];
			var pk = vg.pointKey(pt[0], pt[1]);
			var bucket = vertIndex.get(pk);
			if (!bucket) {
				bucket = [];
				vertIndex.set(pk, bucket);
			}
			var already = false;
			for (var b = 0; b < bucket.length; b++) {
				if (bucket[b].osmId === osmId) {
					already = true;
					break;
				}
			}
			if (!already) bucket.push({ osmId: osmId, lat: pt[0], lng: pt[1] });
		}
	});
	vertIndex.forEach(function (bucket) {
		if (bucket.length < 2) return;
		for (var i = 0; i < bucket.length; i++) {
			for (var j = i + 1; j < bucket.length; j++) {
				record(
					bucket[i].osmId,
					bucket[j].osmId,
					bucket[i].lat,
					bucket[i].lng,
					'conexao'
				);
			}
		}
	});

	// T-junction: endpoint → interior de segmento (não vértice — já coberto acima)
	var touchStamp = new Int32Array(segs.length);
	var touchGen = 1;
	ways.forEach(function (pts, osmId) {
		var ends = [pts[0], pts[pts.length - 1]];
		for (var e = 0; e < ends.length; e++) {
			var ep = ends[e];
			touchGen++;
			if (touchGen >= 0x7fffffff) {
				touchStamp.fill(0);
				touchGen = 1;
			}
			var keys = vg.segmentCellKeys(ep, ep, cell);
			for (var ki = 0; ki < keys.length; ki++) {
				var bucket = grid.get(keys[ki]);
				if (!bucket) continue;
				for (var bi = 0; bi < bucket.length; bi++) {
					var sidx = bucket[bi];
					if (touchStamp[sidx] === touchGen) continue;
					touchStamp[sidx] = touchGen;
					var seg = segs[sidx];
					if (seg.osmId === osmId) continue;
					var pr = vg.projectPointOnSegment(ep, seg.a, seg.b);
					if (pr.distKm > touchKm) continue;
					if (pr.t <= 1e-6 || pr.t >= 1 - 1e-6) continue;
					record(osmId, seg.osmId, pr.lat, pr.lng, 'conexao');
				}
			}
		}
	});

	// segmento × segmento — um par (i,j) i<j no máximo uma vez (stamp),
	// sem Set de todas as combinações vizinhas (estoura V8 em UF/RMSP).
	var pairStamp = new Int32Array(segs.length);
	var pairGen = 1;
	for (var si = 0; si < segs.length; si++) {
		var sa = segs[si];
		pairGen++;
		if (pairGen >= 0x7fffffff) {
			pairStamp.fill(0);
			pairGen = 1;
		}
		var skeys = vg.segmentCellKeys(sa.a, sa.b, cell);
		for (var ski = 0; ski < skeys.length; ski++) {
			var sbucket = grid.get(skeys[ski]);
			if (!sbucket) continue;
			for (var sbi = 0; sbi < sbucket.length; sbi++) {
				var sj = sbucket[sbi];
				if (sj <= si) continue;
				if (pairStamp[sj] === pairGen) continue;
				pairStamp[sj] = pairGen;
				var sb = segs[sj];
				if (sa.osmId === sb.osmId) continue;
				var hit = vg.segmentIntersection(sa.a, sa.b, sb.a, sb.b);
				if (!hit) continue;
				record(sa.osmId, sb.osmId, hit.lat, hit.lng, 'cruzamento');
			}
		}
	}

	return { hits: hits, extrasByOsm: extrasByOsm };
}

function writeUtf8Lines(filePath, lines) {
	var fd = fs.openSync(filePath, 'w');
	try {
		for (var i = 0; i < lines.length; i++) {
			fs.writeSync(fd, lines[i] + '\n', null, 'utf8');
		}
	} finally {
		fs.closeSync(fd);
	}
}

/**
 * Emite pontos e ligações agregados a log_nu.
 */
function emitArtifacts(ways, osmToLogs, hits, extrasByOsm, maxSegKm) {
	var pontoLines = [];
	/** Dedup ligações sem Set monolítico (teto V8 ~16.7M). */
	var ligSeen = new Map(); // "lo|hi|tipo|osmA|osmB" → Set(pointKey)
	var ligLines = [];
	var nVertice = 0;
	var nCruz = 0;
	var nConex = 0;
	var nAmostra = 0;

	ways.forEach(function (pts, osmId) {
		var extras = extrasByOsm.get(osmId) || [];
		var points = vg.emitDensifiedPoints(pts, extras, maxSegKm);
		var logs = osmToLogs.get(osmId);
		if (!logs || !logs.size) return;
		for (var pi = 0; pi < points.length; pi++) {
			var pt = points[pi];
			if (pt.origem === 'vertice') nVertice++;
			else if (pt.origem === 'cruzamento') nCruz++;
			else if (pt.origem === 'conexao') nConex++;
			else if (pt.origem === 'amostra') nAmostra++;
			logs.forEach(function (logNu) {
				pontoLines.push(
					[
						logNu,
						fmtCoord(pt.lat),
						fmtCoord(pt.lng),
						pt.origem,
						osmId
					].join('@')
				);
			});
		}
	});

	for (var hi = 0; hi < hits.length; hi++) {
		var h = hits[hi];
		var logsA = osmToLogs.get(h.osmA);
		var logsB = osmToLogs.get(h.osmB);
		if (!logsA || !logsB) continue;
		logsA.forEach(function (la) {
			logsB.forEach(function (lb) {
				if (la === lb) return; // continuidade intra-log: omitida nas ligações
				var lo = la < lb ? la : lb;
				var hiLog = la < lb ? lb : la;
				var osmLo = h.osmA;
				var osmHi = h.osmB;
				var pairKey =
					lo + '|' + hiLog + '|' + h.tipo + '|' + osmLo + '|' + osmHi;
				var pk = vg.pointKey(h.lat, h.lng);
				var pts = ligSeen.get(pairKey);
				if (!pts) {
					pts = new Set();
					ligSeen.set(pairKey, pts);
				}
				if (pts.has(pk)) return;
				pts.add(pk);
				ligLines.push(
					[
						lo,
						hiLog,
						fmtCoord(h.lat),
						fmtCoord(h.lng),
						h.tipo,
						osmLo,
						osmHi
					].join('@')
				);
			});
		});
	}

	return {
		pontoLines: pontoLines,
		ligLines: ligLines,
		stats: {
			nVertice: nVertice,
			nCruzamento: nCruz,
			nConexao: nConex,
			nAmostra: nAmostra,
			nPontoRows: pontoLines.length,
			nLigacaoRows: ligLines.length
		}
	};
}

async function main() {
	var opt;
	try {
		opt = parseCli(process.argv.slice(2));
	} catch (e) {
		console.error(e.message || e);
		process.exit(1);
	}
	if (opt.help || !opt.dneGeo || !opt.geom || !opt.out || !opt.uf) {
		console.log(usage());
		process.exit(opt.help ? 0 : 1);
	}
	if (!isFinite(opt.cell) || opt.cell <= 0) {
		console.error('--cell inválido');
		process.exit(1);
	}
	if (!isFinite(opt.maxSegKm) || opt.maxSegKm <= 0) {
		console.error('--max-seg-km inválido');
		process.exit(1);
	}

	var t0 = Date.now();
	var uf = opt.uf;
	fs.mkdirSync(opt.out, { recursive: true });

	var dnePath = path.join(opt.dneGeo, 'DNE_GEO_LOGRADOURO_' + uf + '.TXT');
	if (!fs.existsSync(dnePath)) {
		console.error('Arquivo DNE não encontrado:', dnePath);
		process.exit(1);
	}

	var geomRes = txtAt.resolveDatasetPaths(opt.geom, 'OSM_LOGRADOURO_GEOM_' + uf);
	if (geomRes.mode === 'missing' || !geomRes.paths.length) {
		console.error('GEOM não encontrado em', opt.geom, 'para', uf);
		process.exit(1);
	}

	console.log('[1/5] Coletando osm_way_ids de', dnePath);
	var dne = await collectDneIds(dnePath, opt.onlyOk);
	if (dne.colsShort) {
		console.error(
			'AVISO: ' +
				dne.colsShort +
				' linhas com <26 cols (join antigo sem osm_way_ids?). Use DNE_GEO com coluna 26.'
		);
	}
	console.log(
		'  ok com ids:',
		dne.rowsOk,
		'| skip:',
		dne.rowsSkip,
		'| osm ids únicos:',
		dne.osmToLogs.size,
		'| log_nu:',
		dne.logToOsms.size
	);
	if (!dne.osmToLogs.size) {
		console.error('Nenhum osm_id para processar.');
		process.exit(1);
	}

	console.log('[2/5] Carregando GEOM (' + geomRes.mode + ', ' + geomRes.paths.length + ' arquivo(s))');
	var geom = await loadGeomSubset(geomRes.paths, dne.osmToLogs);
	console.log('  ways com polyline:', geom.ways.size, '| linhas GEOM ignoradas (fora do set):', geom.skipped);
	var missingGeom = dne.osmToLogs.size - geom.ways.size;
	if (missingGeom > 0) {
		console.log('  osm_ids sem GEOM:', missingGeom);
	}
	var onewayCounts = { 0: 0, 1: 0, 2: 0, 3: 0 };
	geom.onewayByOsm.forEach(function (code) {
		onewayCounts[code] = (onewayCounts[code] || 0) + 1;
	});
	console.log(
		'  oneway (0 ausente / 1 frente / 2 reverso / 3 duplo):',
		onewayCounts[0] + '/' + onewayCounts[1] + '/' + onewayCounts[2] + '/' + onewayCounts[3]
	);
	var bboxDropped = 0;
	if (opt.bbox) {
		bboxDropped = filterWaysByBbox(geom.ways, opt.bbox);
		geom.onewayByOsm.forEach(function (_c, oid) {
			if (!geom.ways.has(oid)) geom.onewayByOsm.delete(oid);
		});
		onewayCounts = { 0: 0, 1: 0, 2: 0, 3: 0 };
		geom.onewayByOsm.forEach(function (code) {
			onewayCounts[code] = (onewayCounts[code] || 0) + 1;
		});
		console.log(
			'  bbox',
			[opt.bbox.minLng, opt.bbox.minLat, opt.bbox.maxLng, opt.bbox.maxLat].join(','),
			'→ ways no recorte:',
			geom.ways.size,
			'(fora:',
			bboxDropped + ')'
		);
		console.log(
			'  oneway no recorte:',
			onewayCounts[0] + '/' + onewayCounts[1] + '/' + onewayCounts[2] + '/' + onewayCounts[3]
		);
		if (!geom.ways.size) {
			console.error('Nenhuma way intersecta o --bbox.');
			process.exit(1);
		}
	}

	console.log('[3/5] Grade de segmentos cell=' + opt.cell);
	var gridBuilt = buildSegmentGrid(geom.ways, opt.cell);
	console.log('  segmentos:', gridBuilt.segs.length, '| células:', gridBuilt.grid.size);

	console.log('[4/5] Cruzamentos / conexões touchKm=' + opt.touchKm);
	var conn = findConnectionsFull(
		geom.ways,
		gridBuilt.segs,
		gridBuilt.grid,
		opt.cell,
		opt.touchKm
	);
	console.log('  hits (pares way):', conn.hits.length);

	console.log('[5/5] Densificando e emitindo artefatos maxSegKm=' + opt.maxSegKm);
	var art = emitArtifacts(
		geom.ways,
		dne.osmToLogs,
		conn.hits,
		conn.extrasByOsm,
		opt.maxSegKm
	);

	var pontoPath = path.join(opt.out, 'DNE_GEO_VIA_PONTO_' + uf + '.TXT');
	var ligPath = path.join(opt.out, 'DNE_GEO_VIA_LIGACAO_' + uf + '.TXT');
	var relPath = path.join(opt.out, 'DNE_GEO_VIA_RELATORIO_' + uf + '.json');

	writeUtf8Lines(pontoPath, art.pontoLines);
	writeUtf8Lines(ligPath, art.ligLines);

	var elapsedMs = Date.now() - t0;
	var rel = {
		uf: uf,
		dne_geo: path.resolve(opt.dneGeo),
		geom: path.resolve(opt.geom),
		out: path.resolve(opt.out),
		cell: opt.cell,
		max_seg_km: opt.maxSegKm,
		touch_km: opt.touchKm,
		bbox: opt.bbox,
		ways_fora_bbox: bboxDropped,
		rows_ok: dne.rowsOk,
		rows_skip: dne.rowsSkip,
		osm_ids_dne: dne.osmToLogs.size,
		log_nu: dne.logToOsms.size,
		ways_geom: geom.ways.size,
		oneway: onewayCounts,
		osm_sem_geom: missingGeom,
		segmentos: gridBuilt.segs.length,
		celulas: gridBuilt.grid.size,
		hits_way: conn.hits.length,
		pontos: art.stats,
		elapsed_ms: elapsedMs,
		finished_at: new Date().toISOString()
	};
	fs.writeFileSync(relPath, JSON.stringify(rel, null, 2), 'utf8');

	console.log('OK');
	console.log('  ', pontoPath, '(' + art.pontoLines.length + ' linhas)');
	console.log('  ', ligPath, '(' + art.ligLines.length + ' linhas)');
	console.log('  ', relPath);
	console.log('  tempo:', (elapsedMs / 1000).toFixed(1) + 's');
}

module.exports = {
	collectDneIds: collectDneIds,
	loadGeomSubset: loadGeomSubset,
	buildSegmentGrid: buildSegmentGrid,
	findConnectionsFull: findConnectionsFull,
	emitArtifacts: emitArtifacts,
	parseCli: parseCli,
	parseBbox: parseBbox,
	wayIntersectsBbox: wayIntersectsBbox,
	filterWaysByBbox: filterWaysByBbox
};

if (require.main === module) {
	main().catch(function (err) {
		console.error(err);
		process.exit(1);
	});
}
