/**
 * Relatório de qualidade das coordenadas em CEP_EXTERNO.TXT.
 *
 * Detecta buckets por gaps em `consultado_em` e grava um markdown por bucket
 * (+ um índice). Também pode analisar só a última rodada (--last).
 *
 *   node scripts/cep-externo-quality.mjs
 *   node scripts/cep-externo-quality.mjs --cache=G:\dne-geo-local\CEP_EXTERNO.TXT
 *   node scripts/cep-externo-quality.mjs --dne=G:\dne-geo-local --out=G:\dne-geo-local\qualidade
 *   node scripts/cep-externo-quality.mjs --last   # só o bucket mais recente
 *   node scripts/cep-externo-quality.mjs --ceps=a,b,c --label=bucket-005  # lista explícita
 */
import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { createRequire } from 'module';
import { fileURLToPath } from 'url';

const require = createRequire(import.meta.url);
const cepExt = require('../cep-externo.js');
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

const BR_CENTER = { lat: -14.235004, lng: -51.92528, tol: 0.001 };

function parseArgs(argv) {
	const o = {
		cache: 'G:\\dne-geo-local\\CEP_EXTERNO.TXT',
		dne: 'G:\\dne-geo-local',
		out: 'G:\\dne-geo-local\\qualidade',
		ufs: ['SP', 'RJ', 'MG', 'ES'],
		gapSec: 90,
		last: false,
		label: null,
		ceps: null,
	};
	for (const a of argv) {
		if (a.startsWith('--cache=')) o.cache = a.slice(8);
		else if (a.startsWith('--dne=')) o.dne = a.slice(6);
		else if (a.startsWith('--out=')) o.out = a.slice(6);
		else if (a.startsWith('--ufs=')) o.ufs = a.slice(6).split(',').map((s) => s.trim().toUpperCase());
		else if (a.startsWith('--gap-sec=')) o.gapSec = Number(a.slice(10));
		else if (a.startsWith('--label=')) o.label = a.slice(8);
		else if (a.startsWith('--ceps=')) o.ceps = a.slice(7).split(',').map((c) => cepExt.digitsCep(c)).filter(Boolean);
		else if (a.startsWith('--ceps-file=')) o.cepsFile = a.slice(12);
		else if (a === '--last') o.last = true;
	}
	return o;
}

function num(v) {
	const n = Number(v);
	return Number.isFinite(n) ? n : null;
}

function distKm(lat1, lng1, lat2, lng2) {
	const dLat = (lat1 - lat2) * 111;
	const dLng = (lng1 - lng2) * 111 * Math.cos(((lat1 + lat2) / 2) * Math.PI / 180);
	return Math.sqrt(dLat * dLat + dLng * dLng);
}

function pct(arr, p) {
	if (!arr.length) return null;
	const i = Math.min(arr.length - 1, Math.floor(arr.length * p));
	return Math.round(arr[i] * 100) / 100;
}

function isBrazilCenter(lat, lng) {
	return Math.abs(lat - BR_CENTER.lat) < BR_CENTER.tol
		&& Math.abs(lng - BR_CENTER.lng) < BR_CENTER.tol;
}

async function loadMunBboxFromOk(dir, ufs) {
	const mun = new Map();
	for (const uf of ufs) {
		const file = path.join(dir, `DNE_GEO_LOGRADOURO_${uf}.TXT`);
		if (!fs.existsSync(file)) continue;
		const rl = readline.createInterface({
			input: fs.createReadStream(file, { encoding: 'utf8' }),
			crlfDelay: Infinity,
		});
		for await (const line of rl) {
			const p = line.split('@');
			if (p[20] !== 'ok') continue;
			const lat = num(p[14]);
			const lng = num(p[15]);
			const munNu = String(p[13] || '').replace(/\D/g, '').slice(0, 7);
			if (!munNu || lat === null || lng === null) continue;
			let b = mun.get(munNu);
			if (!b) {
				b = { n: 0, latMin: lat, latMax: lat, lngMin: lng, lngMax: lng, sLat: 0, sLng: 0 };
				mun.set(munNu, b);
			}
			b.n++;
			b.sLat += lat;
			b.sLng += lng;
			if (lat < b.latMin) b.latMin = lat;
			if (lat > b.latMax) b.latMax = lat;
			if (lng < b.lngMin) b.lngMin = lng;
			if (lng > b.lngMax) b.lngMax = lng;
		}
	}
	const dil = 0.02; // ~2 km
	for (const b of mun.values()) {
		b.latMin -= dil;
		b.latMax += dil;
		b.lngMin -= dil;
		b.lngMax += dil;
		b.lat = b.sLat / b.n;
		b.lng = b.sLng / b.n;
	}
	return mun;
}

/** Agrupa por gap em consultado_em (ordenando por tempo). */
function detectBuckets(rows, gapSec) {
	const sorted = [...rows].sort((a, b) => {
		const ta = Date.parse(a.consultado_em || 0) || 0;
		const tb = Date.parse(b.consultado_em || 0) || 0;
		return ta - tb || a.cep.localeCompare(b.cep);
	});
	const buckets = [];
	let cur = null;
	for (const r of sorted) {
		const t = Date.parse(r.consultado_em || 0) || 0;
		if (!cur) {
			cur = { rows: [r], t0: t, t1: t };
			continue;
		}
		if (t - cur.t1 > gapSec * 1000) {
			buckets.push(cur);
			cur = { rows: [r], t0: t, t1: t };
		} else {
			cur.rows.push(r);
			if (t > cur.t1) cur.t1 = t;
			if (t && t < cur.t0) cur.t0 = t;
		}
	}
	if (cur) buckets.push(cur);
	return buckets;
}

function analyzeBucket(rows, mun) {
	const byStatus = {};
	const byCity = {};
	const byUf = {};
	const coordKey = new Map();
	const cepByCoord = new Map();
	let withCoords = 0;
	let inBbox = 0;
	let outBbox = 0;
	let noBbox = 0;
	let brazilCenter = 0;
	const distToCenter = [];
	const outExamples = [];
	const brazilExamples = [];

	for (const r of rows) {
		byStatus[r.status] = (byStatus[r.status] || 0) + 1;
		const city = r.api_city || '?';
		byCity[city] = (byCity[city] || 0) + 1;
		const uf = r.api_state || '?';
		byUf[uf] = (byUf[uf] || 0) + 1;

		const lat = r.lat;
		const lng = r.lng;
		if (lat == null || lng == null || !Number.isFinite(lat) || !Number.isFinite(lng)) continue;
		withCoords++;

		const k = `${lat.toFixed(4)},${lng.toFixed(4)}`;
		coordKey.set(k, (coordKey.get(k) || 0) + 1);
		if (!cepByCoord.has(k)) cepByCoord.set(k, []);
		cepByCoord.get(k).push(r.cep);

		if (isBrazilCenter(lat, lng)) {
			brazilCenter++;
			if (brazilExamples.length < 10) {
				brazilExamples.push({
					cep: r.cep,
					api_city: r.api_city,
					api_ibge: r.api_ibge,
					lat,
					lng,
				});
			}
		}

		const ibge = String(r.api_ibge || '').replace(/\D/g, '').slice(0, 7);
		const b = ibge ? mun.get(ibge) : null;
		if (!b || b.n < 5) {
			noBbox++;
			continue;
		}
		const inside = lat >= b.latMin && lat <= b.latMax && lng >= b.lngMin && lng <= b.lngMax;
		const d = distKm(lat, lng, b.lat, b.lng);
		distToCenter.push(d);
		if (inside) {
			inBbox++;
		} else {
			outBbox++;
			if (outExamples.length < 15) {
				outExamples.push({
					cep: r.cep,
					api_city: r.api_city,
					api_ibge: ibge,
					lat,
					lng,
					mun_center: [Math.round(b.lat * 1e6) / 1e6, Math.round(b.lng * 1e6) / 1e6],
					dist_km: Math.round(d * 10) / 10,
				});
			}
		}
	}

	const shared = [...coordKey.entries()].filter(([, c]) => c > 1).sort((a, b) => b[1] - a[1]);
	const multi5 = shared.filter(([, c]) => c >= 5);
	distToCenter.sort((a, b) => a - b);

	const topCities = Object.entries(byCity).sort((a, b) => b[1] - a[1]).slice(0, 15);

	return {
		n: rows.length,
		with_coords: withCoords,
		by_status: byStatus,
		by_uf: byUf,
		top_cities: topCities,
		unique_coord_4dp: coordKey.size,
		coords_shared_by_2plus: shared.length,
		coords_shared_by_5plus: multi5.length,
		top_shared_coords: multi5.slice(0, 10).map(([k, c]) => ({
			coord: k,
			n_ceps: c,
			sample_ceps: cepByCoord.get(k).slice(0, 5),
		})),
		mun_bbox: {
			in: inBbox,
			out: outBbox,
			no_bbox_ref: noBbox,
			pct_in: inBbox + outBbox
				? Math.round((1000 * inBbox) / (inBbox + outBbox)) / 10
				: null,
		},
		brazil_center_hits: brazilCenter,
		brazil_center_examples: brazilExamples,
		dist_to_mun_center_km: {
			p50: pct(distToCenter, 0.5),
			p75: pct(distToCenter, 0.75),
			p90: pct(distToCenter, 0.9),
			p99: pct(distToCenter, 0.99),
			max: distToCenter.length
				? Math.round(distToCenter[distToCenter.length - 1] * 10) / 10
				: null,
		},
		out_of_bbox_examples: outExamples,
	};
}

function isoShort(ms) {
	if (!ms) return '?';
	return new Date(ms).toISOString().replace(/\.\d{3}Z$/, 'Z');
}

function fileStamp(ms) {
	const d = new Date(ms || Date.now());
	const p = (n) => String(n).padStart(2, '0');
	return `${d.getUTCFullYear()}${p(d.getUTCMonth() + 1)}${p(d.getUTCDate())}-${p(d.getUTCHours())}${p(d.getUTCMinutes())}`;
}

function renderMarkdown(meta, report) {
	const lines = [];
	lines.push(`# Qualidade CEP externo — ${meta.id}`);
	lines.push('');
	lines.push(`| | |`);
	lines.push(`|--|--|`);
	lines.push(`| **Bucket** | ${meta.id} |`);
	lines.push(`| **CEPs** | ${report.n} |`);
	lines.push(`| **Período (UTC)** | ${isoShort(meta.t0)} → ${isoShort(meta.t1)} |`);
	lines.push(`| **Cache** | \`${meta.cache}\` |`);
	lines.push(`| **Gerado em** | ${new Date().toISOString()} |`);
	lines.push('');
	lines.push('## Status da API');
	lines.push('');
	lines.push('| status | n |');
	lines.push('|--------|--:|');
	for (const [k, v] of Object.entries(report.by_status).sort((a, b) => b[1] - a[1])) {
		lines.push(`| \`${k}\` | ${v} |`);
	}
	lines.push('');
	lines.push(`Com coordenadas numéricas: **${report.with_coords}** / ${report.n}`);
	lines.push('');
	lines.push('## Composição');
	lines.push('');
	lines.push('### Por UF (API)');
	lines.push('');
	lines.push('| UF | n |');
	lines.push('|----|--:|');
	for (const [k, v] of Object.entries(report.by_uf).sort((a, b) => b[1] - a[1])) {
		lines.push(`| ${k} | ${v} |`);
	}
	lines.push('');
	lines.push('### Top cidades (API)');
	lines.push('');
	lines.push('| Cidade | n |');
	lines.push('|--------|--:|');
	for (const [c, n] of report.top_cities) {
		lines.push(`| ${c} | ${n} |`);
	}
	lines.push('');
	lines.push('## Qualidade da coordenada');
	lines.push('');
	lines.push('Validação contra bbox municipal das vias `ok` do join DNE↔OSM (±~2 km de dilatação),');
	lines.push('cruzando `api_ibge` do cache com o município.');
	lines.push('');
	lines.push('| Check | Valor |');
	lines.push('|-------|------:|');
	lines.push(`| Dentro do bbox do município | **${report.mun_bbox.in}** / ${report.mun_bbox.in + report.mun_bbox.out} (**${report.mun_bbox.pct_in ?? '—'}%**) |`);
	lines.push(`| Fora do bbox | ${report.mun_bbox.out} |`);
	lines.push(`| Sem referência de bbox (IBGE ralo) | ${report.mun_bbox.no_bbox_ref} |`);
	lines.push(`| Coordenadas únicas (4 casas) | ${report.unique_coord_4dp} / ${report.with_coords} |`);
	lines.push(`| Mesmo ponto para ≥2 CEPs | ${report.coords_shared_by_2plus} |`);
	lines.push(`| Mesmo ponto para ≥5 CEPs | ${report.coords_shared_by_5plus} |`);
	lines.push(`| **Centro do Brasil** (−14,235 / −51,925) | **${report.brazil_center_hits}** |`);
	lines.push(`| Dist. ao centroide mun. — mediana | ${report.dist_to_mun_center_km.p50 ?? '—'} km |`);
	lines.push(`| p75 | ${report.dist_to_mun_center_km.p75 ?? '—'} km |`);
	lines.push(`| p90 | ${report.dist_to_mun_center_km.p90 ?? '—'} km |`);
	lines.push(`| p99 | ${report.dist_to_mun_center_km.p99 ?? '—'} km |`);
	lines.push(`| máx | ${report.dist_to_mun_center_km.max ?? '—'} km |`);
	lines.push('');

	if (report.brazil_center_hits > 0) {
		lines.push('### Hits no centro do Brasil (fallback inválido)');
		lines.push('');
		lines.push('O footprint municipal do join **deve rejeitar** estes pontos.');
		lines.push('');
		lines.push('| CEP | cidade API | IBGE | lat | lng |');
		lines.push('|-----|------------|------|-----|-----|');
		for (const e of report.brazil_center_examples) {
			lines.push(`| ${e.cep} | ${e.api_city} | ${e.api_ibge} | ${e.lat} | ${e.lng} |`);
		}
		lines.push('');
	}

	if (report.top_shared_coords.length) {
		lines.push('### Clusters de CEPs no mesmo ponto (≥5)');
		lines.push('');
		lines.push('| Coordenada (4dp) | n CEPs | exemplos |');
		lines.push('|------------------|-------:|----------|');
		for (const s of report.top_shared_coords) {
			lines.push(`| \`${s.coord}\` | ${s.n_ceps} | ${s.sample_ceps.join(', ')} |`);
		}
		lines.push('');
	}

	if (report.out_of_bbox_examples.length) {
		lines.push('### Exemplos fora do bbox municipal');
		lines.push('');
		lines.push('| CEP | cidade | IBGE | dist. centro (km) | lat | lng |');
		lines.push('|-----|--------|------|------------------:|-----|-----|');
		for (const e of report.out_of_bbox_examples) {
			lines.push(
				`| ${e.cep} | ${e.api_city} | ${e.api_ibge} | ${e.dist_km} | ${e.lat} | ${e.lng} |`
			);
		}
		lines.push('');
	}

	lines.push('## Leitura');
	lines.push('');
	const pctIn = report.mun_bbox.pct_in;
	if (pctIn != null && pctIn >= 99 && report.brazil_center_hits <= Math.max(2, report.n * 0.005)) {
		lines.push(
			`- Município/IBGE coerente na grande maioria; **${report.brazil_center_hits}** ponto(s) no centro do Brasil — gate de footprint obrigatório.`
		);
	} else if (pctIn != null) {
		lines.push(
			`- **${pctIn}%** dentro do bbox municipal; revisar cauda (p99=${report.dist_to_mun_center_km.p99} km, máx=${report.dist_to_mun_center_km.max} km).`
		);
	}
	lines.push(
		`- Colapso de CEPs no mesmo ponto indica precisão de **CEP/bairro**, não de via — usar ` +
			'`geo_regra=cep_externo` com **bbox vazia**.'
	);
	lines.push('');
	return lines.join('\n');
}

async function main() {
	const opts = parseArgs(process.argv.slice(2));
	console.error(`Cache: ${opts.cache}`);
	console.error(`DNE:   ${opts.dne}`);
	console.error(`Out:   ${opts.out}`);

	const cache = await cepExt.loadCache(opts.cache);
	console.error(`CEPs no cache: ${cache.size}`);
	if (cache.size === 0) {
		console.error('Cache vazio — nada a analisar.');
		process.exit(1);
	}

	console.error('Carregando bbox municipal (linhas ok do join)…');
	const mun = await loadMunBboxFromOk(opts.dne, opts.ufs);
	console.error(`Municípios com bbox: ${mun.size}`);

	fs.mkdirSync(opts.out, { recursive: true });

	let cepsList = opts.ceps;
	if (opts.cepsFile) {
		const raw = fs.readFileSync(opts.cepsFile, 'utf8');
		cepsList = raw.split(/\r?\n/).map((c) => cepExt.digitsCep(c.trim())).filter(Boolean);
	}

	let buckets;
	if (cepsList && cepsList.length) {
		const rows = cepsList.map((c) => cache.get(c)).filter(Boolean);
		if (!rows.length) {
			console.error('Nenhum CEP da lista encontrado no cache.');
			process.exit(1);
		}
		const t0 = Math.min(...rows.map((r) => Date.parse(r.consultado_em) || Date.now()));
		const t1 = Math.max(...rows.map((r) => Date.parse(r.consultado_em) || Date.now()));
		buckets = [{ rows, t0, t1, forcedLabel: opts.label }];
	} else {
		buckets = detectBuckets([...cache.values()], opts.gapSec);
		if (opts.last) buckets = buckets.slice(-1);
	}

	console.error(`Buckets: ${buckets.length}`);

	const indexRows = [];
	const reports = [];

	for (let i = 0; i < buckets.length; i++) {
		const b = buckets[i];
		const id = b.forcedLabel || `bucket-${String(i + 1).padStart(3, '0')}-${fileStamp(b.t0)}`;
		const report = analyzeBucket(b.rows, mun);
		const meta = { id, t0: b.t0, t1: b.t1, cache: opts.cache, n: b.rows.length };
		const md = renderMarkdown(meta, report);
		const mdPath = path.join(opts.out, `${id}.md`);
		const jsonPath = path.join(opts.out, `${id}.json`);
		fs.writeFileSync(mdPath, md, 'utf8');
		fs.writeFileSync(jsonPath, JSON.stringify({ meta, report }, null, 2), 'utf8');
		console.error(`  ${id}: n=${report.n} bbox_in=${report.mun_bbox.pct_in}% br_center=${report.brazil_center_hits} → ${mdPath}`);
		indexRows.push({ id, n: report.n, pct_in: report.mun_bbox.pct_in, br: report.brazil_center_hits, t0: b.t0, t1: b.t1 });
		reports.push({ meta, report });
	}

	// índice
	const idx = [];
	idx.push('# Qualidade CEP externo — índice de buckets');
	idx.push('');
	idx.push(`Cache: \`${opts.cache}\` · gerado ${new Date().toISOString()}`);
	idx.push('');
	idx.push('| Bucket | CEPs | % no bbox mun. | Centro BR | Período UTC |');
	idx.push('|--------|-----:|---------------:|----------:|-------------|');
	for (const r of indexRows) {
		idx.push(
			`| [${r.id}](./${r.id}.md) | ${r.n} | ${r.pct_in ?? '—'} | ${r.br} | ${isoShort(r.t0)} |`
		);
	}
	idx.push('');
	idx.push(`**Total no cache analisado:** ${indexRows.reduce((s, r) => s + r.n, 0)} CEPs em ${indexRows.length} bucket(s).`);
	idx.push('');
	const idxPath = path.join(opts.out, 'README.md');
	fs.writeFileSync(idxPath, idx.join('\n'), 'utf8');
	console.error(`Índice: ${idxPath}`);

	// também espelha no repo (docs) se out for em G:
	const repoOut = path.join(ROOT, 'docs', 'geo', 'cep-externo-qualidade');
	if (path.resolve(opts.out) !== path.resolve(repoOut)) {
		fs.mkdirSync(repoOut, { recursive: true });
		for (const f of fs.readdirSync(opts.out)) {
			if (f.endsWith('.md') || f.endsWith('.json')) {
				fs.copyFileSync(path.join(opts.out, f), path.join(repoOut, f));
			}
		}
		console.error(`Espelho: ${repoOut}`);
	}

	console.log(JSON.stringify({ buckets: indexRows.length, out: opts.out }));
}

main().catch((e) => {
	console.error(e);
	process.exit(1);
});
