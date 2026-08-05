/**
 * Amostra CEPs sem coordenada no DNE_GEO e consulta AwesomeAPI.
 * Cache em `CEP_EXTERNO_{UF}.TXT` (um arquivo por UF). CEPs já no cache **não**
 * são reconsultados.
 *
 *   node scripts/sample-awesomeapi-cep.mjs
 *   node scripts/sample-awesomeapi-cep.mjs --dir=G:\dne-geo-br --n=1000
 *   node scripts/sample-awesomeapi-cep.mjs --dir=G:\dne-geo-br --ufs=SP,RJ --n=500
 *   node scripts/sample-awesomeapi-cep.mjs --cache=G:\legado\CEP_EXTERNO.TXT   # monólito
 *
 * Fontes em `--dir` (pasta do join):
 *   DNE_GEO_LOGRADOURO_{UF}.TXT  → candidatos sem geo
 *   CEP_EXTERNO_{UF}.TXT         → cache por UF (lê e grava)
 *
 * Chave: AWESOMEAPI_API_KEY em .env.local
 * Spec: docs/geo/cep-externo.md
 */
import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { spawnSync } from 'child_process';
import { createRequire } from 'module';
import { fileURLToPath } from 'url';

const require = createRequire(import.meta.url);
const cepExt = require('../cep-externo.js');

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const DEFAULT_DIR = 'G:\\dne-geo-br';

function loadEnvLocal() {
	const p = path.join(ROOT, '.env.local');
	if (!fs.existsSync(p)) throw new Error('Falta .env.local com AWESOMEAPI_API_KEY');
	const out = {};
	for (const line of fs.readFileSync(p, 'utf8').split(/\r?\n/)) {
		const m = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
		if (!m) continue;
		let v = m[2].trim();
		if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
			v = v.slice(1, -1);
		}
		out[m[1]] = v;
	}
	return out;
}

/** UFs com DNE_GEO_LOGRADOURO_{UF}.TXT na pasta. */
function detectUfsFromDir(dir) {
	if (!dir || !fs.existsSync(dir)) return [];
	return fs
		.readdirSync(dir)
		.map((n) => {
			const m = n.match(/^DNE_GEO_LOGRADOURO_([A-Za-z]{2})\.TXT$/i);
			return m ? m[1].toUpperCase() : null;
		})
		.filter(Boolean)
		.sort();
}

function parseArgs(argv) {
	const o = {
		dir: DEFAULT_DIR,
		n: 1000,
		ufs: null, // null → auto a partir de DNE_GEO_* no --dir
		concurrency: 4,
		/** null = cache multi-UF em --dir; string = monólito legado */
		cache: null,
		cacheDir: null,
		qualityOut: null,
		delayMs: 40,
		force: false,
		retryErrors: false,
		noQuality: false,
	};
	for (const a of argv) {
		if (a.startsWith('--dir=')) o.dir = a.slice(6);
		else if (a.startsWith('--n=')) o.n = Number(a.slice(4));
		else if (a.startsWith('--ufs=')) o.ufs = a.slice(6).split(',').map((s) => s.trim().toUpperCase()).filter(Boolean);
		else if (a.startsWith('--concurrency=')) o.concurrency = Number(a.slice(14));
		else if (a.startsWith('--cache=')) o.cache = a.slice(8);
		else if (a.startsWith('--cache-dir=')) o.cacheDir = a.slice(12);
		else if (a.startsWith('--quality-out=')) o.qualityOut = a.slice(14);
		else if (a.startsWith('--delay-ms=')) o.delayMs = Number(a.slice(11));
		else if (a === '--force') o.force = true;
		else if (a === '--retry-errors') o.retryErrors = true;
		else if (a === '--no-quality') o.noQuality = true;
	}
	if (!o.cacheDir) o.cacheDir = o.dir;
	if (!o.qualityOut) o.qualityOut = path.join(o.dir, 'qualidade');
	return o;
}

const CAPITAL_HINTS = new Set([
	'sao paulo', 'rio de janeiro', 'belo horizonte', 'vitoria',
	'campinas', 'santos', 'guarulhos', 'osasco', 'sao bernardo do campo',
	'santo andre', 'ribeirao preto', 'sorocaba', 'niteroi', 'duque de caxias',
	'nova iguacu', 'sao goncalo', 'uberlandia', 'juiz de fora', 'contagem',
	'betim', 'vila velha', 'serra', 'cariacica', 'curitiba', 'porto alegre',
	'florianopolis', 'salvador', 'recife', 'fortaleza', 'brasilia', 'manaus',
	'belem', 'goiania',
]);

function normName(s) {
	return String(s || '')
		.normalize('NFD')
		.replace(/[\u0300-\u036f]/g, '')
		.toLowerCase()
		.replace(/\s+/g, ' ')
		.trim();
}

function relevanceScore(row) {
	let s = 0;
	const city = normName(row.loc_no);
	if (CAPITAL_HINTS.has(city)) s += 100;
	if (city === 'sao paulo') s += 40;
	if (city === 'rio de janeiro') s += 30;
	if (city === 'belo horizonte') s += 25;
	if (row.geo_status === 'sem_nome_osm') s += 50;
	else if (row.geo_status === 'ambiguo') s += 20;
	const cep = row.cep;
	if (cep.startsWith('01') || cep.startsWith('02') || cep.startsWith('04') || cep.startsWith('05')) s += 15;
	if (cep.startsWith('20') || cep.startsWith('21') || cep.startsWith('22')) s += 12;
	if (cep.startsWith('30') || cep.startsWith('31')) s += 10;
	const tlo = normName(row.tlo);
	if (tlo === 'rua' || tlo === 'avenida') s += 8;
	if (tlo === 'praca' || tlo === 'alameda') s += 4;
	return s;
}

async function collectMissing(dir, ufs) {
	const rows = [];
	for (const uf of ufs) {
		const file = path.join(dir, `DNE_GEO_LOGRADOURO_${uf}.TXT`);
		if (!fs.existsSync(file)) {
			console.error(`aviso: ausente ${file}`);
			continue;
		}
		const rl = readline.createInterface({
			input: fs.createReadStream(file, { encoding: 'utf8' }),
			crlfDelay: Infinity,
		});
		let nUf = 0;
		for await (const line of rl) {
			if (!line) continue;
			const p = line.split('@');
			const status = p[20] || '';
			if (status === 'ok') continue;
			if (p[14]) continue;
			const cep = cepExt.digitsCep(p[7]);
			if (!cep) continue;
			const row = {
				log_nu: p[0],
				uf: p[1] || uf,
				loc_nu: p[2],
				log_no: p[5],
				cep,
				tlo: p[8] || '',
				loc_no: p[11] || '',
				bai_no: p[12] || '',
				mun_nu: p[13] || '',
				geo_status: status,
			};
			row.score = relevanceScore(row);
			rows.push(row);
			nUf++;
		}
		console.error(`  ${uf}: ${nUf} linhas sem geo com CEP (acum. ${rows.length})`);
	}
	return rows;
}

function sampleRelevant(rows, n) {
	rows.sort((a, b) => b.score - a.score || a.cep.localeCompare(b.cep));
	const byCep = new Map();
	for (const r of rows) {
		if (!byCep.has(r.cep)) byCep.set(r.cep, r);
	}
	const unique = [...byCep.values()].sort((a, b) => b.score - a.score);

	const buckets = { sp_cap: [], rj_cap: [], bh: [], other: [] };
	for (const r of unique) {
		const c = normName(r.loc_no);
		if (c === 'sao paulo') buckets.sp_cap.push(r);
		else if (c === 'rio de janeiro') buckets.rj_cap.push(r);
		else if (c === 'belo horizonte') buckets.bh.push(r);
		else buckets.other.push(r);
	}

	const targets = {
		sp_cap: Math.round(n * 0.45),
		rj_cap: Math.round(n * 0.15),
		bh: Math.round(n * 0.1),
		other: 0,
	};
	targets.other = n - targets.sp_cap - targets.rj_cap - targets.bh;

	const picked = [];
	const usedCep = new Set();
	function take(arr, count) {
		let got = 0;
		for (const r of arr) {
			if (got >= count) break;
			if (usedCep.has(r.cep)) continue;
			usedCep.add(r.cep);
			picked.push(r);
			got++;
		}
		return got;
	}
	for (const [k, t] of Object.entries(targets)) take(buckets[k], t);
	for (const r of unique) {
		if (picked.length >= n) break;
		if (usedCep.has(r.cep)) continue;
		usedCep.add(r.cep);
		picked.push(r);
	}
	return picked.slice(0, n);
}

function sleep(ms) {
	return new Promise((r) => setTimeout(r, ms));
}

async function fetchCep(cep, apiKey) {
	const url = `https://cep.awesomeapi.com.br/json/${cep}?token=${encodeURIComponent(apiKey)}`;
	const res = await fetch(url, {
		headers: { Accept: 'application/json', token: apiKey },
	});
	const text = await res.text();
	let body = null;
	try {
		body = JSON.parse(text);
	} catch {
		body = null;
	}
	return { http: res.status, body };
}

async function mapPool(items, concurrency, fn) {
	const results = new Array(items.length);
	let i = 0;
	async function worker() {
		while (i < items.length) {
			const idx = i++;
			results[idx] = await fn(items[idx], idx);
		}
	}
	await Promise.all(Array.from({ length: concurrency }, () => worker()));
	return results;
}

function cacheHitPolicy(row, opts) {
	if (opts.force) return false; // miss → reconsultar
	if (!row) return false;
	if (opts.retryErrors && row.status === 'error') return false;
	return true; // hit → não reconsultar
}

/**
 * Carrega cache: monólito (--cache=arquivo) ou multi-UF no cacheDir.
 * Também inclui monólito CEP_EXTERNO.TXT no dir se ainda existir.
 */
async function loadCaches(opts) {
	if (opts.cache) {
		const map = await cepExt.loadCache(opts.cache);
		return { map, mode: 'file', paths: [opts.cache] };
	}
	const listed = cepExt.listCacheFiles(opts.cacheDir, opts.ufs);
	const map = await cepExt.loadCacheMulti(opts.cacheDir, { ufs: opts.ufs });
	return {
		map,
		mode: 'dir',
		paths: listed.map((f) => f.path),
		files: listed,
	};
}

async function main() {
	const opts = parseArgs(process.argv.slice(2));
	const env = loadEnvLocal();
	const apiKey = env.AWESOMEAPI_API_KEY;
	if (!apiKey) throw new Error('AWESOMEAPI_API_KEY vazio no .env.local');

	if (!opts.ufs || !opts.ufs.length) {
		opts.ufs = detectUfsFromDir(opts.dir);
		if (!opts.ufs.length) {
			throw new Error(
				`Nenhuma DNE_GEO_LOGRADOURO_*.TXT em ${opts.dir}. ` +
				`Rode o join ou passe --ufs=SP,RJ`
			);
		}
	}

	console.error(`Dir:       ${opts.dir}  (DNE_GEO_LOGRADOURO_*)`);
	console.error(`Cache:     ${opts.cache || opts.cacheDir + '\\CEP_EXTERNO_{UF}.TXT'}`);
	console.error(`Qualidade: ${opts.qualityOut}`);
	console.error(`Amostra alvo: ${opts.n} CEPs novos | UFs: ${opts.ufs.join(',')}`);

	const loaded = await loadCaches(opts);
	const cache = loaded.map;
	console.error(
		`Cache carregado: ${cache.size} CEPs` +
		(loaded.mode === 'dir'
			? ` (${loaded.paths.length} arquivos)`
			: ` (monólito)`)
	);
	if (loaded.mode === 'dir' && loaded.paths.length === 0) {
		console.error('  (nenhum CEP_EXTERNO_*.TXT ainda — cache vazio, ok)');
	}

	console.error('Coletando linhas sem coordenada…');
	const missing = await collectMissing(opts.dir, opts.ufs);
	console.error(`Total sem geo com CEP (linhas): ${missing.length}`);

	// candidatos únicos por CEP, sem os já cacheados
	const byCep = new Map();
	for (const r of missing) {
		if (!byCep.has(r.cep)) byCep.set(r.cep, r);
	}
	let candidates = [...byCep.values()];
	const before = candidates.length;
	candidates = candidates.filter((r) => !cacheHitPolicy(cache.get(r.cep), opts));
	console.error(
		`CEPs únicos sem geo: ${before} | já no cache (pulados): ${before - candidates.length} | elegíveis: ${candidates.length}`
	);

	if (candidates.length === 0) {
		console.error('Nada a consultar — cache já cobre os CEPs faltantes elegíveis.');
		const report = {
			cache_size: cache.size,
			eligible: 0,
			fetched: 0,
			skipped_cached: before,
			dir: opts.dir,
			ufs: opts.ufs,
		};
		console.log(JSON.stringify(report));
		return;
	}

	const sample = sampleRelevant(candidates, opts.n);
	console.error(`A consultar agora: ${sample.length}`);

	const byStatus = {};
	const byCity = {};
	for (const r of sample) {
		byStatus[r.geo_status] = (byStatus[r.geo_status] || 0) + 1;
		byCity[r.loc_no || '?'] = (byCity[r.loc_no || '?'] || 0) + 1;
	}
	console.error('Por status:', byStatus);
	console.error(
		'Top cidades:',
		Object.fromEntries(Object.entries(byCity).sort((a, b) => b[1] - a[1]).slice(0, 12))
	);

	console.error('Consultando API…');
	const t0 = Date.now();
	let done = 0;
	const fetched = await mapPool(sample, opts.concurrency, async (row) => {
		try {
			if (opts.delayMs > 0) await sleep(opts.delayMs);
			const { http, body } = await fetchCep(row.cep, apiKey);
			const rec = cepExt.fromAwesomeResponse(row.cep, http, body);
			// se API não trouxe UF, herda a do DNE (candidatos do collect)
			if (!rec.api_state && row.uf) rec.api_state = row.uf;
			done++;
			if (done % 50 === 0 || done === sample.length) {
				const elapsed = (Date.now() - t0) / 1000;
				process.stderr.write(`\r  ${done}/${sample.length} (${(done / Math.max(elapsed, 0.001)).toFixed(1)}/s)   `);
			}
			return rec;
		} catch (e) {
			done++;
			const rec = cepExt.fromAwesomeResponse(row.cep, 0, null);
			if (row.uf) rec.api_state = row.uf;
			return rec;
		}
	});
	process.stderr.write('\n');

	let saveInfo;
	if (opts.cache) {
		cepExt.mergeAndSave(opts.cache, cache, fetched);
		saveInfo = { mode: 'file', path: opts.cache, size: cache.size };
	} else {
		const r = cepExt.mergeAndSaveByUf(opts.cacheDir, cache, fetched);
		saveInfo = { mode: 'by_uf', ufs: r.ufs, files: r.files, size: r.size };
		console.error(`Cache gravado por UF: ${r.ufs.join(',')}`);
	}

	const summary = {
		fetched: fetched.length,
		cache_size_after: cache.size,
		skipped_cached: before - candidates.length,
		ok: fetched.filter((r) => r.status === 'ok').length,
		not_found: fetched.filter((r) => r.status === 'not_found').length,
		invalid: fetched.filter((r) => r.status === 'invalid').length,
		error: fetched.filter((r) => r.status === 'error').length,
		empty_coords: fetched.filter((r) => r.status === 'empty_coords').length,
		elapsed_sec: Math.round(((Date.now() - t0) / 1000) * 10) / 10,
		dir: opts.dir,
		ufs: opts.ufs,
		cache_save: saveInfo,
	};
	const repPath = path.join(opts.dir, 'CEP_EXTERNO_RELATORIO.json');
	fs.writeFileSync(
		repPath,
		JSON.stringify({ summary, byStatus, sample_size: sample.length, at: new Date().toISOString() }, null, 2),
		'utf8'
	);

	// Qualidade do bucket que acabou de ser buscado
	if (!opts.noQuality && fetched.length > 0) {
		const stamp = new Date().toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z').slice(0, 15);
		const label = `bucket-${stamp}-n${fetched.length}`;
		const listPath = path.join(opts.qualityOut, `${label}.ceps.txt`);
		fs.mkdirSync(opts.qualityOut, { recursive: true });
		fs.writeFileSync(listPath, fetched.map((r) => r.cep).join('\n') + '\n', 'utf8');
		console.error(`Gerando relatório de qualidade (${label})…`);
		const qArgs = [
			path.join(ROOT, 'scripts', 'cep-externo-quality.mjs'),
			`--dne=${opts.dir}`,
			`--out=${opts.qualityOut}`,
			`--ceps-file=${listPath}`,
			`--label=${label}`,
			`--ufs=${opts.ufs.join(',')}`,
		];
		if (opts.cache) qArgs.push(`--cache=${opts.cache}`);
		else qArgs.push(`--cache-dir=${opts.cacheDir}`);
		const q = spawnSync(process.execPath, qArgs, {
			encoding: 'utf8',
			maxBuffer: 20 * 1024 * 1024,
		});
		if (q.stderr) process.stderr.write(q.stderr);
		if (q.status !== 0) {
			console.error('aviso: falha ao gerar qualidade (cache ok).', q.error || q.status);
		} else {
			summary.quality_label = label;
			summary.quality_out = opts.qualityOut;
		}
	}

	console.error('\n=== RESUMO ===');
	console.error(JSON.stringify(summary, null, 2));
	console.log(JSON.stringify(summary));
}

main().catch((e) => {
	console.error(e);
	process.exit(1);
});
