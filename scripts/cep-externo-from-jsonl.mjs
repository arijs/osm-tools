/**
 * Converte results.jsonl da amostra antiga → CEP_EXTERNO.TXT (formato DNE).
 *
 *   node scripts/cep-externo-from-jsonl.mjs \
 *     --in=_ignore/awesomeapi-sample/results.jsonl \
 *     --out=G:\dne-geo-local\CEP_EXTERNO.TXT
 */
import fs from 'fs';
import path from 'path';
import { createRequire } from 'module';
import { fileURLToPath } from 'url';

const require = createRequire(import.meta.url);
const cepExt = require('../cep-externo.js');
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

function parseArgs(argv) {
	const o = {
		in: path.join(ROOT, '_ignore', 'awesomeapi-sample', 'results.jsonl'),
		out: path.join(ROOT, '_ignore', 'cep-externo', 'CEP_EXTERNO.TXT'),
	};
	for (const a of argv) {
		if (a.startsWith('--in=')) o.in = a.slice(5);
		else if (a.startsWith('--out=')) o.out = a.slice(6);
	}
	return o;
}

function main() {
	const opts = parseArgs(process.argv.slice(2));
	if (!fs.existsSync(opts.in)) {
		console.error('Ausente:', opts.in);
		process.exit(1);
	}
	const map = new Map();
	const lines = fs.readFileSync(opts.in, 'utf8').split(/\r?\n/).filter(Boolean);
	let n = 0;
	for (const line of lines) {
		const j = JSON.parse(line);
		const cep = cepExt.digitsCep(j.cep);
		if (!cep) continue;
		// se o CEP já existe, mantém o primeiro (ou prefere ok)
		const prev = map.get(cep);
		const http = j.http != null ? j.http : 0;
		const row = cepExt.fromAwesomeResponse(cep, http, {
			city: j.api_city,
			state: j.sample?.uf || j.uf,
			district: j.api_district,
			city_ibge: j.api_ibge,
			address: j.api_address,
			address_type: j.api_address_type,
			lat: j.lat,
			lng: j.lng,
		});
		// fromAwesomeResponse com body parcial: se lat veio no topo do jsonl
		if (j.lat != null && j.lng != null && row.status !== 'ok') {
			const lat = Number(j.lat);
			const lng = Number(j.lng);
			if (isFinite(lat) && isFinite(lng)) {
				row.lat = lat;
				row.lng = lng;
				row.status = 'ok';
				row.http_status = http || 200;
			}
		}
		if (j.api_city) row.api_city = j.api_city;
		if (j.api_ibge) row.api_ibge = String(j.api_ibge);
		if (j.api_district) row.api_district = j.api_district;
		if (j.uf || j.sample?.uf) row.api_state = j.uf || j.sample.uf;
		row.consultado_em = row.consultado_em || new Date().toISOString();
		row._dirty = true;
		if (!prev || (prev.status !== 'ok' && row.status === 'ok')) {
			map.set(cep, row);
			n++;
		}
	}
	cepExt.writeCache(opts.out, map);
	console.error(`CEPs gravados: ${map.size} (de ${lines.length} linhas jsonl) → ${opts.out}`);
}

main();
