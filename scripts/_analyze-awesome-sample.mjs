/**
 * Pós-análise da amostra AwesomeAPI: qualidade da coordenada além de city/IBGE match.
 */
import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { fileURLToPath } from 'url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const sampleDir = path.join(ROOT, '_ignore', 'awesomeapi-sample');
const dneDir = process.argv[2] || 'G:\\dne-geo-local';

function num(v) {
	const n = Number(v);
	return Number.isFinite(n) ? n : null;
}

function distKm(lat1, lng1, lat2, lng2) {
	const dLat = (lat1 - lat2) * 111;
	const dLng = (lng1 - lng2) * 111 * Math.cos(((lat1 + lat2) / 2) * Math.PI / 180);
	return Math.sqrt(dLat * dLat + dLng * dLng);
}

async function loadMunBboxFromOk(dir, ufs) {
	// mun_nu -> bbox from ok lines
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
			const lat = num(p[14]), lng = num(p[15]);
			const munNu = p[13];
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
	// dilate ~2 km (~0.018°)
	const dil = 0.02;
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

const results = fs.readFileSync(path.join(sampleDir, 'results.jsonl'), 'utf8')
	.split(/\r?\n/).filter(Boolean).map((l) => JSON.parse(l));

console.error('Carregando bbox municipal a partir de linhas ok…');
const mun = await loadMunBboxFromOk(dneDir, ['SP', 'RJ', 'MG', 'ES']);
console.error(`Municípios com bbox: ${mun.size}`);

// coord uniqueness
const coordKey = new Map(); // "lat,lng" rounded 4dp -> count
const cepByCoord = new Map();
for (const r of results) {
	const lat = num(r.lat), lng = num(r.lng);
	if (lat === null) continue;
	const k = `${lat.toFixed(4)},${lng.toFixed(4)}`;
	coordKey.set(k, (coordKey.get(k) || 0) + 1);
	if (!cepByCoord.has(k)) cepByCoord.set(k, []);
	cepByCoord.get(k).push(r.cep);
}

const sharedCoords = [...coordKey.entries()].filter(([, c]) => c > 1).sort((a, b) => b[1] - a[1]);
const multiCepSamePoint = sharedCoords.filter(([, c]) => c >= 5);

// inside mun bbox
let inBbox = 0, outBbox = 0, noBbox = 0;
const outExamples = [];
const distToCenter = [];
for (const r of results) {
	const lat = num(r.lat), lng = num(r.lng);
	if (lat === null) continue;
	const b = mun.get(String(r.mun_nu));
	if (!b || b.n < 5) {
		noBbox++;
		continue;
	}
	const inside = lat >= b.latMin && lat <= b.latMax && lng >= b.lngMin && lng <= b.lngMax;
	if (inside) inBbox++;
	else {
		outBbox++;
		const d = distKm(lat, lng, b.lat, b.lng);
		distToCenter.push(d);
		if (outExamples.length < 15) {
			outExamples.push({
				cep: r.cep,
				loc_no: r.loc_no,
				log_no: r.log_nu,
				api_city: r.api_city,
				lat, lng,
				mun_center: [b.lat, b.lng],
				dist_km: Math.round(d * 10) / 10,
				mun_nu: r.mun_nu,
			});
		}
	}
	if (inside) distToCenter.push(distKm(lat, lng, b.lat, b.lng));
}

distToCenter.sort((a, b) => a - b);
function pct(arr, p) {
	if (!arr.length) return null;
	const i = Math.min(arr.length - 1, Math.floor(arr.length * p));
	return Math.round(arr[i] * 100) / 100;
}

// all-zero-ish or ocean
const suspicious = results.filter((r) => {
	const lat = num(r.lat), lng = num(r.lng);
	if (lat === null) return true;
	if (Math.abs(lat) < 0.01 && Math.abs(lng) < 0.01) return true;
	// outside Brazil rough bbox
	if (lat > 6 || lat < -34 || lng > -30 || lng < -75) return true;
	return false;
});

const report = {
	n: results.length,
	with_coords: results.filter((r) => num(r.lat) !== null).length,
	unique_coord_4dp: coordKey.size,
	coords_shared_by_2plus_ceps: sharedCoords.length,
	coords_shared_by_5plus_ceps: multiCepSamePoint.length,
	top_shared_coords: multiCepSamePoint.slice(0, 10).map(([k, c]) => ({
		coord: k,
		n_ceps: c,
		sample_ceps: cepByCoord.get(k).slice(0, 5),
	})),
	mun_bbox: {
		in: inBbox,
		out: outBbox,
		no_bbox_ref: noBbox,
		pct_in: inBbox + outBbox ? Math.round(1000 * inBbox / (inBbox + outBbox)) / 10 : null,
	},
	dist_to_mun_center_km: {
		p50: pct(distToCenter, 0.5),
		p75: pct(distToCenter, 0.75),
		p90: pct(distToCenter, 0.9),
		p99: pct(distToCenter, 0.99),
		max: distToCenter.length ? Math.round(distToCenter[distToCenter.length - 1] * 10) / 10 : null,
	},
	out_of_bbox_examples: outExamples,
	suspicious_coords: suspicious.length,
	suspicious_examples: suspicious.slice(0, 5),
};

fs.writeFileSync(path.join(sampleDir, 'quality.json'), JSON.stringify(report, null, 2), 'utf8');
console.log(JSON.stringify(report, null, 2));
