/**
 * Parte CEP_EXTERNO.TXT (monólito) em CEP_EXTERNO_{UF}.TXT por api_state.
 *
 *   node scripts/split-cep-externo-by-uf.mjs --in=G:\dne-geo-br\CEP_EXTERNO.TXT
 *   node scripts/split-cep-externo-by-uf.mjs --in=G:\dne-geo-br\CEP_EXTERNO.TXT --out=G:\dne-geo-br --remove-source
 */
import path from 'path';
import { createRequire } from 'module';
import { fileURLToPath } from 'url';

const require = createRequire(import.meta.url);
const cepExt = require('../cep-externo.js');

function parseArgs(argv) {
	const o = { in: null, out: null, removeSource: false };
	for (const a of argv) {
		if (a.startsWith('--in=')) o.in = a.slice(5);
		else if (a.startsWith('--out=')) o.out = a.slice(6);
		else if (a === '--remove-source') o.removeSource = true;
	}
	return o;
}

const opts = parseArgs(process.argv.slice(2));
if (!opts.in) {
	console.error(
		'uso: node scripts/split-cep-externo-by-uf.mjs --in=CEP_EXTERNO.TXT [--out=DIR] [--remove-source]'
	);
	process.exit(1);
}
const outDir = opts.out || path.dirname(path.resolve(opts.in));

const r = await cepExt.splitCacheByUf(opts.in, outDir, {
	removeSource: opts.removeSource,
});

console.error(`Total CEPs: ${r.total}`);
console.error(`Arquivos: ${r.files.length} em ${outDir}`);
const sorted = Object.entries(r.byUf).sort((a, b) => b[1] - a[1]);
for (const [uf, n] of sorted) {
	console.error(`  ${uf}: ${n}`);
}
console.log(JSON.stringify({ total: r.total, byUf: r.byUf, out: outDir }));
