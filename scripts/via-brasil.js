#!/usr/bin/env node
'use strict';

/**
 * Roda `dne-via-cruzamentos.js` nas 27 UFs (pontos densificados + ligações).
 * Ctrl+C interrompe a UF atual; a próxima execução pula quem já tem
 * `DNE_GEO_VIA_RELATORIO_{UF}.json` — o próprio relatório é o marcador
 * de concluído (ele só é escrito no fim).
 *
 *   node scripts/via-brasil.js
 *   node scripts/via-brasil.js --only=sp,rj
 *   node scripts/via-brasil.js --list
 *   node scripts/via-brasil.js --force
 *
 * Flags extras (`--cell=`, `--max-seg-km=`, …) passam para o cruzamentos.
 */

var fs = require('fs');
var path = require('path');
var { spawn } = require('child_process');
var jb = require('./join-brasil');

var ROOT = path.resolve(__dirname, '..');
var VIA = path.join(ROOT, 'scripts', 'dne-via-cruzamentos.js');

function parseCli(argv) {
	var o = {
		dneGeo: 'G:\\dne-geo-br-join5',
		geom: 'G:\\osm-geo-br-uf',
		outDir: 'G:\\dne-geo-via-br',
		only: null,
		force: false,
		dryRun: false,
		list: false,
		help: false,
		extra: []
	};
	for (var i = 0; i < argv.length; i++) {
		var a = argv[i];
		if (a.indexOf('--dne-geo=') === 0) o.dneGeo = a.slice(10);
		else if (a.indexOf('--geom=') === 0) o.geom = a.slice(7);
		else if (a.indexOf('--out=') === 0) o.outDir = a.slice(6);
		else if (a.indexOf('--only=') === 0) o.only = a.slice(7);
		else if (a === '--force') o.force = true;
		else if (a === '--dry-run') o.dryRun = true;
		else if (a === '--list') o.list = true;
		else if (a === '--help' || a === '-h') o.help = true;
		else if (a.indexOf('--') === 0) o.extra.push(a);
		else {
			o.help = true;
			o.unknown = a;
		}
	}
	return o;
}

function usage() {
	return [
		'Uso: node scripts/via-brasil.js [opções]',
		'',
		'  --dne-geo=DIR  join (DNE_GEO_LOGRADOURO_{UF}.TXT, 26 cols)  [G:\\dne-geo-br-join5]',
		'  --geom=DIR     OSM_LOGRADOURO_GEOM_{UF} das 27 UFs         [G:\\osm-geo-br-uf]',
		'  --out=DIR      saída DNE_GEO_VIA_*                           [G:\\dne-geo-via-br]',
		'  --only=sp,rj   subset (UF, região: sul, sudeste, norte, …)',
		'  --force        refaz UFs que já têm relatório',
		'  --list         status sem rodar',
		'  --dry-run      imprime comandos',
		'',
		'Demais flags vão para dne-via-cruzamentos.js (--cell=, --max-seg-km=, --touch-km=).',
		'Ctrl+C: a UF em curso para; rode de novo para continuar.'
	].join('\n');
}

function reportPath(outDir, uf) {
	return path.join(outDir, 'DNE_GEO_VIA_RELATORIO_' + uf + '.json');
}

/** Cruzamentos dessa UF terminaram: relatório com finished_at no disco. */
function jobDone(outDir, uf) {
	var p = reportPath(outDir, uf);
	if (!fs.existsSync(p)) return false;
	try {
		var j = JSON.parse(fs.readFileSync(p, 'utf8'));
		return !!(j && j.finished_at);
	} catch (e) {
		return false;
	}
}

function buildArgs(opts, uf) {
	return [
		VIA,
		'--dne-geo=' + opts.dneGeo,
		'--geom=' + opts.geom,
		'--out=' + opts.outDir,
		'--uf=' + uf
	].concat(opts.extra || []);
}

function statusLine(outDir, uf) {
	if (!jobDone(outDir, uf)) return uf + '  -';
	var j = JSON.parse(fs.readFileSync(reportPath(outDir, uf), 'utf8'));
	var p = j.pontos || {};
	return uf + '  done  pontos=' + (p.nPontoRows || 0) + ' ligacoes=' + (p.nLigacaoRows || 0)
		+ '  ' + Math.round((j.elapsed_ms || 0) / 1000) + 's';
}

var currentChild = null;
var stopAfter = false;

function runOne(opts, uf) {
	return new Promise(function (resolve) {
		var args = buildArgs(opts, uf);
		console.error('\n======== VIA ' + uf + ' ========');
		console.error('node ' + args.join(' '));
		if (opts.dryRun) {
			resolve(0);
			return;
		}
		var env = Object.assign({}, process.env);
		if (!env.NODE_OPTIONS || env.NODE_OPTIONS.indexOf('max-old-space-size') < 0) {
			env.NODE_OPTIONS = ((env.NODE_OPTIONS || '') + ' --max-old-space-size=8192').trim();
		}
		var child = spawn(process.execPath, args, { cwd: ROOT, env: env, stdio: 'inherit', windowsHide: true });
		currentChild = child;
		child.on('error', function (err) {
			console.error(err);
			currentChild = null;
			resolve(1);
		});
		child.on('exit', function (code) {
			currentChild = null;
			resolve(code == null ? 1 : code);
		});
	});
}

async function main(argv) {
	var opts = parseCli(argv || process.argv.slice(2));
	if (opts.unknown) {
		console.error('argumento desconhecido: ' + opts.unknown);
		console.error(usage());
		return 1;
	}
	if (opts.help) {
		console.log(usage());
		return 0;
	}
	var ufs = jb.selectUfs(opts.only);
	if (!ufs.length) {
		console.error('Nenhuma UF em --only. Exemplos: --only=sp,rj  --only=sul');
		return 1;
	}
	console.error('DNE_GEO : ' + opts.dneGeo);
	console.error('GEOM    : ' + opts.geom);
	console.error('Out     : ' + opts.outDir);
	console.error('UFs     : ' + ufs.join(' '));
	if (opts.list) {
		ufs.forEach(function (uf) { console.error(statusLine(opts.outDir, uf)); });
		return 0;
	}
	fs.mkdirSync(opts.outDir, { recursive: true });

	function onStop() {
		stopAfter = true;
		if (currentChild && !currentChild.killed) {
			try { currentChild.kill('SIGINT'); } catch (e) { /* ignore */ }
		}
	}
	process.on('SIGINT', onStop);
	process.on('SIGTERM', onStop);
	if (process.platform === 'win32') process.on('SIGBREAK', onStop);

	for (var i = 0; i < ufs.length; i++) {
		var uf = ufs[i];
		if (stopAfter) break;
		if (!opts.force && jobDone(opts.outDir, uf)) {
			console.error('[skip] ' + uf + ' já tem ' + path.basename(reportPath(opts.outDir, uf)));
			continue;
		}
		var code = await runOne(opts, uf);
		if (stopAfter || code === 130) {
			console.error('Interrompido. Rode o mesmo comando para retomar.');
			return 130;
		}
		if (code !== 0 || (!opts.dryRun && !jobDone(opts.outDir, uf))) {
			console.error('Falhou via ' + uf + ' (exit ' + code + ')');
			return code || 1;
		}
	}
	if (stopAfter) {
		console.error('Interrompido. Rode o mesmo comando para retomar.');
		return 130;
	}
	console.error('\nStatus:');
	ufs.forEach(function (uf) { console.error('  ' + statusLine(opts.outDir, uf)); });
	return 0;
}

module.exports = { parseCli: parseCli, jobDone: jobDone, buildArgs: buildArgs, reportPath: reportPath, main: main };

if (require.main === module) {
	main().then(function (code) {
		process.exit(code || 0);
	}).catch(function (err) {
		console.error(err);
		process.exit(1);
	});
}
