#!/usr/bin/env node
'use strict';

/**
 * Roda `dne-geo-join.js` nas 27 UFs, com progresso em disco.
 * Ctrl+C interrompe a UF atual; a próxima execução pula quem já tem
 * `DNE_GEO_RELATORIO_{UF}.json` (ou use `--force` para refazer).
 *
 *   node scripts/join-brasil.js
 *   node scripts/join-brasil.js --only=sp,rj
 *   node scripts/join-brasil.js --only=sudeste --list
 *   node scripts/join-brasil.js --force
 *
 * Flags extras (`--sem-fuzzy`, `--quiet`, …) passam para o join.
 */

var fs = require('fs');
var path = require('path');
var { spawn } = require('child_process');

var ROOT = path.resolve(__dirname, '..');
var JOIN = path.join(ROOT, 'dne-geo-join.js');
var ufBr = require(path.join(ROOT, 'uf-br'));
var PROGRESS_NAME = 'JOIN_BRASIL_PROGRESS.json';

function allUfs() {
	var seen = Object.create(null);
	var out = [];
	Object.keys(ufBr.IBGE_UF).forEach(function (k) {
		var u = ufBr.IBGE_UF[k];
		if (seen[u]) return;
		seen[u] = true;
		out.push(u);
	});
	return out.sort();
}

function parseCli(argv) {
	var o = {
		dneDir: 'D:\\dev\\ddsoft\\ddsoft-online\\_ignore\\Delimitado',
		osmDir: 'G:\\osm-geo-br-uf',
		outDir: 'G:\\dne-geo-br-join4',
		only: null,
		force: false,
		dryRun: false,
		list: false,
		help: false,
		extra: []
	};
	for (var i = 0; i < argv.length; i++) {
		var a = argv[i];
		if (a.indexOf('--dne=') === 0) o.dneDir = a.slice(6);
		else if (a.indexOf('--osm=') === 0) o.osmDir = a.slice(6);
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
		'Uso: node scripts/join-brasil.js [opções]',
		'',
		'  --dne=DIR     DNE Delimitado (LOG_LOGRADOURO_{UF}.TXT)',
		'  --osm=DIR     extract OSM (G:\\osm-geo-br-uf)',
		'  --out=DIR     saída DNE_GEO_* (G:\\dne-geo-br-join4)',
		'  --only=sp,rj  subset (UF, região: sul, sudeste, norte, …)',
		'  --force       refaz UFs que já têm relatório',
		'  --list        status sem join',
		'  --dry-run     imprime comandos',
		'',
		'Demais flags vão para dne-geo-join.js (--quiet, --sem-fuzzy, …).',
		'Ctrl+C: a UF em curso para; rode de novo para continuar.'
	].join('\n');
}

function selectUfs(only) {
	if (!only) return allUfs();
	var allow = ufBr.parseUfFilter(only, null);
	if (!allow) return [];
	return ufBr.ufAllowList(allow);
}

function progressPath(outDir) {
	return path.join(outDir, PROGRESS_NAME);
}

function loadProgress(outDir) {
	var p = progressPath(outDir);
	if (!fs.existsSync(p)) return { version: 1, jobs: {} };
	try {
		return JSON.parse(fs.readFileSync(p, 'utf8'));
	} catch (e) {
		return { version: 1, jobs: {}, loadError: String(e.message || e) };
	}
}

function saveProgress(outDir, prog) {
	fs.mkdirSync(outDir, { recursive: true });
	prog.updatedAt = new Date().toISOString();
	fs.writeFileSync(progressPath(outDir), JSON.stringify(prog, null, 2), 'utf8');
}

function reportPath(outDir, uf) {
	return path.join(outDir, 'DNE_GEO_RELATORIO_' + uf + '.json');
}

function logradouroOutPath(outDir, uf) {
	return path.join(outDir, 'DNE_GEO_LOGRADOURO_' + uf + '.TXT');
}

/** Join dessa UF terminou: relatório JSON no disco. */
function jobDone(outDir, uf) {
	var p = reportPath(outDir, uf);
	if (!fs.existsSync(p)) return false;
	try {
		var j = JSON.parse(fs.readFileSync(p, 'utf8'));
		return !!(j && (j.uf || j.geo_status));
	} catch (e) {
		return false;
	}
}

function hasOsmExtract(osmDir, uf) {
	var flat = path.join(osmDir, 'OSM_LOGRADOURO_' + uf + '.TXT');
	var shard = path.join(osmDir, 'OSM_LOGRADOURO_' + uf);
	return fs.existsSync(flat) || fs.existsSync(shard);
}

function hasDneLog(dneDir, uf) {
	return fs.existsSync(path.join(dneDir, 'LOG_LOGRADOURO_' + uf + '.TXT'));
}

function readReportSummary(outDir, uf) {
	var p = reportPath(outDir, uf);
	if (!fs.existsSync(p)) return null;
	try {
		var j = JSON.parse(fs.readFileSync(p, 'utf8'));
		var ok = j.geo_status && j.geo_status.ok;
		return {
			ok: ok == null ? null : ok,
			linhas: j.linhas_dne != null ? j.linhas_dne : null,
			okPct: j.geo_status && j.linhas_dne
				? ((j.geo_status.ok || 0) / j.linhas_dne * 100).toFixed(1)
				: null
		};
	} catch (e) {
		return null;
	}
}

function statusLine(uf, outDir, job) {
	var done = jobDone(outDir, uf);
	var st = job && job.status ? job.status : (done ? 'done' : '-');
	var sum = readReportSummary(outDir, uf);
	var extra = sum && sum.ok != null
		? '  ok=' + sum.ok + (sum.okPct != null ? ' (' + sum.okPct + '%)' : '')
		: '';
	return uf + '  ' + st + extra;
}

function quoteArg(a) {
	return /\s/.test(a) ? '"' + a + '"' : a;
}

function buildJoinArgs(opts, uf) {
	return [
		JOIN,
		'--dne=' + opts.dneDir,
		'--osm=' + opts.osmDir,
		'--out=' + opts.outDir,
		'--uf=' + uf
	].concat(opts.extra || []);
}

var currentChild = null;
var stopAfter = false;

function installStopHandlers() {
	function onStop() {
		stopAfter = true;
		if (currentChild && !currentChild.killed) {
			try { currentChild.kill('SIGINT'); } catch (e) { /* ignore */ }
		}
	}
	process.on('SIGINT', onStop);
	process.on('SIGTERM', onStop);
	if (process.platform === 'win32') process.on('SIGBREAK', onStop);
}

function runOne(opts, uf) {
	return new Promise(function (resolve) {
		var args = buildJoinArgs(opts, uf);
		console.error('\n======== JOIN ' + uf + ' ========');
		console.error('node ' + args.map(quoteArg).join(' '));

		if (opts.dryRun) {
			resolve({ code: 0, dryRun: true });
			return;
		}

		var env = Object.assign({}, process.env);
		if (!env.NODE_OPTIONS) {
			env.NODE_OPTIONS = '--max-old-space-size=8192';
		} else if (env.NODE_OPTIONS.indexOf('max-old-space-size') < 0) {
			env.NODE_OPTIONS += ' --max-old-space-size=8192';
		}

		var child = spawn(process.execPath, args, {
			cwd: ROOT,
			env: env,
			stdio: 'inherit',
			windowsHide: true
		});
		currentChild = child;
		child.on('error', function (err) {
			console.error(err);
			currentChild = null;
			resolve({ code: 1, error: err });
		});
		child.on('exit', function (code, signal) {
			currentChild = null;
			resolve({ code: code == null ? 1 : code, signal: signal });
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
	if (!fs.existsSync(JOIN)) {
		console.error('dne-geo-join.js não encontrado em ' + JOIN);
		return 1;
	}

	var ufs = selectUfs(opts.only);
	if (!ufs.length) {
		console.error('Nenhuma UF em --only. Exemplos: --only=sp,rj  --only=sul');
		return 1;
	}

	if (!opts.list && !opts.dryRun) {
		if (!fs.existsSync(opts.dneDir)) {
			console.error('DNE não encontrado: ' + opts.dneDir);
			return 1;
		}
		if (!fs.existsSync(opts.osmDir)) {
			console.error('OSM não encontrado: ' + opts.osmDir);
			return 1;
		}
	}

	fs.mkdirSync(opts.outDir, { recursive: true });
	var prog = loadProgress(opts.outDir);
	prog.dne = opts.dneDir;
	prog.osm = opts.osmDir;
	prog.out = opts.outDir;

	console.error('DNE : ' + opts.dneDir);
	console.error('OSM : ' + opts.osmDir);
	console.error('Out : ' + opts.outDir);
	console.error('UFs : ' + ufs.join(' '));
	console.error('');

	if (opts.list) {
		ufs.forEach(function (uf) {
			console.error(statusLine(uf, opts.outDir, prog.jobs[uf]));
		});
		return 0;
	}

	installStopHandlers();

	for (var i = 0; i < ufs.length; i++) {
		var uf = ufs[i];
		if (stopAfter) {
			console.error('Interrompido. Rode o mesmo comando para retomar.');
			return 130;
		}

		if (!opts.force && jobDone(opts.outDir, uf)) {
			console.error('[skip] ' + uf + ' já tem ' + path.basename(reportPath(opts.outDir, uf)));
			prog.jobs[uf] = Object.assign({}, prog.jobs[uf], {
				status: 'skipped',
				skipped: true,
				finishedAt: new Date().toISOString()
			});
			saveProgress(opts.outDir, prog);
			continue;
		}

		if (!opts.dryRun && !hasOsmExtract(opts.osmDir, uf)) {
			console.error('[skip] ' + uf + ': sem OSM_LOGRADOURO_' + uf + ' em ' + opts.osmDir);
			prog.jobs[uf] = {
				status: 'no-extract',
				updatedAt: new Date().toISOString()
			};
			saveProgress(opts.outDir, prog);
			continue;
		}
		if (!opts.dryRun && !hasDneLog(opts.dneDir, uf)) {
			console.error('[skip] ' + uf + ': sem LOG_LOGRADOURO_' + uf + '.TXT em ' + opts.dneDir);
			prog.jobs[uf] = {
				status: 'no-dne',
				updatedAt: new Date().toISOString()
			};
			saveProgress(opts.outDir, prog);
			continue;
		}

		prog.jobs[uf] = {
			status: 'running',
			startedAt: new Date().toISOString()
		};
		saveProgress(opts.outDir, prog);

		var result = await runOne(opts, uf);
		var done = jobDone(opts.outDir, uf);
		var status = 'failed';
		if (opts.dryRun) status = 'dry-run';
		else if (done && result.code === 0) status = 'done';
		else if (result.signal === 'SIGINT' || result.code === 130 || stopAfter) status = 'interrupted';
		else if (result.code === 0 && !done) status = 'incomplete';
		else status = 'failed';

		var sum = readReportSummary(opts.outDir, uf);
		prog.jobs[uf] = {
			status: status,
			exitCode: result.code,
			signal: result.signal || null,
			startedAt: prog.jobs[uf].startedAt,
			finishedAt: new Date().toISOString(),
			ok: sum && sum.ok,
			linhas: sum && sum.linhas,
			outLog: logradouroOutPath(opts.outDir, uf)
		};
		saveProgress(opts.outDir, prog);
		console.error('[job ' + uf + '] → ' + status);

		if (status === 'interrupted' || result.code === 130 || stopAfter) {
			console.error('Interrompido. Rode o mesmo comando para retomar.');
			return 130;
		}
		if (status === 'failed' || status === 'incomplete') {
			console.error('Falhou join ' + uf + ' (exit ' + result.code + ')');
			return result.code || 1;
		}
	}

	console.error('\nProgresso: ' + progressPath(opts.outDir));
	console.error('Status:');
	ufs.forEach(function (uf) {
		console.error('  ' + statusLine(uf, opts.outDir, prog.jobs[uf]));
	});
	return 0;
}

module.exports = {
	parseCli: parseCli,
	allUfs: allUfs,
	selectUfs: selectUfs,
	jobDone: jobDone,
	hasOsmExtract: hasOsmExtract,
	hasDneLog: hasDneLog,
	buildJoinArgs: buildJoinArgs,
	reportPath: reportPath,
	main: main,
	PROGRESS_NAME: PROGRESS_NAME
};

if (require.main === module) {
	main().then(function (code) {
		process.exit(code || 0);
	}).catch(function (err) {
		console.error(err);
		process.exit(1);
	});
}
