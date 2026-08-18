#!/usr/bin/env node
'use strict';

/**
 * Extrai `OSM_LOGRADOURO_GEOM_{UF}` (+ logradouro) do PBF do Brasil, por fatia,
 * com progresso em disco — Ctrl+C interrompe o extract atual; a próxima
 * execução retoma (ou pula fatias já `eof` com GEOM).
 *
 * Uso: docs/geo/extrair-geom-brasil.md
 *
 *   set NODE_OPTIONS=--max-old-space-size=8192
 *   node scripts/extract-brasil-way-geom.js --list
 *   node scripts/extract-brasil-way-geom.js
 *   node scripts/extract-brasil-way-geom.js --only=sp,rj
 */

var fs = require('fs');
var path = require('path');
var { spawn } = require('child_process');

var ROOT = path.resolve(__dirname, '..');
var EXTRACT = path.join(ROOT, 'extract-geocode-pbf.js');
var PROGRESS_NAME = 'EXTRACT_GEOM_PROGRESS.json';

/** Mesma receita de docs/geo/operacao-comandos.md (SE por UF). */
var JOBS = [
	{ id: 'norte', out: 'norte', region: 'norte' },
	{ id: 'nordeste', out: 'nordeste', region: 'nordeste' },
	{ id: 'centro-oeste', out: 'centro-oeste', region: 'centro-oeste' },
	{ id: 'sul', out: 'sul', region: 'sul' },
	{ id: 'es', out: 'es', uf: 'ES' },
	{ id: 'mg', out: 'mg', uf: 'MG' },
	{ id: 'rj', out: 'rj', uf: 'RJ' },
	{ id: 'sp', out: 'sp', uf: 'SP' }
];

function parseCli(argv) {
	var o = {
		pbf: 'G:\\brazil-260724.osm.pbf',
		outBase: 'G:\\osm-geo-br-geom',
		shardLines: 20000,
		datasets: 'logradouro,geom',
		only: null,
		force: false,
		dryRun: false,
		list: false,
		waveNodes: 0,
		waveStreets: 0,
		withBairro: false
	};
	for (var i = 0; i < argv.length; i++) {
		var a = argv[i];
		if (a.indexOf('--pbf=') === 0) o.pbf = a.slice(6);
		else if (a.indexOf('--out-base=') === 0) o.outBase = a.slice(11);
		else if (a.indexOf('--shard-lines=') === 0) o.shardLines = Number(a.slice(14)) | 0;
		else if (a.indexOf('--datasets=') === 0) o.datasets = a.slice(11);
		else if (a.indexOf('--only=') === 0) {
			o.only = a
				.slice(7)
				.split(/[,+\s]+/)
				.map(function (s) {
					return s.trim().toLowerCase();
				})
				.filter(Boolean);
		} else if (a.indexOf('--wave-nodes=') === 0) o.waveNodes = Number(a.slice(13)) | 0;
		else if (a.indexOf('--wave-streets=') === 0) o.waveStreets = Number(a.slice(15)) | 0;
		else if (a === '--force') o.force = true;
		else if (a === '--dry-run') o.dryRun = true;
		else if (a === '--list') o.list = true;
		else if (a === '--with-bairro') o.withBairro = true;
		else if (a === '--help' || a === '-h') o.help = true;
	}
	if (o.withBairro && o.datasets === 'logradouro,geom') {
		o.datasets = 'bairro,logradouro,geom';
	}
	return o;
}

function usage() {
	return [
		'Uso: node scripts/extract-brasil-way-geom.js [opções]',
		'',
		'  --pbf=G:\\brazil-260724.osm.pbf',
		'  --out-base=G:\\osm-geo-br-geom     pastas por fatia (não mexe nos osm-geo-br-* atuais)',
		'  --shard-lines=20000',
		'  --datasets=logradouro,geom        (ou --with-bairro)',
		'  --only=norte,sp,rj                subset de jobs',
		'  --force                           refaz fatias já done',
		'  --list                            status sem extrair',
		'  --dry-run                         imprime comandos',
		'',
		'Doc: docs/geo/extrair-geom-brasil.md'
	].join('\n');
}

function progressPath(outBase) {
	return path.join(outBase, PROGRESS_NAME);
}

function loadProgress(outBase) {
	var p = progressPath(outBase);
	if (!fs.existsSync(p)) return { version: 1, jobs: {} };
	try {
		return JSON.parse(fs.readFileSync(p, 'utf8'));
	} catch (e) {
		return { version: 1, jobs: {}, loadError: String(e.message || e) };
	}
}

function saveProgress(outBase, prog) {
	fs.mkdirSync(outBase, { recursive: true });
	prog.updatedAt = new Date().toISOString();
	fs.writeFileSync(progressPath(outBase), JSON.stringify(prog, null, 2), 'utf8');
}

function readCheckpoint(outDir) {
	var p = path.join(outDir, 'extract-checkpoint.json');
	if (!fs.existsSync(p)) return null;
	try {
		return JSON.parse(fs.readFileSync(p, 'utf8'));
	} catch (e) {
		return null;
	}
}

/** Conta artefatos OSM_LOGRADOURO_GEOM_* (flat .TXT ou pasta de shards). */
function countGeomArtifacts(outDir) {
	if (!fs.existsSync(outDir)) return { files: 0, dirs: 0, names: [] };
	var names = [];
	var files = 0;
	var dirs = 0;
	var entries = fs.readdirSync(outDir);
	for (var i = 0; i < entries.length; i++) {
		var n = entries[i];
		if (!/^OSM_LOGRADOURO_GEOM_/i.test(n)) continue;
		names.push(n);
		var full = path.join(outDir, n);
		if (fs.statSync(full).isDirectory()) dirs++;
		else files++;
	}
	return { files: files, dirs: dirs, names: names };
}

function jobDone(outDir, cp) {
	if (!cp) return false;
	if (cp.stoppedEarly) return false;
	if (!cp.cursor || !cp.cursor.eof) return false;
	if (!cp.datasets || !cp.datasets.geom) return false;
	var g = countGeomArtifacts(outDir);
	return g.files + g.dirs > 0;
}

function jobNeedsWipe(cp) {
	if (!cp || !cp.stoppedEarly) return false;
	var pending = (cp.counts && cp.counts.logradouroPending) || 0;
	return pending > 0;
}

function selectJobs(only) {
	if (!only || !only.length) return JOBS.slice();
	var set = Object.create(null);
	for (var i = 0; i < only.length; i++) set[only[i]] = 1;
	return JOBS.filter(function (j) {
		return set[j.id] || set[j.out];
	});
}

function buildExtractArgs(opts, job, outDir, resume) {
	var args = [
		EXTRACT,
		opts.pbf,
		'--out=' + outDir,
		'--datasets=' + opts.datasets,
		'--shard-lines=' + opts.shardLines
	];
	if (job.region) args.push('--region=' + job.region);
	if (job.uf) args.push('--uf=' + job.uf);
	if (opts.waveNodes > 0) args.push('--wave-nodes=' + opts.waveNodes);
	if (opts.waveStreets > 0) args.push('--wave-streets=' + opts.waveStreets);
	if (resume) args.push('--resume');
	return args;
}

function statusLine(job, outDir, progJob) {
	var cp = readCheckpoint(outDir);
	var geom = countGeomArtifacts(outDir);
	var done = jobDone(outDir, cp);
	var wipe = jobNeedsWipe(cp);
	var st = progJob && progJob.status ? progJob.status : done ? 'done' : cp ? 'partial' : 'pending';
	if (done) st = 'done';
	if (wipe) st = 'needs-wipe';
	var bits = [
		job.id.padEnd(14),
		st.padEnd(12),
		'datasets.geom=' + (cp && cp.datasets && cp.datasets.geom ? '1' : '0'),
		'eof=' + (cp && cp.cursor && cp.cursor.eof ? '1' : '0'),
		'geom=' + (geom.files + geom.dirs),
		cp && cp.counts && cp.counts.logradouroGeom != null
			? 'ways=' + cp.counts.logradouroGeom
			: ''
	];
	return bits.filter(Boolean).join('  ');
}

function runOne(opts, job, resume) {
	return new Promise(function (resolve) {
		var outDir = path.join(opts.outBase, job.out);
		var args = buildExtractArgs(opts, job, outDir, resume);
		console.error('\n======== job=' + job.id + ' resume=' + !!resume + ' ========');
		console.error('node ' + args.map(function (a) {
			return /\s/.test(a) ? '"' + a + '"' : a;
		}).join(' '));

		if (opts.dryRun) {
			resolve({ code: 0, dryRun: true });
			return;
		}

		fs.mkdirSync(outDir, { recursive: true });
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
		child.on('error', function (err) {
			console.error(err);
			resolve({ code: 1, error: err });
		});
		child.on('exit', function (code, signal) {
			resolve({ code: code == null ? 1 : code, signal: signal });
		});
	});
}

async function main() {
	var opts = parseCli(process.argv.slice(2));
	if (opts.help) {
		console.log(usage());
		return 0;
	}
	if (!fs.existsSync(EXTRACT)) {
		console.error('extract-geocode-pbf.js não encontrado em ' + EXTRACT);
		return 1;
	}
	if (!opts.list && !opts.dryRun && !fs.existsSync(opts.pbf)) {
		console.error('PBF não encontrado: ' + opts.pbf);
		return 1;
	}

	var jobs = selectJobs(opts.only);
	if (!jobs.length) {
		console.error('Nenhum job em --only. Ids: ' + JOBS.map(function (j) {
			return j.id;
		}).join(', '));
		return 1;
	}

	fs.mkdirSync(opts.outBase, { recursive: true });
	var prog = loadProgress(opts.outBase);
	prog.pbf = opts.pbf;
	prog.outBase = opts.outBase;
	prog.datasets = opts.datasets;
	prog.shardLines = opts.shardLines;

	console.error('PBF     : ' + opts.pbf);
	console.error('Out base: ' + opts.outBase);
	console.error('Datasets: ' + opts.datasets);
	console.error('Jobs    : ' + jobs.map(function (j) {
		return j.id;
	}).join(', '));
	console.error('');

	if (opts.list) {
		jobs.forEach(function (job) {
			var outDir = path.join(opts.outBase, job.out);
			console.log(statusLine(job, outDir, prog.jobs[job.id]));
		});
		return 0;
	}

	var stopAfter = false;
	function onSig() {
		stopAfter = true;
		console.error('\n[orquestrador] Ctrl+C — não inicia próximo job após o extract atual.\n');
	}
	process.on('SIGINT', onSig);
	process.on('SIGTERM', onSig);

	for (var i = 0; i < jobs.length; i++) {
		if (stopAfter) {
			console.error('Parado antes do job ' + jobs[i].id);
			break;
		}
		var job = jobs[i];
		var outDir = path.join(opts.outBase, job.out);
		var cp = readCheckpoint(outDir);
		var done = jobDone(outDir, cp);

		if (done && !opts.force) {
			console.error('[skip] ' + job.id + ' já completo (eof + GEOM)');
			prog.jobs[job.id] = Object.assign({}, prog.jobs[job.id] || {}, {
				status: 'done',
				outDir: outDir,
				skipped: true,
				finishedAt: new Date().toISOString()
			});
			saveProgress(opts.outBase, prog);
			continue;
		}

		if (jobNeedsWipe(cp)) {
			console.error(
				'[bloqueado] ' +
					job.id +
					': cancelado com logradouroPending — apague ' +
					outDir +
					' e rode de novo (sem --resume). Ver extract-e-artefatos.md'
			);
			prog.jobs[job.id] = {
				status: 'needs-wipe',
				outDir: outDir,
				updatedAt: new Date().toISOString()
			};
			saveProgress(opts.outBase, prog);
			return 2;
		}

		var resume = !opts.force && !!cp && !done;
		if (opts.force && fs.existsSync(outDir) && !opts.dryRun) {
			// extract sem --resume dá wipe dos OSM_* na pasta
			resume = false;
		}

		prog.jobs[job.id] = {
			status: 'running',
			outDir: outDir,
			resume: resume,
			startedAt: new Date().toISOString()
		};
		saveProgress(opts.outBase, prog);

		var result = await runOne(opts, job, resume);
		cp = readCheckpoint(outDir);
		done = jobDone(outDir, cp);

		var status = 'failed';
		if (opts.dryRun) status = 'dry-run';
		else if (done) status = 'done';
		else if (cp && cp.stoppedEarly) status = 'interrupted';
		else if (result.code === 0) status = 'incomplete';
		else status = 'failed';

		prog.jobs[job.id] = {
			status: status,
			outDir: outDir,
			resume: resume,
			exitCode: result.code,
			signal: result.signal || null,
			startedAt: prog.jobs[job.id].startedAt,
			finishedAt: new Date().toISOString(),
			logradouroGeom: cp && cp.counts ? cp.counts.logradouroGeom : null,
			eof: !!(cp && cp.cursor && cp.cursor.eof),
			stoppedEarly: !!(cp && cp.stoppedEarly)
		};
		saveProgress(opts.outBase, prog);
		console.error('[job ' + job.id + '] → ' + status);

		if (status === 'interrupted' || result.code === 130 || stopAfter) {
			console.error('Interrompido. Rode o mesmo comando para retomar.');
			return 130;
		}
		if (status === 'failed') {
			console.error('Falhou job ' + job.id + ' (exit ' + result.code + ')');
			return result.code || 1;
		}
		if (jobNeedsWipe(cp)) {
			console.error(
				'Extract parou com pendentes. Apague ' + outDir + ' e recomece esta fatia.'
			);
			return 2;
		}
	}

	console.error('\nProgresso: ' + progressPath(opts.outBase));
	console.error('Status:');
	jobs.forEach(function (job) {
		console.error('  ' + statusLine(job, path.join(opts.outBase, job.out), prog.jobs[job.id]));
	});
	return 0;
}

main()
	.then(function (code) {
		process.exit(code || 0);
	})
	.catch(function (err) {
		console.error(err);
		process.exit(1);
	});
