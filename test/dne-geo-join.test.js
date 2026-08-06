'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var fs = require('fs');
var os = require('os');
var path = require('path');
var join = require('../dne-geo-join');

// Município A: tudo dentro de ~1 km. Homônimo distante a ~150 km.
var A_LAT = -20.000, A_LNG = -40.000;
var LONGE_LAT = -21.000, LONGE_LNG = -41.000;

function osmRow(id, name, nameNorm, kind, lat, lng, nodes, alt) {
	var d = 0.0005;
	return [
		id, name, nameNorm, kind, 'ZZ', '', '', '', '', '',
		lat, lng, lat - d, lat + d, lng - d, lng + d, nodes || 4,
		alt || '', alt ? alt.toLowerCase() : '', 'way'
	].join('@');
}

function setupDirs() {
	var base = fs.mkdtempSync(path.join(os.tmpdir(), 'dne-geo-join-'));
	var dne = path.join(base, 'dne');
	var osm = path.join(base, 'osm');
	var out = path.join(base, 'out');
	fs.mkdirSync(dne);
	fs.mkdirSync(osm);

	// LOG_LOCALIDADE: 1 = município com IBGE, 2 = distrito subordinado a 1 (sem IBGE),
	// 3 = município vizinho (para exclusão multi-município)
	var loc = [
		'1@ZZ@Cidade A@@1@M@@Cid A@1234567',
		'2@ZZ@Distrito B@@1@D@1@Dist B@',
		'3@ZZ@Cidade C@@1@M@@Cid C@7654321'
	].join('\n');
	fs.writeFileSync(path.join(dne, 'LOG_LOCALIDADE.TXT'), Buffer.from(loc, 'latin1'));

	var bai = [
		'10@ZZ@1@Centro@Ctr',
		'11@ZZ@1@Vila Nova@Vl Nova',
		'12@ZZ@2@Bairro do Distrito@Bro Dist',
		'13@ZZ@3@Centro C@Ctr C'
	].join('\n');
	fs.writeFileSync(path.join(dne, 'LOG_BAIRRO.TXT'), Buffer.from(bai, 'latin1'));

	// LOG_NU@UF@LOC@BAI_INI@BAI_FIM@LOG_NO@COMPL@CEP@TLO@STA@ABREV
	var log = [
		'100@ZZ@1@10@@Alfa Unica@@11111000@Rua@S@R Alfa Unica',
		'101@ZZ@1@10@@Beta Unica@@11111001@Rua@S@R Beta Unica',
		'102@ZZ@1@11@@Comum@@11111002@Rua@S@R Comum',
		'103@ZZ@1@11@@Goiás@@11111003@Travessa@S@Tv Goiás',
		'104@ZZ@1@11@@Luiz Gushiken@@11111004@Rua@S@R Luiz Gushiken',
		'105@ZZ@1@11@@da República@@11111005@Praça@S@Pç da República',
		'106@ZZ@1@11@@Parquinho@@11111006@Rua@S@R Parquinho',
		'107@ZZ@2@12@@do Distrito@@11111007@Rua@S@R do Distrito',
		'108@ZZ@1@11@@Inexistente@@11111008@Rua@S@R Inexistente',
		'109@ZZ@1@11@@Alterada@@11111009@Rua@S@R Alterada',
		// loteamento na periferia: existe no OSM, mas fora da pegada de âncoras (~3 km)
		'110@ZZ@1@11@@Periferia@@11111010@Rua@S@R Periferia',
		// multi-município: mesma via reivindicada por loc 1 (2 linhas) e loc 3 (1 linha)
		'112@ZZ@1@10@@Compartilhada@@11111012@Rua@S@R Compartilhada',
		'113@ZZ@1@11@@Compartilhada@@11111013@Rua@S@R Compartilhada',
		'114@ZZ@3@13@@Compartilhada@@11111014@Rua@S@R Compartilhada',
		// âncora do município C (perto de A, para a pegada de C cobrir a via compartilhada)
		'115@ZZ@3@13@@Gama Unica@@11111015@Rua@S@R Gama Unica',
		// DNE sem título; OSM com Doutor → regra `titulo`
		'116@ZZ@1@10@@Olimpio Carr Ribeiro@@04775120@Rua@S@R Olimpio Carr Ribeiro'
	].join('\n');
	fs.writeFileSync(path.join(dne, 'LOG_LOGRADOURO_ZZ.TXT'), Buffer.from(log, 'latin1'));

	var ways = [
		// âncoras: nome único no DNE e um cluster só → constroem o footprint de loc 1
		osmRow(1, 'Rua Alfa Unica', 'rua alfa unica', 'residential', A_LAT, A_LNG),
		osmRow(2, 'Rua Beta Unica', 'rua beta unica', 'residential', A_LAT - 0.001, A_LNG - 0.001),
		// homônimo: um no município, outro a 150 km — footprint tem que escolher o de perto
		osmRow(3, 'Rua Comum', 'rua comum', 'residential', A_LAT - 0.002, A_LNG - 0.002),
		osmRow(4, 'Rua Comum', 'rua comum', 'residential', LONGE_LAT, LONGE_LNG, 40),
		// DNE diz Travessa, OSM diz Rua → regra `nucleo`
		osmRow(5, 'Rua Goiás', 'rua goias', 'residential', A_LAT - 0.003, A_LNG - 0.003),
		// DNE grafa Luiz, OSM grafa Luis → regra `fonetico`
		osmRow(6, 'Rua Luis Gushiken', 'rua luis gushiken', 'residential', A_LAT - 0.004, A_LNG - 0.004),
		// praça mapeada como parque → só casa porque TLO_TX é de área
		osmRow(7, 'Praça da República', 'praca da republica', 'park', A_LAT - 0.001, A_LNG - 0.003),
		// armadilha: parque cujo nome bate com uma RUA do DNE → a guarda tem que barrar
		osmRow(8, 'Rua Parquinho', 'rua parquinho', 'park', A_LAT - 0.002, A_LNG - 0.004),
		// via no distrito: 2 clusters, então NÃO vira âncora — o distrito fica sem
		// pegada própria e precisa herdar a do município pai. O cluster distante é
		// mais pesado (40 nós), então o desempate por tamanho escolheria errado:
		// só o footprint herdado salva.
		osmRow(9, 'Rua do Distrito', 'rua do distrito', 'residential', A_LAT - 0.003, A_LNG - 0.001),
		osmRow(11, 'Rua do Distrito', 'rua do distrito', 'residential', LONGE_LAT, LONGE_LNG, 40),
		// nome principal diferente; casa pelo name_alt
		osmRow(10, 'Rua Nome Novo', 'rua nome novo', 'residential', A_LAT - 0.004, A_LNG - 0.002, 4, 'Rua Alterada'),
		// ~3 km ao sul: fora da pegada dilatada. Precisa do homônimo distante,
		// senão o próprio nome vira âncora do município e entra na pegada.
		osmRow(12, 'Rua Periferia', 'rua periferia', 'residential', A_LAT - 0.030, A_LNG - 0.002),
		osmRow(13, 'Rua Periferia', 'rua periferia', 'residential', LONGE_LAT, LONGE_LNG, 40),
		// via na borda, um cluster só — reivindicada por A e C
		osmRow(14, 'Rua Compartilhada', 'rua compartilhada', 'residential', A_LAT - 0.0015, A_LNG - 0.0015),
		// âncora de C (perto de A)
		osmRow(15, 'Rua Gama Unica', 'rua gama unica', 'residential', A_LAT - 0.0005, A_LNG - 0.0005),
		// DNE "Olímpio Carr Ribeiro" ↔ OSM "Rua Doutor Olímpio Carr Ribeiro"
		osmRow(16, 'Rua Doutor Olimpio Carr Ribeiro', 'rua doutor olimpio carr ribeiro',
			'residential', A_LAT - 0.0025, A_LNG - 0.001)
	].join('\n');
	fs.writeFileSync(path.join(osm, 'OSM_LOGRADOURO_ZZ.TXT'), ways, 'utf8');

	return { base: base, dne: dne, osm: osm, out: out };
}

/**
 * Fixture mínima: âncoras em dois polos (raio grande, células do meio vazias)
 * + um logradouro no buraco → envelope; e cluster compartilhado por 2 municípios.
 */
function setupEnvelopeDirs() {
	var base = fs.mkdtempSync(path.join(os.tmpdir(), 'dne-geo-env-'));
	var dne = path.join(base, 'dne');
	var osm = path.join(base, 'osm');
	var out = path.join(base, 'out');
	fs.mkdirSync(dne);
	fs.mkdirSync(osm);

	var loc = [
		'1@ZZ@Cidade A@@1@M@@Cid A@1234567',
		'3@ZZ@Cidade C@@1@M@@Cid C@7654321'
	].join('\n');
	fs.writeFileSync(path.join(dne, 'LOG_LOCALIDADE.TXT'), Buffer.from(loc, 'latin1'));

	var bai = [
		'10@ZZ@1@Centro@Ctr',
		'13@ZZ@3@Centro C@Ctr C'
	].join('\n');
	fs.writeFileSync(path.join(dne, 'LOG_BAIRRO.TXT'), Buffer.from(bai, 'latin1'));

	// âncoras polares: Norte e Sul a ±0,04° (~4,4 km) — o meio fica sem célula
	var log = [
		'200@ZZ@1@10@@Polo Norte@@20000001@Rua@S@R Polo Norte',
		'201@ZZ@1@10@@Polo Sul@@20000002@Rua@S@R Polo Sul',
		// no buraco do meio: nome com 2 clusters (não vira âncora)
		'202@ZZ@1@10@@No Buraco@@20000003@Rua@S@R No Buraco',
		// multi-município
		'203@ZZ@1@10@@Fronteira@@20000004@Rua@S@R Fronteira',
		'204@ZZ@1@10@@Fronteira@@20000005@Rua@S@R Fronteira',
		'205@ZZ@3@13@@Fronteira@@20000006@Rua@S@R Fronteira',
		'206@ZZ@3@13@@Ancora C@@20000007@Rua@S@R Ancora C'
	].join('\n');
	fs.writeFileSync(path.join(dne, 'LOG_LOGRADOURO_ZZ.TXT'), Buffer.from(log, 'latin1'));

	var N = A_LAT + 0.04, S = A_LAT - 0.04;
	var ways = [
		osmRow(1, 'Rua Polo Norte', 'rua polo norte', 'residential', N, A_LNG),
		osmRow(2, 'Rua Polo Sul', 'rua polo sul', 'residential', S, A_LNG),
		// candidato no centro (buraco) + homônimo longe
		osmRow(3, 'Rua No Buraco', 'rua no buraco', 'residential', A_LAT, A_LNG),
		osmRow(4, 'Rua No Buraco', 'rua no buraco', 'residential', LONGE_LAT, LONGE_LNG, 40),
		// cluster único na fronteira (perto do polo sul de A e da âncora de C)
		osmRow(5, 'Rua Fronteira', 'rua fronteira', 'residential', S + 0.005, A_LNG),
		osmRow(6, 'Rua Ancora C', 'rua ancora c', 'residential', S + 0.003, A_LNG)
	].join('\n');
	fs.writeFileSync(path.join(osm, 'OSM_LOGRADOURO_ZZ.TXT'), ways, 'utf8');

	return { base: base, dne: dne, osm: osm, out: out };
}

function readOut(dir, uf) {
	var txt = fs.readFileSync(path.join(dir, 'DNE_GEO_LOGRADOURO_' + uf + '.TXT'), 'utf8');
	var byId = {};
	txt.split(/\r?\n/).filter(Boolean).forEach(function (line) {
		var p = line.split('@');
		byId[p[0]] = p;
	});
	return byId;
}

test('dne-geo-join: lê OSM em shards (mesmo resultado que flat)', async function (t) {
	var d = setupDirs();
	t.after(function () { fs.rmSync(d.base, { recursive: true, force: true }); });

	// Converte flat → 2 shards de 5 linhas (MANIFEST)
	var flat = path.join(d.osm, 'OSM_LOGRADOURO_ZZ.TXT');
	var lines = fs.readFileSync(flat, 'utf8').split(/\r?\n/).filter(Boolean);
	fs.unlinkSync(flat);
	var root = path.join(d.osm, 'OSM_LOGRADOURO_ZZ');
	var shardDir = path.join(root, '5-linhas');
	fs.mkdirSync(shardDir, { recursive: true });
	var mid = Math.ceil(lines.length / 2);
	fs.writeFileSync(path.join(shardDir, '000001.txt'), lines.slice(0, mid).join('\n') + '\n');
	fs.writeFileSync(path.join(shardDir, '000002.txt'), lines.slice(mid).join('\n') + '\n');
	fs.writeFileSync(path.join(root, 'MANIFEST.json'), JSON.stringify({
		dataset_key: 'OSM_LOGRADOURO_ZZ',
		shard_lines: 5,
		shard_dir: '5-linhas',
		complete: true,
		total_lines: lines.length,
		shard_count: 2,
		shards: [
			{ file: '000001.txt', lines: mid },
			{ file: '000002.txt', lines: lines.length - mid }
		]
	}));

	var res = join.resolveOsmLogradouro(d.osm, 'ZZ');
	assert.equal(res.mode, 'shard');
	assert.equal(res.paths.length, 2);

	var rel = await join.run({
		dneDir: d.dne, osmDir: d.osm, outDir: d.out, uf: 'ZZ', quiet: true
	});
	assert.equal(rel.osm.mode, 'shard');
	assert.equal(rel.osm.files, 2);
	assert.equal(rel.osm.linhas, lines.length);
	assert.ok(rel.geo_status.ok >= 11);

	var row = readOut(d.out, 'ZZ');
	assert.equal(row['100'][20], 'ok');
	assert.equal(row['100'][21], 'exato');
	assert.equal(row['103'][21], 'nucleo');
});

test('dne-geo-join: cascata, footprint, guarda de área e status', async function (t) {
	var d = setupDirs();
	t.after(function () { fs.rmSync(d.base, { recursive: true, force: true }); });

	var rel = await join.run({
		dneDir: d.dne, osmDir: d.osm, outDir: d.out, uf: 'ZZ', quiet: true
	});

	var row = readOut(d.out, 'ZZ');
	assert.equal(Object.keys(row).length, 16);
	assert.equal(row['100'].length, 26, 'contrato de 26 colunas');

	// --- desnormalização DNE
	assert.equal(row['100'][11], 'Cidade A');
	assert.equal(row['100'][12], 'Centro');
	assert.equal(row['100'][13], '1234567', 'IBGE do município');
	assert.equal(row['107'][13], '1234567', 'distrito herda o IBGE do LOC_NU_SUB');

	// --- cascata: cada degrau marca sua regra
	assert.equal(row['100'][20], 'ok');
	assert.equal(row['100'][21], 'exato');
	assert.equal(row['103'][21], 'nucleo', 'DNE Travessa Goiás ↔ OSM Rua Goiás');
	assert.equal(row['104'][21], 'fonetico', 'DNE Luiz ↔ OSM Luis');
	assert.equal(row['105'][21], 'area', 'Praça ↔ leisure=park');
	assert.equal(row['109'][21], 'name_alt', 'casou pela denominação alternativa');
	assert.equal(row['116'][20], 'ok');
	assert.equal(row['116'][21], 'titulo', 'DNE sem Doutor ↔ OSM com Doutor');

	// --- guarda kind-aware: parque não vira Rua
	assert.equal(row['106'][20], 'sem_nome_osm', 'Rua não pode casar com parque homônimo');
	assert.equal(row['106'][14], '', 'sem coordenada');

	// --- footprint escolhe o homônimo do município, não o de 150 km
	assert.equal(row['102'][20], 'ok');
	assert.ok(Math.abs(Number(row['102'][14]) - A_LAT) < 0.01,
		'lat deve ficar no município A, veio ' + row['102'][14]);
	assert.equal(row['102'][24], '1', 'só um candidato dentro do footprint');

	// --- distrito sem âncora própria casa herdando a pegada do pai,
	//     e escolhe o cluster de perto mesmo sendo o mais leve
	assert.equal(row['107'][20], 'ok');
	assert.ok(Math.abs(Number(row['107'][14]) - A_LAT) < 0.01,
		'devia pegar o cluster do município pai, veio ' + row['107'][14]);

	// --- sem correspondente: vazio, nunca chutado
	assert.equal(row['108'][20], 'sem_nome_osm');
	assert.equal(row['108'][14], '');
	assert.equal(row['108'][15], '');
	assert.equal(row['108'][16], '');
	assert.equal(row['108'][22], '');
	assert.equal(row['108'][23], '0');

	// --- longe demais da mancha: continua `ambiguo` (ou ok se a 2ª volta
	//     expandir a pegada até lá — o caso controlado de envelope está no teste dedicado)
	assert.ok(row['110'][20] === 'ambiguo' || row['110'][20] === 'ok');

	// --- exclusão multi-município: loc 1 tem 2 linhas, loc 3 tem 1 → loc 3 perde
	assert.equal(row['112'][20], 'ok', 'loc 1 (maioria) fica com o cluster');
	assert.equal(row['113'][20], 'ok');
	assert.equal(row['114'][20], 'ambiguo', 'loc 3 revogado por conflito_municipio');
	assert.equal(row['114'][14], '', 'sem coordenada após revogação');
	assert.ok((rel.ambiguo_por_motivo.conflito_municipio || 0) >= 1);
	assert.ok(rel.revogados_conflito_municipio >= 1);
	assert.ok(rel.clusters_multi_municipio >= 1);

	// --- relatório
	assert.equal(rel.uf, 'ZZ');
	assert.equal(rel.linhas_dne, 16);
	// ok: base 8 + compartilhada×2 + gama + titulo = 12; periferia pode ou não
	assert.ok(rel.geo_status.ok >= 12);
	assert.equal(rel.geo_status.sem_nome_osm, 2);
	assert.ok((rel.geo_status.ambiguo || 0) >= 1); // pelo menos o conflito
	assert.ok(rel.localidades.com_footprint >= 1);
	assert.ok(rel.localidades.herdados_de_subordinacao >= 1);
	assert.ok((rel.geo_regra.titulo || 0) >= 1);
	assert.ok(Array.isArray(rel.titulo_exemplos));
	assert.ok(rel.titulo_exemplos.some(function (e) {
		return e.log_nu === '116' && e.geo_regra === 'titulo';
	}));
	assert.ok(Array.isArray(rel.sem_nome_osm_exemplos));
	assert.ok(rel.sem_nome_osm_exemplos.length >= 1);

	// --- subproduto de bairro
	var bairro = fs.readFileSync(path.join(d.out, 'DNE_GEO_BAIRRO_ZZ.TXT'), 'utf8')
		.split(/\r?\n/).filter(Boolean).map(function (l) { return l.split('@'); });
	assert.ok(bairro.length >= 2);
	bairro.forEach(function (b) {
		assert.equal(b.length, 13);
		assert.ok(Number(b[8]) <= Number(b[9]), 'lat_min <= lat_max');
	});
});

test('dne-geo-join: UF sem extract sai toda como sem_extract', async function (t) {
	var d = setupDirs();
	t.after(function () { fs.rmSync(d.base, { recursive: true, force: true }); });

	var rel = await join.run({
		dneDir: d.dne, osmDir: d.osm, outDir: d.out, uf: 'ZZ', quiet: true,
		// aponta para uma UF cujo OSM_LOGRADOURO não existe
	});
	assert.ok(rel.geo_status.ok > 0); // sanidade do caminho normal

	var d2 = setupDirs();
	t.after(function () { fs.rmSync(d2.base, { recursive: true, force: true }); });
	fs.rmSync(path.join(d2.osm, 'OSM_LOGRADOURO_ZZ.TXT'));
	var rel2 = await join.run({
		dneDir: d2.dne, osmDir: d2.osm, outDir: d2.out, uf: 'ZZ', quiet: true
	});
	assert.equal(rel2.geo_status.sem_extract, 16);
	assert.equal(rel2.geo_status.ok, undefined);
});

test('dne-geo-join: envelope (buraco na pegada) e exclusão multi-município', async function (t) {
	var d = setupEnvelopeDirs();
	t.after(function () { fs.rmSync(d.base, { recursive: true, force: true }); });

	var rel = await join.run({
		dneDir: d.dne, osmDir: d.osm, outDir: d.out, uf: 'ZZ', quiet: true
	});
	var row = readOut(d.out, 'ZZ');

	// buraco: km da mancha ≈ 0, fora das células dos polos → envelope recupera
	assert.equal(row['202'][20], 'ok', 'envelope recupera candidato no buraco da pegada');
	assert.ok(row['202'][14] !== '');
	assert.ok(rel.envelope_recuperados >= 1);

	// multi: loc 1 tem 2 linhas, loc 3 tem 1
	assert.equal(row['203'][20], 'ok');
	assert.equal(row['204'][20], 'ok');
	assert.equal(row['205'][20], 'ambiguo');
	assert.equal(row['205'][14], '');
	assert.ok(rel.revogados_conflito_municipio >= 1);
	assert.ok(rel.clusters_multi_municipio >= 1);
});

/**
 * dilate=0: todas as vias ok na mesma célula 0,01°; alvo ~1,5 km ao sul
 * (outra célula) com o mesmo CEP-5 44444 + homônimo longe.
 * Envelope desligado → só vizinho_cep5 recupera o cluster perto.
 */
function setupVizinhoCep5Dirs() {
	var base = fs.mkdtempSync(path.join(os.tmpdir(), 'dne-geo-v5-'));
	var dne = path.join(base, 'dne');
	var osm = path.join(base, 'osm');
	var out = path.join(base, 'out');
	fs.mkdirSync(dne);
	fs.mkdirSync(osm);

	var loc = ['1@ZZ@Cidade A@@1@M@@Cid A@1234567'].join('\n');
	fs.writeFileSync(path.join(dne, 'LOG_LOCALIDADE.TXT'), Buffer.from(loc, 'latin1'));
	var bai = ['10@ZZ@1@Centro@Ctr'].join('\n');
	fs.writeFileSync(path.join(dne, 'LOG_BAIRRO.TXT'), Buffer.from(bai, 'latin1'));

	var log = [
		'300@ZZ@1@10@@Ancora Um@@44444001@Rua@S@R Ancora Um',
		'301@ZZ@1@10@@Ancora Dois@@44444002@Rua@S@R Ancora Dois',
		'302@ZZ@1@10@@Vizinha Um@@44444100@Rua@S@R Vizinha Um',
		'303@ZZ@1@10@@Vizinha Dois@@44444110@Rua@S@R Vizinha Dois',
		'304@ZZ@1@10@@Vizinha Tres@@44444120@Rua@S@R Vizinha Tres',
		// mesmo CEP-5 44444; nome com 2 clusters OSM (perto + longe)
		'305@ZZ@1@10@@Alvo Cep@@44444999@Rua@S@R Alvo Cep'
	].join('\n');
	fs.writeFileSync(path.join(dne, 'LOG_LOGRADOURO_ZZ.TXT'), Buffer.from(log, 'latin1'));

	// Offsets < 0,01° → mesma célula; SUL ≈ 1,55 km (fora da célula com dilate=0)
	var SUL = A_LAT - 0.014;
	var ways = [
		osmRow(1, 'Rua Ancora Um', 'rua ancora um', 'residential', A_LAT, A_LNG),
		osmRow(2, 'Rua Ancora Dois', 'rua ancora dois', 'residential', A_LAT - 0.0003, A_LNG),
		osmRow(3, 'Rua Vizinha Um', 'rua vizinha um', 'residential', A_LAT - 0.0005, A_LNG + 0.0002),
		osmRow(4, 'Rua Vizinha Dois', 'rua vizinha dois', 'residential', A_LAT - 0.0007, A_LNG - 0.0002),
		osmRow(5, 'Rua Vizinha Tres', 'rua vizinha tres', 'residential', A_LAT - 0.0004, A_LNG + 0.0001),
		osmRow(6, 'Rua Alvo Cep', 'rua alvo cep', 'residential', SUL, A_LNG),
		osmRow(7, 'Rua Alvo Cep', 'rua alvo cep', 'residential', LONGE_LAT, LONGE_LNG, 40)
	].join('\n');
	fs.writeFileSync(path.join(osm, 'OSM_LOGRADOURO_ZZ.TXT'), ways, 'utf8');

	return { base: base, dne: dne, osm: osm, out: out };
}

test('dne-geo-join: vizinho_cep5 recupera fora_do_footprint perto do CEP-5', async function (t) {
	var d = setupVizinhoCep5Dirs();
	t.after(function () { fs.rmSync(d.base, { recursive: true, force: true }); });

	var rel = await join.run({
		dneDir: d.dne, osmDir: d.osm, outDir: d.out, uf: 'ZZ', quiet: true,
		footprintDilate: 0,
		semEnvelope: true,
		vizinhoCep5TolKm: 2,
		vizinhoCep5Min: 3
	});
	var row = readOut(d.out, 'ZZ');

	assert.equal(row['305'][20], 'ok', 'recuperado por vizinhança CEP-5');
	assert.equal(row['305'][21], 'vizinho_cep5');
	assert.ok(row['305'][14] !== '');
	assert.ok(Math.abs(Number(row['305'][14]) - (A_LAT - 0.014)) < 0.002,
		'escolhe o cluster perto das vizinhas, não o de 150 km');
	assert.ok(rel.vizinho_cep5_recuperados >= 1);
	assert.ok(Array.isArray(rel.vizinho_cep5_exemplos));
	assert.ok(rel.vizinho_cep5_exemplos.some(function (e) {
		return e.log_nu === '305' && e.fonte === 'cep5' && e.n_vizinhos >= 3;
	}));
});

test('dne-geo-join: --sem-vizinho-cep5 deixa fora_do_footprint vazio', async function (t) {
	var d = setupVizinhoCep5Dirs();
	t.after(function () { fs.rmSync(d.base, { recursive: true, force: true }); });

	var rel = await join.run({
		dneDir: d.dne, osmDir: d.osm, outDir: d.out, uf: 'ZZ', quiet: true,
		footprintDilate: 0,
		semEnvelope: true,
		semVizinhoCep5: true
	});
	var row = readOut(d.out, 'ZZ');
	assert.equal(row['305'][20], 'ambiguo');
	assert.equal(row['305'][14], '');
	assert.equal(rel.vizinho_cep5_recuperados || 0, 0);
});

test('digitsCep5 e nearestDistKm', function () {
	assert.equal(join.digitsCep5('04775-120'), '04775');
	assert.equal(join.digitsCep5('04775120'), '04775');
	assert.equal(join.digitsCep5(''), '');
	var d = join.nearestDistKm(0, 0, [{ lat: 0, lng: 0.01 }, { lat: 1, lng: 1 }]);
	assert.ok(d < 2 && d > 0.5);
	assert.equal(join.nearestDistKm(0, 0, []), Infinity);
});

test('dne-geo-join: parseCli lê as opções', function () {
	var o = join.parseCli([
		'--dne=D:\\dne', '--osm=G:\\osm', '--out=G:\\out', '--uf=sp',
		'--cluster-cell=0.05', '--max-extent-km=20', '--footprint-dilate=2',
		'--envelope-tol-km=0.5', '--sem-envelope', '--sem-exclusao-cluster',
		'--vizinho-cep5-tol-km=0.8', '--vizinho-cep5-min=2', '--sem-vizinho-cep5',
		'--quiet'
	]);
	assert.equal(o.dneDir, 'D:\\dne');
	assert.equal(o.osmDir, 'G:\\osm');
	assert.equal(o.outDir, 'G:\\out');
	assert.equal(o.uf, 'sp');
	assert.equal(o.clusterCell, 0.05);
	assert.equal(o.maxExtentKm, 20);
	assert.equal(o.footprintDilate, 2);
	assert.equal(o.envelopeTolKm, 0.5);
	assert.equal(o.semEnvelope, true);
	assert.equal(o.semExclusaoCluster, true);
	assert.equal(o.vizinhoCep5TolKm, 0.8);
	assert.equal(o.vizinhoCep5Min, 2);
	assert.equal(o.semVizinhoCep5, true);
	assert.equal(o.quiet, true);
});

test('dne-geo-join: coluna 26 traz as ways do cluster vencedor', async function (t) {
	var d = setupDirs();
	t.after(function () { fs.rmSync(d.base, { recursive: true, force: true }); });

	await join.run({
		dneDir: d.dne, osmDir: d.osm, outDir: d.out, uf: 'ZZ', quiet: true
	});
	var row = readOut(d.out, 'ZZ');
	var IDS = 25; // 0-based: coluna 26

	// Linha que casou: os ids são exatamente as ways do cluster escolhido.
	assert.equal(row['100'][20], 'ok');
	assert.equal(row['100'][IDS], '1', 'Alfa Unica → way 1');

	// Cluster com mais de uma way: ids em ordem numérica, sem repetição, e a
	// contagem bate com a coluna osm_ways (24).
	var comum = row['102'];
	assert.equal(comum[20], 'ok');
	var ids = comum[IDS].split('+');
	assert.equal(ids.length, Number(comum[23]), 'osm_way_ids bate com osm_ways');
	assert.deepEqual(ids.slice().sort(function (a, b) { return a - b; }), ids, 'ordem numérica');
	assert.equal(new Set(ids).size, ids.length, 'sem repetição');

	// Linha sem match não inventa ids.
	var semMatch = Object.keys(row).map(function (k) { return row[k]; })
		.find(function (r) { return r[20] !== 'ok'; });
	assert.ok(semMatch, 'há linha sem match no fixture');
	assert.equal(semMatch[IDS], '', 'geo_status != ok → coluna vazia');

	// Determinismo: rodar de novo dá o mesmo byte.
	var out2 = path.join(d.base, 'out2');
	await join.run({
		dneDir: d.dne, osmDir: d.osm, outDir: out2, uf: 'ZZ', quiet: true
	});
	assert.equal(
		fs.readFileSync(path.join(d.out, 'DNE_GEO_LOGRADOURO_ZZ.TXT'), 'utf8'),
		fs.readFileSync(path.join(out2, 'DNE_GEO_LOGRADOURO_ZZ.TXT'), 'utf8')
	);
});

test('dne-geo-join: ponto de addr:street não entra em osm_way_ids', async function (t) {
	var d = setupDirs();
	t.after(function () { fs.rmSync(d.base, { recursive: true, force: true }); });

	// `OSM_ADDR_POINT_ZZ` só é consultado para nome que NENHUMA way tem — o ponto
	// vira candidato sem `id`, e um id que não resolve do outro lado seria pior
	// que coluna vazia.
	fs.writeFileSync(
		path.join(d.osm, 'OSM_ADDR_POINT_ZZ.TXT'),
		[9001, A_LAT, A_LNG, 'Rua Inexistente', 'rua inexistente', '10', '', '', '', ''].join('@') + '\n',
		'utf8'
	);
	await join.run({
		dneDir: d.dne, osmDir: d.osm, outDir: d.out, uf: 'ZZ', quiet: true
	});
	var row = readOut(d.out, 'ZZ');
	assert.equal(row['108'][20], 'ok', 'casou pelo ponto de numeração');
	assert.equal(row['108'][25], '', 'sem way, sem id');
});
