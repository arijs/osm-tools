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

	// LOG_LOCALIDADE: 1 = município com IBGE, 2 = distrito subordinado a 1 (sem IBGE)
	var loc = [
		'1@ZZ@Cidade A@@1@M@@Cid A@1234567',
		'2@ZZ@Distrito B@@1@D@1@Dist B@'
	].join('\n');
	fs.writeFileSync(path.join(dne, 'LOG_LOCALIDADE.TXT'), Buffer.from(loc, 'latin1'));

	var bai = [
		'10@ZZ@1@Centro@Ctr',
		'11@ZZ@1@Vila Nova@Vl Nova',
		'12@ZZ@2@Bairro do Distrito@Bro Dist'
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
		// loteamento na periferia: existe no OSM, mas fora da pegada de âncoras
		'110@ZZ@1@11@@Periferia@@11111010@Rua@S@R Periferia'
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
		osmRow(13, 'Rua Periferia', 'rua periferia', 'residential', LONGE_LAT, LONGE_LNG, 40)
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

test('dne-geo-join: cascata, footprint, guarda de área e status', async function (t) {
	var d = setupDirs();
	t.after(function () { fs.rmSync(d.base, { recursive: true, force: true }); });

	var rel = await join.run({
		dneDir: d.dne, osmDir: d.osm, outDir: d.out, uf: 'ZZ', quiet: true
	});

	var row = readOut(d.out, 'ZZ');
	assert.equal(Object.keys(row).length, 11);
	assert.equal(row['100'].length, 25, 'contrato de 25 colunas');

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

	// --- fora da pegada: fica `ambiguo`, sem coordenada.
	// Já houve aqui uma "âncora local" (bairro/CEP) que recuperava esse caso; foi
	// medida na base real, rendia 36 linhas em 341 813, e saiu. Ver
	// docs/geo/amostras-ambiguo-sp.md.
	assert.equal(row['110'][20], 'ambiguo');
	assert.equal(row['110'][14], '', 'sem coordenada quando o candidato está fora da pegada');
	assert.equal(rel.ambiguo_por_motivo.fora_do_footprint, 1);

	// --- relatório
	assert.equal(rel.uf, 'ZZ');
	assert.equal(rel.linhas_dne, 11);
	assert.equal(rel.geo_status.ok, 8);
	assert.equal(rel.geo_status.sem_nome_osm, 2);
	assert.equal(rel.geo_status.ambiguo, 1);
	assert.ok(rel.localidades.com_footprint >= 1);
	assert.ok(rel.localidades.herdados_de_subordinacao >= 1);

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
	assert.equal(rel2.geo_status.sem_extract, 11);
	assert.equal(rel2.geo_status.ok, undefined);
});

test('dne-geo-join: parseCli lê as opções', function () {
	var o = join.parseCli([
		'--dne=D:\\dne', '--osm=G:\\osm', '--out=G:\\out', '--uf=sp',
		'--cluster-cell=0.05', '--max-extent-km=20', '--footprint-dilate=2', '--quiet'
	]);
	assert.equal(o.dneDir, 'D:\\dne');
	assert.equal(o.osmDir, 'G:\\osm');
	assert.equal(o.outDir, 'G:\\out');
	assert.equal(o.uf, 'sp');
	assert.equal(o.clusterCell, 0.05);
	assert.equal(o.maxExtentKm, 20);
	assert.equal(o.footprintDilate, 2);
	assert.equal(o.quiet, true);
});
