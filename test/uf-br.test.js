'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var uf = require('../uf-br');

test('ufFromIbge por prefixo', function () {
	assert.equal(uf.ufFromIbge('3550308'), 'SP');
	assert.equal(uf.ufFromIbge('3304557'), 'RJ');
	assert.equal(uf.ufFromIbge('3106200'), 'MG');
	assert.equal(uf.ufFromIbge('3205309'), 'ES');
	assert.equal(uf.ufFromIbge('1302603'), 'AM');
	assert.equal(uf.ufFromIbge('4314902'), 'RS');
});

test('ufFromTags ISO e addr:state', function () {
	assert.equal(uf.ufFromTags({ 'ISO3166-2': 'BR-SP' }), 'SP');
	assert.equal(uf.ufFromTags({ 'addr:state': 'RJ' }), 'RJ');
	assert.equal(uf.ufFromTags({ place: 'state', name: 'Minas Gerais' }), 'MG');
});

test('ufFromPoint Sudeste e outras regiões', function () {
	assert.equal(uf.ufFromPoint(-23.55, -46.63), 'SP');
	assert.equal(uf.ufFromPoint(-22.9, -43.2), 'RJ');
	assert.equal(uf.ufFromPoint(-30.03, -51.23), 'RS');
	assert.equal(uf.ufFromPoint(-3.12, -60.02), 'AM');
});

test('parseUfFilter região e UF', function () {
	var se = uf.parseUfFilter('', 'sudeste');
	assert.ok(se.SP && se.RJ && se.MG && se.ES);
	assert.equal(se.PR, undefined);

	var sul = uf.parseUfFilter('', 'sul');
	assert.ok(sul.PR && sul.SC && sul.RS);
	assert.equal(sul.SP, undefined);

	var co = uf.parseUfFilter('', 'centro-oeste');
	assert.ok(co.DF && co.GO && co.MT && co.MS);

	var mixed = uf.parseUfFilter('SP,ES', 'norte');
	assert.ok(mixed.SP && mixed.ES && mixed.AM && mixed.PA);
});

test('passesUfFilter e tagsDisallowedByFilter', function () {
	var onlySp = uf.parseUfFilter('SP', '');
	assert.equal(uf.passesUfFilter(onlySp, 'SP', -23.5, -46.6), true);
	assert.equal(uf.passesUfFilter(onlySp, 'RJ', -22.9, -43.2), false);
	// ponto em SP mesmo com UF XX
	assert.equal(uf.passesUfFilter(onlySp, 'XX', -23.55, -46.63), true);
	assert.equal(uf.tagsDisallowedByFilter(onlySp, { 'addr:state': 'RJ' }, ''), true);
	assert.equal(uf.tagsDisallowedByFilter(onlySp, { 'addr:state': 'SP' }, ''), false);
	assert.equal(uf.tagsDisallowedByFilter(null, { 'addr:state': 'RJ' }, ''), false);
});

// Regressão do rótulo que discordava do filtro (DDSOFT, 18/08/2026): rodando
// `--uf=MG`, uma via em Patrocínio caía na caixa de MG *e* na de GO, e a de GO —
// menor — vencia o desempate. A via era MANTIDA no run (passesUfFilter aceita
// ponto em caixa permitida) e escrita em OSM_LOGRADOURO_GEOM_GO, onde o
// consumidor de MG nunca ia procurar: 29.505 ways de MG sem traçado.
test('resolveUfFiltered — o filtro do run nomeia o que só a geometria resolve', function () {
	var mg = uf.parseUfFilter('MG', '');
	var patrocinio = { lat: -18.9375, lng: -46.9856 };
	var uberlandia = { lat: -18.9186, lng: -48.2772 };
	var montesClaros = { lat: -16.735, lng: -43.8617 };

	// o defeito, preservado como documentação do porquê — hoje ele só existe no
	// retângulo puro (`ufFromPoint`), que desde 18/08/2026 não decide mais nada:
	// `resolveUf` pergunta ao polígono antes (ver o teste do polígono abaixo).
	assert.equal(uf.ufFromPoint(patrocinio.lat, patrocinio.lng), 'GO');
	assert.equal(uf.ufFromPoint(uberlandia.lat, uberlandia.lng), 'GO');
	assert.equal(uf.ufFromPoint(montesClaros.lat, montesClaros.lng), 'BA');

	// e a correção
	assert.equal(uf.resolveUfFiltered(patrocinio, mg), 'MG');
	assert.equal(uf.resolveUfFiltered(uberlandia, mg), 'MG');
	assert.equal(uf.resolveUfFiltered(montesClaros, mg), 'MG');
});

test('resolveUfFiltered — tag e IBGE continuam mandando', function () {
	var mg = uf.parseUfFilter('MG', '');
	var patrocinio = { lat: -18.9375, lng: -46.9856 };

	// dado explícito da feature vence a geometria; quem descarta é o filtro,
	// não o rótulo — senão o run de MG renomearia Goiás inteiro.
	assert.equal(
		uf.resolveUfFiltered({ tags: { 'addr:state': 'GO' }, lat: patrocinio.lat, lng: patrocinio.lng }, mg),
		'GO'
	);
	assert.equal(
		uf.resolveUfFiltered({ ibge: '5208707', lat: patrocinio.lat, lng: patrocinio.lng }, mg),
		'GO'
	);
});

test('resolveUfFiltered — sem filtro, e fora das caixas permitidas', function () {
	var mg = uf.parseUfFilter('MG', '');
	var patrocinio = { lat: -18.9375, lng: -46.9856 };
	var manaus = { lat: -3.12, lng: -60.02 };

	// sem filtro, o polígono responde a mesma coisa que com filtro
	assert.equal(uf.resolveUfFiltered(patrocinio, null), 'MG');
	// ponto fora de qualquer caixa permitida cai no comportamento antigo —
	// o descarte é do passesUfFilter, e mentir o rótulo não ajudaria ninguém
	assert.equal(uf.resolveUfFiltered(manaus, mg), 'AM');
	assert.equal(uf.passesUfFilter(mg, 'AM', manaus.lat, manaus.lng), false);
});

test('resolveUfFiltered — região não achata vizinho legítimo', function () {
	var se = uf.parseUfFilter('', 'sudeste');
	// Franca e Rio Claro estão dentro do retângulo de MG; com SP no conjunto
	// permitido, o desempate por área continua acertando.
	assert.equal(uf.resolveUfFiltered({ lat: -20.5386, lng: -47.4008 }, se), 'SP');
	assert.equal(uf.resolveUfFiltered({ lat: -22.4149, lng: -47.5651 }, se), 'SP');
	assert.equal(uf.resolveUfFiltered({ lat: -18.9375, lng: -46.9856 }, se), 'MG');
});

// O rótulo passou a sair do polígono da UF (uf-poly.js) — sem filtro nenhum.
// Estas seis linhas são a tabela do defeito: as três primeiras saíam GO/GO/BA
// pelo retângulo, as três últimas só acertavam por sorte do desempate por área.
test('resolveUf — polígono acerta onde o retângulo errava', function () {
	assert.equal(uf.resolveUf({ lat: -18.9375, lng: -46.9856 }), 'MG'); // Patrocínio
	assert.equal(uf.resolveUf({ lat: -18.9186, lng: -48.2772 }), 'MG'); // Uberlândia
	assert.equal(uf.resolveUf({ lat: -16.735, lng: -43.8617 }), 'MG'); // Montes Claros
	assert.equal(uf.resolveUf({ lat: -20.5386, lng: -47.4008 }), 'SP'); // Franca
	assert.equal(uf.resolveUf({ lat: -16.6799, lng: -49.255 }), 'GO'); // Goiânia
	assert.equal(uf.resolveUf({ lat: -22.4149, lng: -47.5651 }), 'SP'); // Rio Claro
});

// O defeito é geral, não é de MG: qualquer par de UFs com retângulos
// sobrepostos tinha o mesmo problema. Pares de cidades reais, uma de cada lado
// da divisa — Petrolina/Juazeiro são 3 km, Barra do Garças/Aragarças são 1 km.
test('resolveUf — divisas de todas as regiões', function () {
	var pares = [
		// Norte
		[-5.3811, -49.1327, 'PA', 'Marabá'],
		[-7.1911, -48.2072, 'TO', 'Araguaína'],
		[-7.5064, -63.0206, 'AM', 'Humaitá'],
		[-8.7619, -63.9039, 'RO', 'Porto Velho'],
		// Nordeste
		[-9.3891, -40.503, 'PE', 'Petrolina'],
		[-9.4111, -40.4986, 'BA', 'Juazeiro'],
		[-7.0771, -41.4666, 'PI', 'Picos'],
		[-7.2131, -39.3153, 'CE', 'Juazeiro do Norte'],
		// Sul
		[-27.1004, -52.6152, 'SC', 'Chapecó'],
		[-26.229, -52.6707, 'PR', 'Pato Branco'],
		[-29.3353, -49.7269, 'RS', 'Torres'],
		[-29.1092, -49.6122, 'SC', 'Sombrio'],
		// Centro-Oeste
		[-15.8901, -52.2569, 'MT', 'Barra do Garças'],
		[-15.8977, -52.2508, 'GO', 'Aragarças'],
		[-15.7939, -47.8828, 'DF', 'Brasília'],
		[-16.0656, -47.9756, 'GO', 'Valparaíso de Goiás'],
		// Sudeste
		[-21.7622, -41.3181, 'RJ', 'Campos dos Goytacazes'],
		[-21.3897, -42.6964, 'MG', 'Cataguases'],
		[-18.7161, -39.8589, 'ES', 'São Mateus'],
		[-20.5575, -48.5686, 'SP', 'Barretos']
	];
	pares.forEach(function (p) {
		assert.equal(uf.resolveUf({ lat: p[0], lng: p[1] }), p[2], p[3]);
	});
});

// DF é enclave dentro de GO: com retângulo isto dependia de a caixa do DF ser
// menor; com polígono, é o polígono do DF que responde.
test('resolveUf — DF não vira GO', function () {
	assert.equal(uf.resolveUf({ lat: -15.8159, lng: -48.1097 }), 'DF'); // Ceilândia
	assert.equal(uf.resolveUf({ lat: -15.5372, lng: -47.3344 }), 'GO'); // Formosa
});

// Ponto no mar: o polígono não responde, e o retângulo antigo assume — assim
// nada que hoje tem rótulo passa a ser XX por causa desta mudança.
test('resolveUf — ponto no mar cai no retângulo, não em XX', function () {
	// mar ao largo de Cabo Frio: fora de todo polígono, dentro do retângulo do RJ
	assert.equal(uf.ufFromPointPoly(-23.2, -41.5), '');
	assert.equal(uf.resolveUf({ lat: -23.2, lng: -41.5 }), 'RJ');
	// Atlântico, longe de qualquer retângulo
	assert.equal(uf.resolveUf({ lat: -6.0, lng: -30.0 }), 'XX');
	// ilha oceânica pertence à UF de direito
	assert.equal(uf.resolveUf({ lat: -3.8576, lng: -32.4297 }), 'PE'); // Noronha
});

// A nomeação e o filtro passaram a sair da MESMA conta. Feature que o polígono
// põe fora de todas as UFs permitidas é descartada do run — antes o retângulo a
// mantinha e ela era gravada com o nome de uma UF que não era a dela.
test('passesUfFilter — o polígono é a fonte de verdade também do filtro', function () {
	var mg = uf.parseUfFilter('MG', '');
	// Patrocínio: dentro da caixa de GO, dentro do polígono de MG → fica, e como MG
	assert.equal(uf.resolveUfFiltered({ lat: -18.9375, lng: -46.9856 }, mg), 'MG');
	assert.equal(uf.passesUfFilter(mg, 'MG', -18.9375, -46.9856), true);
	// Catalão/GO: dentro do retângulo de MG, fora do polígono → sai do run
	assert.equal(uf.resolveUfFiltered({ lat: -18.1658, lng: -47.9469 }, mg), 'GO');
	assert.equal(uf.passesUfFilter(mg, 'GO', -18.1658, -47.9469), false);
	// e no mar continua valendo o retângulo permitido
	var rj = uf.parseUfFilter('RJ', '');
	assert.equal(uf.passesUfFilter(rj, 'XX', -23.2, -41.5), true);
});

test('extractIbge', function () {
	assert.deepEqual(uf.extractIbge({ 'IBGE:GEOCODIGO': '3550308' }), {
		ibge: '3550308',
		source_tag: 'IBGE:GEOCODIGO'
	});
	assert.deepEqual(uf.extractIbge({ 'ref:IBGE': '3304557' }), {
		ibge: '3304557',
		source_tag: 'ref:IBGE'
	});
});
