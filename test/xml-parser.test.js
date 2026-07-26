'use strict';

var test = require('node:test');
var assert = require('node:assert/strict');
var { XMLParser } = require('@arijs/stream-xml-parser');
var {
	parseXmlString,
	parseXmlChunks,
	countOpen,
	countClose
} = require('./helpers');

test('XMLParser is constructible from @arijs/stream-xml-parser', function () {
	var seen = [];
	var xp = new XMLParser({
		event: function (ev) {
			seen.push(ev.name);
		}
	});
	xp.end('<a/>');
	assert.ok(seen.indexOf('startTag') !== -1);
	assert.ok(seen.indexOf('endTag') !== -1);
	assert.ok(seen.indexOf('endStream') !== -1);
});

test('parses a minimal OSM document with tags and attributes', async function () {
	var xml =
		'<?xml version="1.0" encoding="UTF-8"?>' +
		'<osm version="0.6">' +
		'<node id="1" lat="-23.5" lon="-46.6">' +
		'<tag k="name" v="Praça"/>' +
		'</node>' +
		'</osm>';

	var events = await parseXmlString(xml);

	var instruction = events.find(function (e) {
		return e.event === 'instruction';
	});
	assert.ok(instruction, 'expected xml instruction');
	assert.ok(/xml/i.test(instruction.text));

	assert.equal(countOpen(events, 'osm'), 1);
	assert.equal(countOpen(events, 'node'), 1);
	assert.equal(countOpen(events, 'tag'), 1);

	var nodeOpen = events.find(function (e) {
		return e.event === 'open' && e.name === 'node';
	});
	assert.equal(nodeOpen.attrs.id, '1');
	assert.equal(nodeOpen.attrs.lat, '-23.5');
	assert.equal(nodeOpen.attrs.lon, '-46.6');

	var tagOpen = events.find(function (e) {
		return e.event === 'open' && e.name === 'tag';
	});
	assert.equal(tagOpen.attrs.k, 'name');
	assert.equal(tagOpen.attrs.v, 'Praça');

	assert.equal(countClose(events, 'tag'), 1);
	assert.equal(countClose(events, 'osm'), 1);
});

test('self-closing elements yield open + close', async function () {
	var events = await parseXmlString('<root><empty id="x"/><item/></root>');
	assert.equal(countOpen(events, 'empty'), 1);
	assert.equal(countOpen(events, 'item'), 1);
	assert.equal(countClose(events, 'empty'), 1);
	assert.equal(countClose(events, 'item'), 1);
});

test('emits text and cdata events', async function () {
	var events = await parseXmlString(
		'<root>hello<![CDATA[world & more]]></root>'
	);
	assert.ok(
		events.some(function (t) {
			return t.event === 'text' && t.text === 'hello';
		})
	);
	var cdatas = events.filter(function (e) {
		return e.event === 'cdata';
	});
	assert.equal(cdatas.length, 1);
	assert.equal(cdatas[0].cdata, 'world & more');
});

test('streams chunks into XMLParser without losing tags across boundaries', async function () {
	var parts = [
		'<?xml version="1.0"?>',
		'<osm>',
		'<node id="',
		'42"',
		' lat="1" lon="2">',
		'<tag k="a" v="b"/>',
		'</node></osm>'
	];

	var events = await parseXmlChunks(parts);

	assert.equal(countOpen(events, 'node'), 1);
	var node = events.find(function (e) {
		return e.event === 'open' && e.name === 'node';
	});
	assert.equal(node.attrs.id, '42');
	assert.equal(node.attrs.lat, '1');
	assert.equal(countOpen(events, 'tag'), 1);
});
