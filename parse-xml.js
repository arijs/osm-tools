var stream = require('stream');
var XMLStream = require('node-xml-stream-parser');

var OPEN_TAG = 'opentag';
var CLOSE_TAG = 'closetag';
var TEXT = 'text';
var CDATA = 'cdata';
var INSTRUCTION = 'instruction';
var ERROR = 'error';

function ParseXML(options) {
	options || (options = {});
	options.readableObjectMode = true;
	options.objectMode = true;
	stream.Duplex.call(this, options);
	var self = this;
	var parser = this._parser = new XMLStream();
	parser.on(OPEN_TAG, function(name, attrs) {
		self.push({ type: OPEN_TAG, name: name, attrs: attrs });
	});
	parser.on(CLOSE_TAG, function(name) {
		self.push({ type: CLOSE_TAG, name: name });
	});
	parser.on(TEXT, function(text) {
		self.push({ type: TEXT, text: text });
	});
	parser.on(CDATA, function(cdata) {
		self.push({ type: CDATA, cdata: cdata });
	});
	parser.on(INSTRUCTION, function(name, attrs) {
		self.push({ type: INSTRUCTION, name: name, attrs: attrs });
	});
	parser.on(ERROR, function(error) {
		self.push({ type: ERROR, error: error });
	});
	parser.on('finish', function() {
		self.end();
	});
}

ParseXML.OPEN_TAG = OPEN_TAG;
ParseXML.CLOSE_TAG = CLOSE_TAG;
ParseXML.TEXT = TEXT;
ParseXML.CDATA = CDATA;
ParseXML.INSTRUCTION = INSTRUCTION;
ParseXML.ERROR = ERROR;

ParseXML.prototype = Object.create(stream.Duplex.prototype);
ParseXML.prototype.constructor = ParseXML;
ParseXML.prototype._read = function() {};
ParseXML.prototype._write = function(chunk, encoding, callback) {
	this._parser.write(chunk);
	callback();
};
ParseXML.prototype._final = function(callback) {
	this._parser.end();
};

module.exports = ParseXML;
