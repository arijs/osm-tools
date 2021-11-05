var stream = require('stream');
var str_dec = require('string_decoder');

function StreamingDecoder(options) {
	stream.Transform.call(this, options);
	this._decoder = new str_dec.StringDecoder(options && options.defaultEncoding);
}
StreamingDecoder.prototype = Object.create(stream.Transform.prototype);
StreamingDecoder.prototype.constructor = StreamingDecoder;
StreamingDecoder.prototype._transform = function(chunk, encoding, callback) {
	if (encoding === 'buffer') {
		chunk = this._decoder.write(chunk);
	}
	callback(null, chunk);
};
StreamingDecoder.prototype._flush = function(callback) {
	callback(null, this._decoder.end());
};

module.exports = StreamingDecoder;
