var stream = require('stream');

function LineSplitter(options) {
	options || (options = {});
	options.readableObjectMode = true;
	stream.Transform.call(this, options);
	this._partialLine = '';
}
LineSplitter.prototype = Object.create(stream.Transform.prototype);
LineSplitter.prototype.constructor = LineSplitter;
LineSplitter.prototype._transform = function(chunk, encoding, callback) {
	chunk = this._partialLine + chunk.toString();
	for (;;) {
		var nlpos = chunk.indexOf('\n');
		if (-1 === nlpos) {
			this._partialLine = chunk;
			return callback();
		} else {
			this.push(chunk.substr(0, nlpos+1))
			chunk = chunk.substr(nlpos+1);
		}
	}
};
LineSplitter.prototype._flush = function(callback) {
	callback(null, this._partialLine);
	this._partialLine = null;
};

module.exports = LineSplitter;
