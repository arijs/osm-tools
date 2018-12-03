var fs = require('fs');

function makeOutStream(out_fd, bufSize) {
	return {
		buffer: new Buffer(bufSize || 4096),
		pos: 0,
		flush: function() {
			fs.writeSync(out_fd, this.buffer, 0, this.pos);
			this.pos = 0;
		},
		writeByte: function(byte) {
			if (this.pos >= this.buffer.length) {
				this.flush();
			}
			this.buffer[this.pos++] = byte;
		}
	};
}

module.exports = makeOutStream;
