var fs = require('fs');

function makeInStream(in_fd, bufSize, startPos, onBuffer) {
	var stat = fs.fstatSync(in_fd);
	var stream = {
		buffer: new Buffer(bufSize || 4096),
		filePos: startPos,
		totalPos: 0,
		byteCount: 0,
		pos: 0,
		end: 0,
		lastStats: {
			filePos: 0,
			pos: 0,
			end: 0
		},
		fillBufferDisabled: false,
		lastSeek: null,
		_fillBuffer: function() {
			var lastStats = this.lastStats;
			lastStats.filePos = this.filePos;
			lastStats.pos = this.pos;
			lastStats.end = this.end;
			if (this.fillBufferDisabled) {
				console.error(lastStats);
				throw new Error('ReadStream :: _fillBuffer() disabled');
			}
			this.end = fs.readSync(
				in_fd,
				this.buffer,
				0,
				this.buffer.length,
				this.filePos
			);
			this.pos = 0;
			if (this.filePos !== null) {
				this.filePos += this.end;
			}
			onBuffer && onBuffer(this);
		},
		readByte: function() {
			if (this.pos >= this.end) {
				this._fillBuffer();
			}
			if (this.pos < this.end) {
				this.totalPos++;
				this.byteCount++;
				return this.buffer[this.pos++];
			}
			return -1;
		},
		read: function(buffer, bufOffset, length) {
			if (this.pos >= this.end) {
				this._fillBuffer();
			}
			var bytesRead = 0;
			while (bytesRead < length && this.pos < this.end) {
				buffer[bufOffset++] = this.buffer[this.pos++];
				bytesRead++;
			}
			this.totalPos += bytesRead;
			return bytesRead;
		},
		seek: function(seek_pos) {
			var ls = this.lastSeek;
			var start = this.filePos - this.end;
			ls = this.lastSeek = {
				id: ls ? ls.id + 1 : 0,
				seekPos: seek_pos,
				filePos: this.filePos,
				pos: this.pos,
				start: start,
				end: this.end,
				optimized: false,
				afterFilePos: null,
				afterPos: null,
				afterEnd: null
			};
			if (this.filePos != null) {
				if (
					seek_pos >= start &&
					seek_pos <= this.filePos
				) {
					this.pos = seek_pos - start;
					// this.pos = this.filePos - seek_pos;
					ls.optimized = true;
					ls.afterPos = this.pos;
					return; // optimization: reuse current buffer if possible
				}
			}
			this.filePos = seek_pos;
			this.pos = this.end = 0;
			ls.afterFilePos = this.filePos;
			ls.afterPos = this.pos;
			ls.afterEnd = this.end;
		},
		eof: function() {
			if (this.pos >= this.end) {
				this._fillBuffer();
			}
			return !(this.pos < this.end);
		},
		getFilePosition: function() {
			return this.filePos - this.end + this.pos;
		}
	};
	if (stat.size) {
		stream.size = stat.size;
	}
	return stream;
}

module.exports = makeInStream;
