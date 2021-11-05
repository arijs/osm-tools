
function createPercentile(targetLen) {
	var serie = [];
	var slen = 0;
	return {
		push: push,
		lastFold: lastFold,
		getCurrent: getCurrent
	};
	function push(x) {
		for (var i = 0; i < slen; i++) {
			if (x < serie[i][0]) break;
		}
		serie.splice(i, 0, [x, 1]);
		slen++;
		if (slen == (targetLen * 2)) {
			var sbefore = serie.slice();
			fold();
			console.log({before: sbefore, after: serie.slice(), current: serie});
		};
	}
	function fold() {
		debugger;
		var fserie = [];
		var last = slen - 2;
		for (var i = 0; i <= last; i += 2) {
			var fb = 2 * i / last;
			var fa = 2 - fb;
			var a = serie[i], b = serie[i+1];
			fserie.push([
				((a[0] * a[1] * fa) + (b[0] * b[1] * fb)) / (a[1] + b[1]),
				a[1] + b[1]
			]);
		}
		serie = fserie;
		slen = fserie.length;
	}
	function lastFold() {
		var total = 0;
		for (var i = 0; i < slen; i++) {
			total += serie[i][1];
		}
		var fserie = [];
		var ftarget = total / targetLen;
		var fremain = ftarget;
		var fcurrent = [0, 0];
		for (i = 0; i < slen; i++) {
			var fitem = serie[i];
			var part1 = fcurrent[0] + fitem[0];
			var part2 = fcurrent[1] + fitem[1];
			while (part2 > fremain) {
				var diff = part2 - fremain;
				var fpush1 = part1 * fremain / part2;
				var fpush2 = part2 - diff;
				fserie.push([fpush1, fpush2]);
				part1 -= fpush1;
				part2 -= fpush2;
				fremain = ftarget;
			}
			fcurrent[0] = part1;
			fcurrent[1] = part2;
		}
		if (fserie.length == (targetLen - 1)) {
			fserie.push(fcurrent);
			fcurrent[0] = 0;
			fcurrent[1] = 0;
		}
		return fserie;
	}
	function getCurrent() {
		return serie;
	}
}