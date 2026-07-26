'use strict';

/**
 * Run the test suite filtered by --test-name-pattern.
 *
 * Usage:
 *   node scripts/run-named-tests.js tiny
 *   node scripts/run-named-tests.js "index0 small"
 *   node scripts/run-named-tests.js large20
 *   npm run test:fixture -- tiny
 *   npm run test:fixture -- large20
 */

var path = require('path');
var { spawnSync } = require('child_process');

var pattern = process.argv[2];
if (!pattern) {
	console.error('Usage: node scripts/run-named-tests.js <name-pattern>');
	console.error('Examples:');
	console.error('  node scripts/run-named-tests.js tiny');
	console.error('  node scripts/run-named-tests.js "index0 medium"');
	console.error('  node scripts/run-named-tests.js large20');
	process.exit(1);
}

var root = path.resolve(__dirname, '..');
var files = [
	path.join(root, 'test', 'bz2-fixtures.test.js'),
	path.join(root, 'test', 'index0.test.js'),
	path.join(root, 'test', 'xml-parser.test.js')
];

var args = ['--test', '--test-name-pattern=' + pattern].concat(files);
var result = spawnSync(process.execPath, args, {
	stdio: 'inherit',
	cwd: root
});
process.exit(result.status == null ? 1 : result.status);
