import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
function run(command, args, env = process.env) {
  const result = spawnSync(command, args, { cwd: root, env, stdio: 'inherit', timeout: 240000 });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
}
function requireVersion(command, args, expected) {
  const result = spawnSync(command, args, { encoding: 'utf8', timeout: 10000 });
  if (result.status !== 0 || !result.stdout.startsWith(expected)) {
    throw new Error(`Build requires ${expected}; got ${result.stdout || result.error || result.stderr}`);
  }
}
requireVersion('rustc', ['--version'], 'rustc 1.99.1 ');
requireVersion('wasm-pack', ['--version'], 'wasm-pack 0.13.1');
run('wasm-pack', [
  'build', 'crates/wasm', '--target', 'web', '--release', '--no-opt',
  '--out-dir', '../../packages/sql/pkg', '--locked',
], {
  ...process.env,
  CARGO_BUILD_JOBS: process.env.CARGO_BUILD_JOBS ?? '2',
  RUSTFLAGS: '-C link-arg=-zstack-size=16777216',
});
