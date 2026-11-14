import { readFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { Archive } from './archive.js';
import { Collector } from './collector.js';
import { inspect, sample } from './domain.js';

const [command = 'help', ...args] = process.argv.slice(2);

function parseFlags(list) {
  const options = { positional: [] };
  const withValue = new Set(['--state', '--quiet-ms', '--scan-ms', '--max-wait-ms', '--debounce-ms']);
  for (let index = 0; index < list.length; index += 1) {
    const arg = list[index];
    if (withValue.has(arg)) { options[arg.slice(2)] = list[++index]; }
    else if (arg === '--once' || arg === '--no-watch' || arg === '--watch') options[arg.slice(2)] = true;
    else options.positional.push(arg);
  }
  return options;
}

async function collect(list) {
  const options = parseFlags(list);
  const [root, archiveDir] = options.positional;
  if (!root) throw new Error('需要观测目录：npm start -- collect <目录> <归档目录> [--once] [--no-watch]');
  const collector = new Collector({
    root: resolve(root),
    archive: resolve(archiveDir || '.local/archive'),
    state: options.state,
    quietMs: Number(options['quiet-ms'] ?? 1000),
    scanIntervalMs: options.once ? 0 : Number(options['scan-ms'] ?? 5000),
    maxWaitMs: Number(options['max-wait-ms'] ?? 60000),
    watch: !options.once && !options['no-watch'],
    log: (message) => console.error(`[collector] ${message}`),
  });
  await collector.start();
  if (options.once) {
    // 单次运行：补扫定时器会推进稳定性判定，这里等到没有待定条目为止（有界）。
    for (let round = 0; round < 20; round += 1) {
      const pending = Object.values(collector.ledger.items).some((item) => !item.sticky
        && (item.state === 'observed' || item.state === 'stable' || item.state === 'review'));
      if (!pending) break;
      await new Promise((resolve) => { setTimeout(resolve, Math.max(50, collector.quietMs * 1.5)); });
    }
    await collector.stop();
    console.log(JSON.stringify({ summary: collector.summary(), items: collector.items() }, null, 2));
    return;
  }
  console.error(`[collector] 持续观测 ${collector.rootReal}，Ctrl-C 结束`);
  await new Promise((done) => {
    process.once('SIGINT', done);
    process.once('SIGTERM', done);
  });
  await collector.stop();
  console.log(JSON.stringify({ summary: collector.summary(), items: collector.items() }, null, 2));
}

async function run() {
  if (command === 'help') {
    console.log(JSON.stringify({ usage: 'npm start -- <命令> [参数]',
      commands: ['inspect <资料路径>', 'capture <资料路径> [归档目录]', 'list [归档目录]',
        'collect <观测目录> <归档目录> [--once] [--no-watch] [--quiet-ms N] [--scan-ms N] [--max-wait-ms N]',
        'demo'] }, null, 2));
  } else if (command === 'inspect') {
    const [file] = args;
    if (!file) throw new Error('需要资料路径');
    console.log(JSON.stringify(await inspect(resolve(file)), null, 2));
  } else if (command === 'capture') {
    const [file, directory = '.local/archive'] = args;
    if (!file) throw new Error('需要资料路径');
    const inventory = await inspect(resolve(file));
    const receipt = await new Archive(directory).accept(await readFile(file),
      { source: resolve(file), inventory });
    console.log(JSON.stringify(receipt, null, 2));
  } else if (command === 'list') {
    const [directory = '.local/archive'] = args;
    console.log(JSON.stringify(await new Archive(directory).receipts(), null, 2));
  } else if (command === 'collect') {
    await collect(args);
  } else if (command === 'demo') {
    const temporary = await mkdtemp(join(tmpdir(), 'intake-'));
    try {
      const inventory = await inspect(resolve(sample));
      const archive = new Archive(temporary);
      const receipt = await archive.accept(await readFile(sample), { source: sample, inventory });
      console.log(JSON.stringify({ inventory, receipt, verified: (await archive.read(receipt.sha256)).length }, null, 2));
    } finally { await rm(temporary, { recursive: true, force: true }); }
  } else { throw new Error('支持 inspect、capture、list、collect、demo'); }
}
run().catch(error => { console.error(error.message); process.exitCode = 1; });
