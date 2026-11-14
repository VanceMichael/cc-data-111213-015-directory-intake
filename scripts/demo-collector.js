// 持续采集演示：本地写入器主动制造通知缺失、路径复用、复制中变更、
// 硬链接归档、链接逃逸与长时间不停写，验证采集器在重启补扫后
// 既不丢资料，也不把硬链接算成两份不同内容。
import assert from 'node:assert/strict';
import { mkdtemp, rm, mkdir, writeFile, appendFile, link, symlink, rename, readFile, readdir, readlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Collector } from '../src/collector.js';
import { Archive, digest } from '../src/archive.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitFor(condition, description, timeoutMs = 10000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await condition();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`等待超时: ${description}`);
    await sleep(25);
  }
}

// 只读核验：对受观测目录做完整快照（普通文件内容摘要 + 符号链接目标），
// 采集器运行前后必须完全一致。
async function snapshotTree(root) {
  const snapshot = {};
  async function visit(dir) {
    for (const entry of await readdir(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name);
      const rel = path.slice(root.length + 1);
      if (entry.isDirectory()) await visit(path);
      else if (entry.isFile()) snapshot[rel] = `file:${digest(await readFile(path))}`;
      else if (entry.isSymbolicLink()) snapshot[rel] = `link:${await readlink(path)}`;
      else snapshot[rel] = 'other';
    }
  }
  await visit(root);
  return snapshot;
}

const tmp = await mkdtemp(join(tmpdir(), 'collector-demo-'));
const root = join(tmp, 'instrument');
const archiveDir = join(tmp, 'archive');
const outside = join(tmp, 'outside');
const statePath = join(tmp, 'state', 'collector-state.json');
await mkdir(root, { recursive: true });
await mkdir(outside, { recursive: true });
await writeFile(join(outside, 'secret.txt'), '授权根目录之外的内容\n');

const note = (message) => console.log(message);
const archive = new Archive(archiveDir);

// 复制中变更的触发器：采集器读到 report-c.csv 的第一个分块时，写入器立刻追加。
let mutatedOnce = false;
const hooks = { onChunk: async (item) => {
  if (!mutatedOnce && item.path === 'report-c.csv') {
    mutatedOnce = true;
    await appendFile(join(root, 'report-c.csv'), 'P9,9.9\n');
    note('【写入器】在采集器复制 report-c.csv 期间原地追加');
  }
} };

const collectorOptions = {
  root, archive: archiveDir, state: statePath,
  watch: false,          // 模拟通知完全缺失：只能依靠周期重扫
  scanIntervalMs: 150, quietMs: 150, maxWaitMs: 900,
  chunkBytes: 2048, hooks,
  log: (message) => note(`【采集器】${message}`),
};

const running = [];
try {
  note('—— 阶段一：通知完全缺失下的持续采集 ——');
  const collector = await new Collector(collectorOptions).start();
  running.push(collector);

  // 临时文件改名：先写 .tmp 再改名，.tmp 存在时间远短于静默期，不应被领取。
  await writeFile(join(root, 'report-a.csv.tmp'), 'probe,value\nA1,1.0\n');
  await appendFile(join(root, 'report-a.csv.tmp'), 'A2,2.0\n');
  await rename(join(root, 'report-a.csv.tmp'), join(root, 'report-a.csv'));
  note('【写入器】report-a.csv 经临时文件改名到位');

  // 原地追加：同一 inode 上连续生长，只有最终稳定内容可以被领取。
  await writeFile(join(root, 'report-b.csv'), 'probe,value\nB1,1.0\n');
  await sleep(40);
  await appendFile(join(root, 'report-b.csv'), 'B2,2.0\n');
  await sleep(40);
  await appendFile(join(root, 'report-b.csv'), 'B3,3.0\n');
  note('【写入器】report-b.csv 原地追加三次');

  // 复制中变更的目标：足够大，保证采集器读多个分块。
  await writeFile(join(root, 'report-c.csv'),
    'probe,value\n' + Array.from({ length: 900 }, (_, i) => `C${i},${i}.5\n`).join(''));
  note('【写入器】report-c.csv 就位（将在复制期间被变更）');

  // 硬链接归档：同一内容的两个路径。
  await writeFile(join(root, 'report-d.csv'), 'probe,value\nD1,4.0\n');
  await link(join(root, 'report-d.csv'), join(root, 'report-d-archive.csv'));
  note('【写入器】report-d.csv 与其硬链接 report-d-archive.csv 就位');

  // 链接逃逸：指向授权根目录之外的符号链接，不得跟随。
  await symlink('../outside/secret.txt', join(root, 'linked-secret.csv'));
  await symlink('../outside', join(root, 'linked-dir'));
  note('【写入器】放入指向根目录之外的符号链接');

  // 长时间不停写：超过 maxWaitMs 仍在写，应进入待人工检查；停笔后应被领取。
  let streaming = true;
  const streamWriter = (async () => {
    await writeFile(join(root, 'stream.csv'), 'probe,value\n');
    for (let i = 0; streaming && i < 40; i += 1) {
      await appendFile(join(root, 'stream.csv'), `S${i},${i}.1\n`);
      await sleep(50);
    }
  })();

  const claimsOf = (c) => c.ledger.claims;
  const bySource = (c, name) => Object.values(claimsOf(c)).find((claim) => claim.sources.includes(name));

  await waitFor(() => bySource(collector, 'report-a.csv')
    && bySource(collector, 'report-b.csv') && bySource(collector, 'report-c.csv')
    && bySource(collector, 'report-d.csv'), '前四份资料被领取');
  note('【采集器】report-a/b/c/d 已领取');

  // 等待长写文件先进入待人工检查。
  await waitFor(() => Object.values(collector.ledger.items)
    .some((item) => item.path === 'stream.csv' && item.flags.length > 0), 'stream.csv 被标记待人工检查');
  note('【采集器】stream.csv 持续写入超时，已标记待人工检查（继续观察，不丢资料）');
  streaming = false;
  await streamWriter;
  await waitFor(() => bySource(collector, 'stream.csv'), 'stream.csv 停笔后被领取');
  note('【采集器】stream.csv 停笔稳定后已领取，人工检查标记保留在台账历史中');

  // —— 断言阶段一 ——
  const receiptList = await archive.receipts();
  const receiptsFor = (name) => receiptList.filter((r) => r.metadata && r.metadata.source === name);

  const aBytes = await readFile(join(root, 'report-a.csv'));
  assert.equal(receiptsFor('report-a.csv').length, 1, 'report-a.csv 只领取一次');
  assert.equal((await archive.read(receiptsFor('report-a.csv')[0].sha256)).toString(), aBytes.toString());
  assert.ok(!receiptList.some((r) => r.metadata.source.endsWith('.tmp')), '临时文件不得被领取');

  const bBytes = await readFile(join(root, 'report-b.csv'));
  assert.equal(receiptsFor('report-b.csv').length, 1, 'report-b.csv 只领取一次');
  assert.equal((await archive.read(receiptsFor('report-b.csv')[0].sha256)).toString(), bBytes.toString(),
    '原地追加的中间内容不得被领取，最终内容必须完整');

  const cBytes = await readFile(join(root, 'report-c.csv'));
  assert.equal(receiptsFor('report-c.csv').length, 1, 'report-c.csv 只领取一次');
  assert.equal((await archive.read(receiptsFor('report-c.csv')[0].sha256)).toString(), cBytes.toString(),
    '复制期间变更后，归档内容必须等于最终稳定内容，不得拼接');
  const cItem = Object.values(collector.ledger.items).find((item) => item.path === 'report-c.csv');
  assert.ok(cItem.aborts >= 1, 'report-c.csv 应记录至少一次撤销');
  assert.ok(cItem.history.some((h) => h.reason.includes('撤销')), '台账应记录撤销原因');
  note('【采集器】report-c.csv 复制期间变更被撤销一次，最终领取内容与源文件一致');

  const dDigest = digest(await readFile(join(root, 'report-d.csv')));
  const dClaim = collector.ledger.claims[dDigest];
  assert.ok(dClaim, 'report-d.csv 内容已领取');
  assert.deepEqual([...dClaim.sources].sort(), ['report-d-archive.csv', 'report-d.csv'],
    '硬链接两个路径应挂在同一份领取记录上');
  assert.equal(receiptList.filter((r) => r.sha256 === dDigest).length, 1,
    '硬链接不得产生第二份回执');
  note('【采集器】硬链接归档只算一份内容，两个来源路径都记录在同一领取上');

  assert.ok(!receiptList.some((r) => JSON.stringify(r).includes('授权根目录之外')),
    '根目录之外的内容不得进入归档');
  const skipped = collector.ledger.stats.skipped || [];
  assert.ok(skipped.some((s) => s.path.endsWith('linked-secret.csv') && s.reason === 'symlink-not-followed'));
  assert.ok(skipped.some((s) => s.path.endsWith('linked-dir') && s.reason === 'symlink-not-followed'));
  note('【采集器】指向根目录之外的符号链接未被跟随');

  // 路径复用：新内容经临时文件改名覆盖同一路径（新 inode）。
  // 旧内容仍由硬链接 report-d-archive.csv 持有，两份内容都应在案。
  await writeFile(join(root, 'report-d.csv.next'), 'probe,value\nD1,4.0\nD2,4.1\n');
  await rename(join(root, 'report-d.csv.next'), join(root, 'report-d.csv'));
  note('【写入器】report-d.csv 同路径改名覆盖新内容（旧内容仍由硬链接持有）');
  const d2Digest = digest(await readFile(join(root, 'report-d.csv')));
  await waitFor(() => collector.ledger.claims[d2Digest], 'report-d.csv 新内容被领取');
  assert.deepEqual(collector.ledger.claims[d2Digest].sources, ['report-d.csv']);
  assert.deepEqual([...collector.ledger.claims[dDigest].sources].sort(),
    ['report-d-archive.csv', 'report-d.csv'], '旧内容的领取记录保持不变');
  const dArchiveItem = Object.values(collector.ledger.items)
    .find((item) => item.path === 'report-d-archive.csv');
  assert.equal(dArchiveItem.sha256, dDigest, '硬链接路径仍对应旧内容');
  assert.equal(receiptList.length, (await archive.receipts()).length - 1,
    '路径复用只新增一份回执');
  note('【采集器】同一路径的新内容作为新资料领取，旧内容记录不受影响');

  // —— 阶段二：重启补扫 ——
  note('—— 阶段二：重启补扫 ——');
  const receiptsBeforeStop = (await archive.receipts()).length;
  await collector.stop();
  await writeFile(join(root, 'report-e.csv'), 'probe,value\nE1,5.0\n');
  note('【写入器】采集器停止期间写入 report-e.csv（通知与重扫都缺席）');

  const restarted = await new Collector({ ...collectorOptions, hooks: {} }).start();
  running.push(restarted);
  await waitFor(() => restarted.ledger.claims[digest(Buffer.from('probe,value\nE1,5.0\n'))],
    'report-e.csv 在重启补扫后被领取');
  const receiptsAfterRestart = await archive.receipts();
  assert.equal(receiptsAfterRestart.length, receiptsBeforeStop + 1,
    '重启补扫只新增 report-e.csv 一份回执，不重复领取已有内容');
  assert.equal(receiptsAfterRestart.filter((r) => r.sha256 === dDigest).length, 1,
    '硬链接内容在重启后仍然只算一份');
  for (const receipt of receiptsAfterRestart) await archive.read(receipt.sha256);
  note('【采集器】重启补扫领取 report-e.csv，既有领取零重复，全部对象完整性核验通过');

  // —— 阶段三：只读核验 ——
  const beforeScan = await snapshotTree(root);
  await restarted.scan('read-only-check');
  const afterScan = await snapshotTree(root);
  assert.deepEqual(afterScan, beforeScan, '采集器不得改动受观测目录的任何内容');
  assert.ok(!Object.keys(afterScan).some((name) => name.includes('collector-state')
    || name === 'objects' || name === 'receipts'), '采集器不得在受观测目录内写状态或归档');
  note('【采集器】受观测目录在采集前后逐字节一致（只读约束）');

  await restarted.stop();
  const summary = restarted.summary();
  note('—— 最终台账 ——');
  console.log(JSON.stringify({ summary, items: restarted.items() }, null, 2));
  assert.equal(summary.review.length, 0, '演示结束时不应有待人工检查残留');
  note('演示完成：通知缺失下不丢资料，复制中变更被撤销，硬链接只算一份，重启补扫无重复。');
} finally {
  for (const collector of running.splice(0)) await collector.stop().catch(() => {});
  await rm(tmp, { recursive: true, force: true });
}
