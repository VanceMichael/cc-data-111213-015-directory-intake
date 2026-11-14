import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, mkdir, writeFile, appendFile, link, symlink, rename, readFile, readdir, readlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Collector, openVerifiedRegular, STATES } from '../src/collector.js';
import { Archive, digest } from '../src/archive.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function makeEnv() {
  const tmp = await mkdtemp(join(tmpdir(), 'collector-test-'));
  const root = join(tmp, 'instrument');
  const archiveDir = join(tmp, 'archive');
  const outside = join(tmp, 'outside');
  await mkdir(root, { recursive: true });
  await mkdir(outside, { recursive: true });
  const archive = new Archive(archiveDir);
  return { tmp, root, archiveDir, outside, archive,
    state: join(tmp, 'state', 'collector-state.json'),
    cleanup: () => rm(tmp, { recursive: true, force: true }) };
}

const baseOptions = (env, extra = {}) => ({
  root: env.root, archive: env.archiveDir, state: env.state,
  watch: false, scanIntervalMs: 0, quietMs: 80, maxWaitMs: 60000,
  log: () => {}, ...extra,
});

// 启动采集器并登记，测试结束（无论成败）统一停止，避免定时器挂住进程。
async function started(env, running, extra = {}) {
  const collector = await new Collector(baseOptions(env, extra)).start();
  running.push(collector);
  return collector;
}

async function stopAll(running) {
  for (const collector of running.splice(0)) await collector.stop();
}

async function waitFor(condition, description, timeoutMs = 8000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await condition();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`等待超时: ${description}`);
    await sleep(20);
  }
}

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

const itemByPath = (collector, path) => Object.values(collector.ledger.items)
  .find((item) => item.path === path);

test('临时文件改名与原地追加：只有稳定后的完整内容被领取', async () => {
  const env = await makeEnv();
  const running = [];
  try {
    const collector = await started(env, running);
    await writeFile(join(env.root, 'a.csv.tmp'), 'h\n1\n');
    await appendFile(join(env.root, 'a.csv.tmp'), '2\n');
    await rename(join(env.root, 'a.csv.tmp'), join(env.root, 'a.csv'));
    await writeFile(join(env.root, 'b.csv'), 'h\n1\n');
    await sleep(30);
    await appendFile(join(env.root, 'b.csv'), '2\n');
    await sleep(30);
    await appendFile(join(env.root, 'b.csv'), '3\n');
    await collector.scan('t1');
    await sleep(140);
    await collector.scan('t2');
    const receipts = await env.archive.receipts();
    const sources = receipts.map((r) => r.metadata.source).sort();
    assert.deepEqual(sources, ['a.csv', 'b.csv']);
    assert.equal((await env.archive.read(receipts.find((r) => r.metadata.source === 'a.csv').sha256)).toString(), 'h\n1\n2\n');
    assert.equal((await env.archive.read(receipts.find((r) => r.metadata.source === 'b.csv').sha256)).toString(), 'h\n1\n2\n3\n');
    assert.equal(itemByPath(collector, 'a.csv').state, 'claimed');
    assert.equal(itemByPath(collector, 'b.csv').state, 'claimed');
  } finally { await stopAll(running); await env.cleanup(); }
});

test('复制期间源文件变化：撤销本次领取，不拼接新旧内容', async () => {
  const env = await makeEnv();
  const running = [];
  try {
    let mutated = false;
    const hooks = { onChunk: async (item) => {
      if (!mutated && item.path === 'c.csv') {
        mutated = true;
        await appendFile(join(env.root, 'c.csv'), 'tail,9\n');
      }
    } };
    const collector = await started(env, running, { hooks, chunkBytes: 512 });
    await writeFile(join(env.root, 'c.csv'),
      Array.from({ length: 400 }, (_, i) => `row,${i}\n`).join(''));
    await collector.scan('t1');
    await waitFor(() => itemByPath(collector, 'c.csv').state === 'claimed'
      ? itemByPath(collector, 'c.csv') : null, 'c.csv 撤销后最终领取');
    const item = itemByPath(collector, 'c.csv');
    assert.ok(item.aborts >= 1, '应记录至少一次撤销');
    assert.ok(item.history.some((h) => h.reason.includes('撤销')));
    const finalBytes = await readFile(join(env.root, 'c.csv'));
    const archived = await env.archive.read(item.sha256);
    assert.equal(archived.toString(), finalBytes.toString(),
      '归档内容必须等于最终稳定内容，不得是新旧拼接');
    const receipts = await env.archive.receipts();
    assert.equal(receipts.filter((r) => r.metadata.source === 'c.csv').length, 1);
  } finally { await stopAll(running); await env.cleanup(); }
});

test('硬链接只算一份内容；同路径改名覆盖产生新资料且旧内容仍在案', async () => {
  const env = await makeEnv();
  const running = [];
  try {
    const collector = await started(env, running);
    await writeFile(join(env.root, 'd.csv'), 'v1\n');
    await link(join(env.root, 'd.csv'), join(env.root, 'd-archive.csv'));
    await collector.scan('t1');
    await sleep(140);
    await collector.scan('t2');
    const d1 = digest(Buffer.from('v1\n'));
    assert.ok(collector.ledger.claims[d1]);
    assert.deepEqual([...collector.ledger.claims[d1].sources].sort(), ['d-archive.csv', 'd.csv']);
    assert.equal((await env.archive.receipts()).filter((r) => r.sha256 === d1).length, 1);

    // 同一路径改名覆盖：新 inode 新内容；旧内容仍由硬链接持有。
    await writeFile(join(env.root, 'd.csv.next'), 'v2\n');
    await rename(join(env.root, 'd.csv.next'), join(env.root, 'd.csv'));
    await collector.scan('t3');
    await sleep(140);
    await collector.scan('t4');
    const d2 = digest(Buffer.from('v2\n'));
    assert.deepEqual(collector.ledger.claims[d2].sources, ['d.csv']);
    assert.deepEqual([...collector.ledger.claims[d1].sources].sort(), ['d-archive.csv', 'd.csv']);
    assert.equal(itemByPath(collector, 'd-archive.csv').sha256, d1);

    // 同一路径截断重写：inode 不变，内容变化同样被识别为新资料。
    await writeFile(join(env.root, 'd.csv'), 'v3\n');
    await collector.scan('t5');
    await sleep(140);
    await collector.scan('t6');
    const d3 = digest(Buffer.from('v3\n'));
    assert.deepEqual(collector.ledger.claims[d3].sources, ['d.csv']);
    const receipts = await env.archive.receipts();
    assert.equal(receipts.length, 3, '三份不同内容各一份回执');
    assert.equal((await env.archive.read(d1)).toString(), 'v1\n');
    assert.equal((await env.archive.read(d3)).toString(), 'v3\n');
  } finally { await stopAll(running); await env.cleanup(); }
});

test('重启补扫：停机期间写入不丢、已有内容不重复；台账丢失时从回执重建', async () => {
  const env = await makeEnv();
  const running = [];
  try {
    const first = await started(env, running);
    await writeFile(join(env.root, 'x.csv'), 'x\n');
    await link(join(env.root, 'x.csv'), join(env.root, 'x-archive.csv'));
    await first.scan('t1');
    await sleep(140);
    await first.scan('t2');
    const xDigest = digest(Buffer.from('x\n'));
    assert.deepEqual([...first.ledger.claims[xDigest].sources].sort(), ['x-archive.csv', 'x.csv']);
    const receiptsBefore = (await env.archive.receipts()).length;
    await first.stop();
    running.length = 0;

    // 停机期间写入：通知与重扫都缺席。
    await writeFile(join(env.root, 'e.csv'), 'e\n');
    const second = await started(env, running);
    await sleep(140);
    await second.scan('t3');
    assert.ok(second.ledger.claims[digest(Buffer.from('e\n'))], '停机期间写入的文件被补扫领取');
    assert.equal((await env.archive.receipts()).length, receiptsBefore + 1, '只新增一份回执');
    assert.equal((await env.archive.receipts()).filter((r) => r.sha256 === xDigest).length, 1,
      '硬链接内容重启后仍只算一份');
    await second.stop();
    running.length = 0;

    // 台账丢失（模拟崩溃后状态损坏被隔离）：从回执重建去重集合，不重复领取。
    await rm(env.state, { force: true });
    const third = await started(env, running);
    await sleep(140);
    await third.scan('t4');
    assert.equal((await env.archive.receipts()).length, receiptsBefore + 1,
      '台账重建后不得重复领取已有内容');
    assert.ok(third.ledger.claims[xDigest], '去重集合从回执重建');
  } finally { await stopAll(running); await env.cleanup(); }
});

test('符号链接不跟随，链接逃逸与根目录替换被拒绝', async () => {
  const env = await makeEnv();
  const running = [];
  try {
    await writeFile(join(env.outside, 'secret.txt'), 'secret\n');
    await symlink('../outside/secret.txt', join(env.root, 'linked.csv'));
    await symlink('../outside', join(env.root, 'dirlink'));
    await writeFile(join(env.root, 'ok.csv'), 'ok\n');

    // 单元级：符号链接本身、以及经由符号链接目录的路径，都不得被读取。
    const collector = await started(env, running);
    await assert.rejects(openVerifiedRegular(join(env.root, 'linked.csv'), collector.rootReal),
      (error) => error.code === 'not-regular');
    await assert.rejects(openVerifiedRegular(join(env.root, 'dirlink', 'secret.txt'), collector.rootReal),
      (error) => error.code === 'escapes-root');
    const good = await openVerifiedRegular(join(env.root, 'ok.csv'), collector.rootReal);
    await good.handle.close();

    await collector.scan('t1');
    await sleep(140);
    await collector.scan('t2');
    const receipts = await env.archive.receipts();
    assert.deepEqual(receipts.map((r) => r.metadata.source), ['ok.csv']);
    const skipped = collector.ledger.stats.skipped || [];
    assert.ok(skipped.some((s) => s.path.endsWith('linked.csv') && s.reason === 'symlink-not-followed'));
    assert.ok(skipped.some((s) => s.path.endsWith('dirlink') && s.reason === 'symlink-not-followed'));

    // 授权根目录被替换成指向别处的符号链接：重扫必须失败关闭，不得读取任何内容。
    await rename(env.root, `${env.root}.moved`);
    await symlink(env.outside, env.root);
    const summary = await collector.scan('t3');
    assert.ok(summary.rootMismatches >= 1, '根目录解析变化应被记录');
    assert.equal((await env.archive.receipts()).length, 1, '根目录被替换期间不得领取任何内容');
    await rm(env.root);
    await rename(`${env.root}.moved`, env.root);
    await collector.scan('t4');
    assert.equal(itemByPath(collector, 'ok.csv').state, 'claimed');
  } finally { await stopAll(running); await env.cleanup(); }
});

test('只读约束：归档与状态不得在受观测目录内，采集不改动目录内容', async () => {
  const env = await makeEnv();
  const running = [];
  try {
    await writeFile(join(env.root, 'keep.csv'), 'keep\n');
    await assert.rejects(new Collector(baseOptions(env, {
      archive: join(env.root, 'archive-inside'),
    })).start(), /只读/);
    await assert.rejects(new Collector(baseOptions(env, {
      state: join(env.root, 'state.json'),
    })).start(), /只读/);

    const before = await snapshotTree(env.root);
    const collector = await started(env, running);
    await collector.scan('t1');
    await sleep(140);
    await collector.scan('t2');
    assert.equal(itemByPath(collector, 'keep.csv').state, 'claimed');
    assert.deepEqual(await snapshotTree(env.root), before, '受观测目录逐字节不变');
  } finally { await stopAll(running); await env.cleanup(); }
});

test('长时间不停写：超时进入待人工检查，停笔稳定后仍被领取且标记保留', async () => {
  const env = await makeEnv();
  const running = [];
  try {
    const collector = await started(env, running, { maxWaitMs: 400 });
    let streaming = true;
    const writer = (async () => {
      await writeFile(join(env.root, 'stream.csv'), 'h\n');
      for (let i = 0; streaming && i < 30; i += 1) {
        await appendFile(join(env.root, 'stream.csv'), `${i}\n`);
        await sleep(40);
      }
    })();
    const flagged = await waitFor(async () => {
      await collector.scan('poll');
      return Object.values(collector.ledger.items)
        .find((item) => item.path === 'stream.csv' && item.state === 'review') || null;
    }, '持续写入超时进入待人工检查');
    assert.ok(flagged.flags.some((f) => f.includes('未稳定')));
    streaming = false;
    await writer;
    await waitFor(async () => {
      await collector.scan('poll');
      return itemByPath(collector, 'stream.csv').state === 'claimed';
    }, '停笔后领取');
    const item = itemByPath(collector, 'stream.csv');
    assert.ok(item.flags.length > 0, '人工检查标记保留');
    assert.equal((await env.archive.read(item.sha256)).toString(),
      (await readFile(join(env.root, 'stream.csv'))).toString());
  } finally { await stopAll(running); await env.cleanup(); }
});

test('复制期反复变化：达到上限后粘性待人工检查，不再自动领取', async () => {
  const env = await makeEnv();
  const running = [];
  try {
    let active = true;
    const hooks = { onChunk: async (item) => {
      if (active && item.path === 'f.csv') await appendFile(join(env.root, 'f.csv'), 'x\n');
    } };
    const collector = await started(env, running, {
      hooks, chunkBytes: 256, maxCopyAborts: 1,
    });
    await writeFile(join(env.root, 'f.csv'), 'row\n'.repeat(200));
    await collector.scan('t1');
    await waitFor(() => itemByPath(collector, 'f.csv').state === 'review',
      'f.csv 进入待人工检查');
    const item = itemByPath(collector, 'f.csv');
    assert.ok(item.sticky, '复制期反复变化应粘住等待人工');
    assert.equal((await env.archive.receipts()).length, 0, '不得领取拼接内容');
    active = false;
    await sleep(140);
    await collector.scan('t3');
    assert.equal(itemByPath(collector, 'f.csv').state, 'review', '粘性条目不自动恢复');
  } finally { await stopAll(running); await env.cleanup(); }
});

test('目录通知只是提示：通知驱动领取，通知缺失时周期重扫兜底', async () => {
  const env = await makeEnv();
  const running = [];
  try {
    // 通知可用：长周期下仍由通知去抖触发重扫，静默期补扫完成领取。
    const notified = await started(env, running, {
      watch: true, scanIntervalMs: 60000, debounceMs: 30,
    });
    await writeFile(join(env.root, 'n1.csv'), 'n1\n');
    await waitFor(() => itemByPath(notified, 'n1.csv')
      && itemByPath(notified, 'n1.csv').state === 'claimed', '通知驱动的领取');
    assert.ok(notified.ledger.stats.notifications >= 1);
    const notificationsBefore = notified.ledger.stats.notifications;
    await stopAll(running);

    // 通知完全缺失：仅靠周期重扫也能领取。
    const blind = await started(env, running, {
      watch: false, scanIntervalMs: 100,
    });
    await writeFile(join(env.root, 'n2.csv'), 'n2\n');
    await waitFor(() => itemByPath(blind, 'n2.csv')
      && itemByPath(blind, 'n2.csv').state === 'claimed', '周期重扫兜底领取');
    assert.equal(blind.ledger.stats.notifications, notificationsBefore,
      '盲采集器运行期间不得有新通知（统计跨重启累计）');
  } finally { await stopAll(running); await env.cleanup(); }
});

test('状态机只使用约定的对外状态', async () => {
  assert.deepEqual(STATES, ['observed', 'stable', 'claimed', 'review', 'gone']);
  const env = await makeEnv();
  const running = [];
  try {
    const collector = await started(env, running);
    await writeFile(join(env.root, 's.csv'), 's\n');
    await collector.scan('t1');
    assert.equal(itemByPath(collector, 's.csv').state, 'observed');
    await sleep(140);
    await collector.scan('t2');
    assert.equal(itemByPath(collector, 's.csv').state, 'claimed');
    for (const item of Object.values(collector.ledger.items)) {
      assert.ok(STATES.includes(item.state));
    }
  } finally { await stopAll(running); await env.cleanup(); }
});
