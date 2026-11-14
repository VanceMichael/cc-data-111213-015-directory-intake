import { constants, watch as fsWatch } from 'node:fs';
import { mkdir, open, lstat, readdir, readFile, readlink, realpath, rename } from 'node:fs/promises';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { createHash } from 'node:crypto';
import { Archive, writeAtomic } from './archive.js';

// 台账条目的对外状态：观察到 → 稳定候选 → 已领取；异常进入待人工检查。
// gone 是内部审计状态（未领取就消失），不属于对外四态。
export const STATES = ['observed', 'stable', 'claimed', 'review', 'gone'];

export class IntakeError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'IntakeError';
    this.code = code;
  }
}

// 文件签名：路径不是身份，inode 也可能被回收，签名只用于“内容是否变化”的判定，
// 最终身份始终以内容摘要为准。
export function signatureOf(st) {
  return { dev: String(st.dev), ino: String(st.ino), size: String(st.size),
    mtimeNs: String(st.mtimeNs), ctimeNs: String(st.ctimeNs) };
}

export function sameSignature(a, b) {
  return Boolean(a && b && a.dev === b.dev && a.ino === b.ino && a.size === b.size
    && a.mtimeNs === b.mtimeNs && a.ctimeNs === b.ctimeNs);
}

function isInside(rootReal, candidate) {
  const resolved = resolve(candidate);
  return resolved === rootReal || resolved.startsWith(rootReal + sep);
}

// 以只读方式打开受观测文件，并做多层校验：
// 1. lstat 必须是普通文件（符号链接直接拒绝，不跟随）；
// 2. O_NOFOLLOW 防止打开瞬间被替换成链接；
// 3. fstat 与 lstat 的 (dev, ino) 必须一致，否则路径在打开瞬间被替换；
// 4. /proc/self/fd 解析打开句柄的真实路径，必须仍在授权根目录内——
//    即使中间目录被改名或替换成符号链接，也不会读到根目录之外的内容。
export async function openVerifiedRegular(path, rootReal) {
  let before;
  try {
    before = await lstat(path, { bigint: true });
  } catch (error) {
    throw new IntakeError(error.code === 'ENOENT' ? 'vanished' : 'stat-failed',
      `无法查看 ${path}: ${error.message}`);
  }
  if (!before.isFile()) {
    throw new IntakeError('not-regular', `${path} 不是普通文件（符号链接或特殊文件），不跟随`);
  }
  let handle;
  try {
    handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  } catch (error) {
    if (error.code === 'ELOOP') throw new IntakeError('not-regular', `${path} 是符号链接，不跟随`);
    if (error.code === 'ENOENT') throw new IntakeError('vanished', `${path} 已消失`);
    throw new IntakeError('open-failed', `无法打开 ${path}: ${error.message}`);
  }
  try {
    const stat = await handle.stat({ bigint: true });
    if (!stat.isFile()) throw new IntakeError('not-regular', `${path} 不是普通文件`);
    if (stat.dev !== before.dev || stat.ino !== before.ino) {
      throw new IntakeError('path-changed', `${path} 在打开瞬间被替换`);
    }
    const fdPath = await readlink(`/proc/self/fd/${handle.fd}`);
    if (fdPath.endsWith(' (deleted)')) throw new IntakeError('vanished', `${path} 在读取前被删除`);
    if (fdPath !== rootReal && !fdPath.startsWith(rootReal + sep)) {
      throw new IntakeError('escapes-root',
        `${path} 实际位置 ${fdPath} 超出授权根目录 ${rootReal}，拒绝读取`);
    }
    return { handle, stat };
  } catch (error) {
    await handle.close().catch(() => {});
    throw error;
  }
}

// 只读到 maxBytes 为止：源文件在读取期间持续增长时不去追赶，
// 增长会由 readStableContent 的元数据栅栏发现，本次领取随后撤销。
export async function digestOpenFile(handle, { chunkBytes = 1024 * 1024, onChunk, maxBytes = Infinity } = {}) {
  const hash = createHash('sha256');
  const buffer = Buffer.allocUnsafe(chunkBytes);
  const parts = [];
  let position = 0;
  while (position < maxBytes) {
    const want = Math.min(buffer.length, maxBytes - position);
    const { bytesRead } = await handle.read(buffer, 0, want, position);
    if (bytesRead === 0) break;
    const slice = Buffer.from(buffer.subarray(0, bytesRead));
    parts.push(slice);
    hash.update(slice);
    position += bytesRead;
    if (onChunk) await onChunk(position);
  }
  return { bytes: Buffer.concat(parts), sha256: hash.digest('hex'), size: position };
}

// 复制期防护：读前读后各取一次元数据，并做第二遍摘要复核。
// 任何一步发现源文件变化都抛出 mutated-during-read，调用方必须撤销本次领取，
// 不得把前半旧内容和后半新内容拼在一起。
// 注意：这不是对任意写入方式的原子快照承诺，而是“变化必被发现、发现必撤销”。
export async function readStableContent(handle, options = {}) {
  const pre = await handle.stat({ bigint: true });
  const expected = Number(pre.size);
  const first = await digestOpenFile(handle, { ...options, maxBytes: expected });
  const mid = await handle.stat({ bigint: true });
  if (!sameSignature(signatureOf(pre), signatureOf(mid)) || first.size !== expected) {
    throw new IntakeError('mutated-during-read', '复制期间源文件元数据变化，撤销本次领取');
  }
  const second = await digestOpenFile(handle, { maxBytes: expected });
  const post = await handle.stat({ bigint: true });
  if (!sameSignature(signatureOf(mid), signatureOf(post)) || second.size !== expected) {
    throw new IntakeError('mutated-during-read', '复核期间源文件元数据变化，撤销本次领取');
  }
  if (first.sha256 !== second.sha256) {
    throw new IntakeError('mutated-during-read', '两次读取内容不一致，撤销本次领取');
  }
  return first;
}

// 遍历授权根目录，只列出普通文件；符号链接与特殊文件一律不跟随、不读取。
export async function walkRegularFiles(rootReal, { maxDepth = 32 } = {}) {
  const found = [];
  const skipped = [];
  async function visit(dir, depth) {
    if (depth > maxDepth) {
      skipped.push({ path: dir, reason: 'max-depth' });
      return;
    }
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch (error) {
      skipped.push({ path: dir, reason: `unreadable:${error.code || error.message}` });
      return;
    }
    for (const entry of entries) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) await visit(path, depth + 1);
      else if (entry.isFile()) found.push(path);
      else skipped.push({ path, reason: entry.isSymbolicLink() ? 'symlink-not-followed' : 'not-regular' });
    }
  }
  await visit(rootReal, 0);
  return { found, skipped };
}

function freshLedger(rootReal) {
  return { version: 1, rootReal, createdAt: new Date().toISOString(),
    items: {}, claims: {},
    stats: { scans: 0, notifications: 0, watchErrors: 0, rootMismatches: 0 } };
}

export class Collector {
  constructor({ root, archive, state, quietMs = 1000, scanIntervalMs = 5000,
    maxWaitMs = 60000, maxCopyAborts = 3, maxEscalations = 4, watch = true,
    debounceMs = 100, chunkBytes = 1024 * 1024, hooks = {}, log = null } = {}) {
    if (!root) throw new Error('需要观测根目录');
    if (!archive) throw new Error('需要归档目录');
    this.root = resolve(root);
    this.archive = archive instanceof Archive ? archive : new Archive(archive);
    this.statePath = resolve(state || join(this.archive.directory, 'collector-state.json'));
    this.quietMs = quietMs;
    this.scanIntervalMs = scanIntervalMs;
    this.maxWaitMs = maxWaitMs;
    this.maxCopyAborts = maxCopyAborts;
    this.maxEscalations = maxEscalations;
    this.watchEnabled = watch;
    this.debounceMs = debounceMs;
    this.chunkBytes = chunkBytes;
    this.hooks = hooks;
    this.log = log || (() => {});
    this.ledger = null;
    this.rootReal = null;
    this._stopped = false;
    this._queue = Promise.resolve();
    this._interval = null;
    this._debounce = null;
    this._pending = null;
    this._watcher = null;
  }

  async start() {
    this.rootReal = await realpath(this.root);
    // 只读约束：我们自己的写入位置（归档、台账）一律不得在受观测目录内。
    if (isInside(this.rootReal, this.archive.directory)) {
      throw new Error('归档目录不得位于受观测的仪器目录内（只读约束）');
    }
    if (isInside(this.rootReal, this.statePath)) {
      throw new Error('状态台账不得位于受观测的仪器目录内（只读约束）');
    }
    this._stopped = false;
    this.ledger = await this._loadLedger();
    await this._mergeReceiptsIntoClaims();
    await this.scan('startup');
    if (this.watchEnabled) this._startWatch();
    if (this.scanIntervalMs > 0) {
      this._interval = setInterval(() => { this.scan('interval').catch(() => {}); }, this.scanIntervalMs);
    }
    return this;
  }

  async stop() {
    this._stopped = true;
    if (this._interval) { clearInterval(this._interval); this._interval = null; }
    if (this._debounce) { clearTimeout(this._debounce); this._debounce = null; }
    if (this._pending) { clearTimeout(this._pending); this._pending = null; }
    if (this._watcher) { this._watcher.close(); this._watcher = null; }
    await this._queue.catch(() => {});
    if (this.ledger) await this._save();
  }

  // 通知只是提示：去抖后触发一次重扫，真正的依据始终是重扫本身。
  // 通知重复、丢失都不影响正确性，周期重扫兜底。
  scan(reason = 'manual') {
    const run = this._queue.then(() => this._scanOnce(reason));
    this._queue = run.then(() => {}, () => {});
    return run;
  }

  summary() {
    const states = Object.fromEntries(STATES.map(name => [name, 0]));
    const items = Object.values(this.ledger.items);
    for (const item of items) states[item.state] = (states[item.state] || 0) + 1;
    return { root: this.rootReal, scans: this.ledger.stats.scans,
      notifications: this.ledger.stats.notifications,
      watchErrors: this.ledger.stats.watchErrors,
      rootMismatches: this.ledger.stats.rootMismatches,
      states, claims: Object.keys(this.ledger.claims).length,
      review: items.filter(item => item.state === 'review')
        .map(item => ({ path: item.path, flags: item.flags, sticky: item.sticky })) };
  }

  items() {
    return Object.values(this.ledger.items)
      .map(item => ({ path: item.path, state: item.state, sha256: item.sha256,
        aborts: item.aborts, escalations: item.escalations, sticky: item.sticky,
        flags: item.flags.slice(), history: item.history.slice(-5) }));
  }

  async _scanOnce(reason) {
    const now = Date.now();
    // 每轮重扫前重新解析授权根目录：目录被改名或替换成符号链接时，
    // 解析结果会变化，本轮直接中止（失败关闭，不做任何领取）。
    const currentRootReal = await realpath(this.root).catch(() => null);
    if (currentRootReal !== this.rootReal) {
      this.ledger.stats.rootMismatches += 1;
      this.log(`授权根目录解析变化（${this.rootReal} → ${currentRootReal}），本轮重扫中止`);
      await this._save();
      // 根目录被暂时改名/替换时也要自动重试，否则无周期重扫的部署会停摆。
      if (!this._stopped && !this._pending) {
        this._pending = setTimeout(() => {
          this._pending = null;
          this.scan('root-retry').catch(() => {});
        }, Math.max(1000, this.quietMs));
      }
      return this.summary();
    }
    this.ledger.stats.scans += 1;
    const { found, skipped } = await walkRegularFiles(this.rootReal);
    this.ledger.stats.lastScan = { at: new Date(now).toISOString(), reason,
      found: found.length, skipped: skipped.length };
    if (skipped.length > 0) this.ledger.stats.skipped = skipped.slice(-50);

    const seen = new Set();
    for (const abs of found) {
      let st;
      try { st = await lstat(abs, { bigint: true }); }
      catch { continue; }
      if (!st.isFile()) continue;
      const rel = relative(this.rootReal, abs);
      const sig = signatureOf(st);
      const key = `${rel}#${sig.dev}:${sig.ino}`;
      seen.add(key);
      this._observe(key, rel, sig, now);
    }
    for (const [key, item] of Object.entries(this.ledger.items)) {
      if (!seen.has(key) && item.state !== 'claimed' && item.state !== 'gone') {
        this._transition(item, 'gone', '未领取就从目录中消失');
      }
    }
    // 稳定性判定：静默期无变化才成为稳定候选；非粘性待人工条目同样允许回到稳定。
    for (const item of Object.values(this.ledger.items)) {
      if ((item.state === 'observed' || item.state === 'review') && !item.sticky
          && now - item.lastChanged >= this.quietMs) {
        this._transition(item, 'stable', `静默 ${this.quietMs}ms 无变化`);
      }
    }
    for (const item of Object.values(this.ledger.items)) {
      if (item.state === 'stable' && !item.sticky) await this._claim(item);
    }
    // 长时间不停写：始终没稳定过的条目超过 maxWaitMs 进入待人工检查。
    // 之后内容再变化会自动重新观察（不丢资料），反复升级超过上限则粘住等待人工。
    for (const item of Object.values(this.ledger.items)) {
      if (item.state === 'observed' && !item.deadlineEscalated
          && now - item.firstSeen > this.maxWaitMs) {
        item.deadlineEscalated = true;
        this._escalate(item, `超过 ${this.maxWaitMs}ms 未稳定（持续写入）`, false);
      }
    }
    await this._save();
    this._armPending();
    return this.summary();
  }

  // 有待定条目（观察到 / 非粘性待人工）时，安排静默期到期后的补扫。
  // 这样即使通知完全缺失、周期重扫间隔很长，稳定性判定也会按时发生。
  _armPending() {
    if (this._pending) { clearTimeout(this._pending); this._pending = null; }
    if (!this.ledger || this._stopped) return;
    const now = Date.now();
    let earliest = Infinity;
    for (const item of Object.values(this.ledger.items)) {
      if (item.sticky) continue;
      if (item.state === 'observed' || item.state === 'review') {
        earliest = Math.min(earliest, item.lastChanged + this.quietMs);
      }
    }
    if (earliest === Infinity) return;
    const delay = Math.max(1, earliest - now) + 5;
    this._pending = setTimeout(() => {
      this._pending = null;
      this.scan('pending').catch(() => {});
    }, delay);
  }

  _observe(key, rel, sig, now) {
    const item = this.ledger.items[key];
    if (!item) {
      this.ledger.items[key] = { key, path: rel, signature: sig, state: 'observed',
        firstSeen: now, lastChanged: now, lastSeen: now, sha256: null, receiptId: null,
        aborts: 0, escalations: 0, sticky: false, deadlineEscalated: false, flags: [],
        history: [{ at: now, from: null, to: 'observed', reason: '首次出现' }] };
      return;
    }
    item.lastSeen = now;
    if (sameSignature(item.signature, sig)) {
      if (item.state === 'gone') this._transition(item, 'observed', '重新出现');
      return;
    }
    // 签名变化：同一路径（甚至同一 inode）上出现了新内容，重新进入观察周期。
    // firstSeen 随之重置：未稳定超时按“当前这段内容”计算，而不是按路径的历史。
    item.signature = sig;
    item.lastChanged = now;
    item.deadlineEscalated = false;
    if (item.state === 'claimed') {
      item.sha256 = null;
      item.receiptId = null;
      item.firstSeen = now;
      this._transition(item, 'observed', '同一路径出现新内容，重新观察');
    } else if (item.state === 'gone') {
      item.firstSeen = now;
      this._transition(item, 'observed', '重新出现且内容有变化');
    } else if (item.state === 'review' && item.sticky) {
      item.history.push({ at: now, from: 'review', to: 'review', reason: '待人工期间内容仍在变化' });
    } else if (item.state !== 'observed') {
      item.firstSeen = now;
      this._transition(item, 'observed', '内容变化，重新计时');
    }
  }

  _transition(item, to, reason) {
    item.history.push({ at: Date.now(), from: item.state, to, reason });
    if (item.history.length > 20) item.history.splice(0, item.history.length - 20);
    item.state = to;
  }

  _escalate(item, reason, sticky) {
    item.escalations += 1;
    if (!item.flags.includes(reason)) item.flags.push(reason);
    if (sticky || item.escalations > this.maxEscalations) item.sticky = true;
    this._transition(item, 'review', reason);
    this.log(`待人工检查: ${item.path} — ${reason}`);
  }

  async _claim(item) {
    const abs = join(this.rootReal, item.path);
    let opened;
    try {
      opened = await openVerifiedRegular(abs, this.rootReal);
    } catch (error) {
      this._claimFailed(item, error);
      return;
    }
    try {
      const content = await readStableContent(opened.handle, {
        chunkBytes: this.chunkBytes,
        onChunk: this.hooks.onChunk && ((position) => this.hooks.onChunk(item, position)),
      });
      // 内容即身份：同一摘要只领取一次。硬链接或内容相同的另一路径
      // 只把来源路径挂到已有领取记录上，不算第二份资料。
      const existing = this.ledger.claims[content.sha256];
      if (existing) {
        if (!existing.sources.includes(item.path)) existing.sources.push(item.path);
        item.sha256 = content.sha256;
        item.receiptId = existing.receiptId;
        this._transition(item, 'claimed', '内容与已有领取相同，按内容去重');
        this.log(`内容相同，来源并入已有领取: ${item.path} → ${content.sha256.slice(0, 12)}…`);
        return;
      }
      const receipt = await this.archive.accept(content.bytes, {
        origin: 'collector', source: item.path, inode: item.signature.ino,
        device: item.signature.dev, size: content.size,
        mtimeNs: item.signature.mtimeNs, ctimeNs: item.signature.ctimeNs,
        firstSeen: new Date(item.firstSeen).toISOString(),
        attempts: item.aborts + 1, flags: item.flags.slice(),
      });
      this.ledger.claims[content.sha256] = { sha256: content.sha256, receiptId: receipt.id,
        size: content.size, firstClaimedAt: receipt.observedAt, sources: [item.path] };
      item.sha256 = content.sha256;
      item.receiptId = receipt.id;
      this._transition(item, 'claimed', `已领取，摘要 ${content.sha256.slice(0, 12)}…`);
      this.log(`已领取 ${item.path} → ${content.sha256}`);
    } catch (error) {
      this._claimFailed(item, error);
    } finally {
      await opened.handle.close().catch(() => {});
    }
  }

  _claimFailed(item, error) {
    const code = error && error.code ? error.code : 'unexpected';
    if (code === 'vanished') {
      this._transition(item, 'gone', '领取前消失');
      return;
    }
    if (code === 'mutated-during-read' || code === 'path-changed') {
      item.aborts += 1;
      this._transition(item, 'observed', `复制期间源文件变化，撤销本次领取（第 ${item.aborts} 次）`);
      item.lastChanged = Date.now();
      if (item.aborts >= this.maxCopyAborts) {
        this._escalate(item, `复制期间反复变化（${item.aborts} 次），放弃自动领取`, true);
      }
      return;
    }
    if (code === 'escapes-root' || code === 'not-regular') {
      this._escalate(item, error.message, true);
      return;
    }
    item.aborts += 1;
    this._escalate(item, `领取失败（${code}: ${error.message}）`, true);
  }

  _startWatch() {
    try {
      this._watcher = fsWatch(this.rootReal, { recursive: true }, () => {
        this.ledger.stats.notifications += 1;
        if (this._debounce) clearTimeout(this._debounce);
        this._debounce = setTimeout(() => { this.scan('notify').catch(() => {}); }, this.debounceMs);
      });
      this._watcher.on('error', (error) => {
        this.ledger.stats.watchErrors += 1;
        this.log(`目录通知不可用（${error.message}），仅依靠周期重扫`);
      });
    } catch (error) {
      this.ledger.stats.watchErrors += 1;
      this.log(`无法建立目录通知（${error.message}），仅依靠周期重扫`);
    }
  }

  async _loadLedger() {
    let raw;
    try {
      raw = await readFile(this.statePath, 'utf8');
    } catch (error) {
      if (error.code === 'ENOENT') return freshLedger(this.rootReal);
      throw error;
    }
    let ledger;
    try {
      ledger = JSON.parse(raw);
    } catch {
      const quarantine = `${this.statePath}.corrupt-${Date.now()}`;
      await rename(this.statePath, quarantine);
      this.log(`状态台账无法解析，已隔离到 ${quarantine} 并重新开始`);
      return freshLedger(this.rootReal);
    }
    if (ledger.rootReal !== this.rootReal) {
      throw new Error(`状态台账记录的根目录 ${ledger.rootReal} 与当前 ${this.rootReal} 不一致，拒绝沿用`);
    }
    return ledger;
  }

  // 回执是“哪些内容被领取过”的最终依据：进程在写台账前崩溃，
  // 重启后也能从回执重建去重集合，不会重复领取。
  async _mergeReceiptsIntoClaims() {
    const receipts = await this.archive.receipts();
    for (const receipt of receipts) {
      if (!receipt || typeof receipt.sha256 !== 'string') continue;
      const source = receipt.metadata && receipt.metadata.source;
      const existing = this.ledger.claims[receipt.sha256];
      if (!existing) {
        this.ledger.claims[receipt.sha256] = { sha256: receipt.sha256, receiptId: receipt.id,
          size: receipt.size ?? null, firstClaimedAt: receipt.observedAt ?? null,
          sources: source ? [source] : [] };
      } else if (source && !existing.sources.includes(source)) {
        existing.sources.push(source);
      }
    }
  }

  async _save() {
    await mkdir(dirname(this.statePath), { recursive: true });
    await writeAtomic(this.statePath, JSON.stringify(this.ledger, null, 2));
  }
}
