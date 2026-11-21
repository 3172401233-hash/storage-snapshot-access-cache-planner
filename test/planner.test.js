import { test } from 'node:test';
import assert from 'node:assert/strict';
import { planCache } from '../src/planner.js';

test('共享块按引用计数保护，任务运行期间不被淘汰', () => {
  const result = planCache({
    snapshots: [
      { id: 's1', chunks: ['A', 'B'] },
      { id: 's2', chunks: ['A', 'C'] },
    ],
    chunkSizes: { A: 100, B: 100, C: 100 },
    tasks: [
      { id: 't1', snapshotId: 's1', startTime: 0, deadline: 10 },
      { id: 't2', snapshotId: 's2', startTime: 2, deadline: 10 },
    ],
    cacheCapacity: 200,
    bandwidth: 100,
  });

  // 共享块 A 只回源一次，t2 命中缓存。
  const fetched = result.prefetchPlan.map((p) => p.chunkId);
  assert.deepEqual(fetched, ['A', 'B', 'C']);
  assert.equal(result.estimatedRefetchBytes, 300);

  // 容量只能容纳两块，t2 需要 C 时淘汰非共享的 B，共享块 A 受保护。
  assert.deepEqual(
    result.evictionOrder.map((e) => e.chunkId),
    ['B'],
  );
  assert.equal(result.effectiveRefCounts.A, 2);
  assert.equal(result.lateTasks.length, 0);
});

test('容量不足时无法淘汰已钉住的依赖块并报告缺口', () => {
  const result = planCache({
    snapshots: [{ id: 's1', chunks: ['A', 'B'] }],
    chunkSizes: { A: 100, B: 100 },
    tasks: [{ id: 't1', snapshotId: 's1', startTime: 0, deadline: 1.5 }],
    cacheCapacity: 100,
    bandwidth: 100,
  });

  // A 入库后缓存已满，B 到来时 A 仍被运行中的 t1 钉住，禁止淘汰。
  assert.deepEqual(result.evictionOrder, []);
  assert.equal(result.capacityShortfalls.length, 1);
  assert.equal(result.capacityShortfalls[0].chunkId, 'B');
  assert.equal(result.capacityShortfalls[0].reason, 'all-blocks-pinned');
  // B 直读不缓存，仍计入回源量；完成时间 2 超过 deadline 1.5。
  assert.equal(result.estimatedRefetchBytes, 200);
  assert.deepEqual(
    result.lateTasks.map((t) => t.taskId),
    ['t1'],
  );
});

test('快照删除释放引用计数，独有块优先被淘汰', () => {
  const result = planCache({
    snapshots: [
      { id: 's1', chunks: ['A', 'D'], deleted: true },
      { id: 's2', chunks: ['A', 'C'] },
    ],
    chunkSizes: { A: 100, C: 100, D: 100 },
    tasks: [{ id: 't1', snapshotId: 's2', startTime: 0, deadline: 10 }],
    cacheCapacity: 200,
    bandwidth: 100,
    initialCache: ['A', 'D'],
  });

  // s1 删除后 D 的引用计数归零，A 仍被 s2 共享。
  assert.equal(result.effectiveRefCounts.D, 0);
  assert.equal(result.effectiveRefCounts.A, 1);
  // 为 C 腾空间时优先淘汰已删除快照的独有块 D。
  assert.deepEqual(result.evictionOrder, [
    { chunkId: 'D', bytes: 100, reason: 'snapshot-deleted' },
  ]);
  // A 命中初始缓存，只有 C 回源。
  assert.equal(result.estimatedRefetchBytes, 100);
});

test('使用外部引用计数时快照删除会扣减引用', () => {
  const result = planCache({
    snapshots: [
      { id: 's1', chunks: ['A'], deleted: true },
      { id: 's2', chunks: ['A'] },
    ],
    refCounts: { A: 5 },
    chunkSizes: { A: 100 },
    tasks: [{ id: 't1', snapshotId: 's2', startTime: 0, deadline: 10 }],
    cacheCapacity: 100,
    bandwidth: 100,
  });
  assert.equal(result.effectiveRefCounts.A, 4);
});

test('deadline 冲突时报告无法按时恢复的任务', () => {
  const result = planCache({
    snapshots: [
      { id: 's1', chunks: ['A'] },
      { id: 's2', chunks: ['B'] },
    ],
    chunkSizes: { A: 100, B: 100 },
    tasks: [
      { id: 't1', snapshotId: 's1', startTime: 0, deadline: 1 },
      { id: 't2', snapshotId: 's2', startTime: 0, deadline: 1.5 },
    ],
    cacheCapacity: 200,
    bandwidth: 100,
  });

  // 回源通道串行共享：t1 在 0-1 取 A，t2 只能 1-2 取 B，错过 1.5 的 deadline。
  assert.deepEqual(result.lateTasks, [
    { taskId: 't2', deadline: 1.5, estimatedCompletion: 2, missBytes: 100 },
  ]);
  assert.deepEqual(
    result.prefetchPlan.map((p) => [p.chunkId, p.start, p.end]),
    [
      ['A', 0, 1],
      ['B', 1, 2],
    ],
  );
});

test('恢复读取沿快照 DAG 父链闭包取块', () => {
  const result = planCache({
    snapshots: [
      { id: 'base', chunks: ['A'] },
      { id: 'child', parents: ['base'], chunks: ['B'] },
    ],
    chunkSizes: { A: 100, B: 100 },
    tasks: [{ id: 't1', snapshotId: 'child', startTime: 0, deadline: 10 }],
    cacheCapacity: 200,
    bandwidth: 100,
  });
  assert.deepEqual(
    result.prefetchPlan.map((p) => p.chunkId).sort(),
    ['A', 'B'],
  );
  assert.equal(result.estimatedRefetchBytes, 200);
});
