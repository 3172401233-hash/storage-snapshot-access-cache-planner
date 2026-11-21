/**
 * 快照访问缓存规划器。
 *
 * 只处理存储访问元数据（快照 DAG、块引用、访问历史、deadline、容量、带宽），
 * 不读取或搬运真实数据块。输出预取计划、淘汰顺序、预计回源量和无法按时恢复的任务。
 */

/**
 * @typedef {Object} PlannerInput
 * @property {Array<{id:string, parents?:string[], chunks?:string[], deleted?:boolean}>} snapshots
 *   快照 DAG。chunks 为该快照直接引用的数据块；恢复时需要沿 parents 闭包读取。
 *   deleted=true 表示快照已删除，其引用计数会被释放。
 * @property {Object<string, number>} [chunkSizes] 数据块大小（字节），缺省按 0 处理。
 * @property {Object<string, number>} [refCounts] 外部观测到的引用计数；
 *   提供时会在此基础上扣减已删除快照的引用，否则从存活快照 DAG 推导。
 * @property {Array<{chunkId:string, timestamp?:number, taskId?:string}>} [accessHistory]
 *   历史访问记录，用于淘汰打分（频次 + 最近使用时间）。
 * @property {Array<{id:string, snapshotId:string, startTime:number, deadline:number}>} tasks
 *   恢复任务。任务一旦开始，其依赖块在任务完成前禁止淘汰。
 * @property {number} cacheCapacity 缓存容量（字节）。
 * @property {number} bandwidth 回源带宽（字节/单位时间），所有任务共享一条回源通道。
 * @property {string[]} [initialCache] 初始已在缓存中的块。
 */

/**
 * 计算预取与淘汰计划。
 * @param {PlannerInput} input
 */
export function planCache(input) {
  const {
    snapshots = [],
    chunkSizes = {},
    refCounts: providedRefCounts,
    accessHistory = [],
    tasks = [],
    cacheCapacity = 0,
    bandwidth = 0,
    initialCache = [],
  } = input;

  if (!(bandwidth > 0)) {
    throw new Error('bandwidth must be a positive number');
  }
  if (!(cacheCapacity >= 0)) {
    throw new Error('cacheCapacity must be a non-negative number');
  }

  const snapshotById = new Map();
  for (const snap of snapshots) {
    if (snapshotById.has(snap.id)) {
      throw new Error(`duplicate snapshot id: ${snap.id}`);
    }
    snapshotById.set(snap.id, {
      id: snap.id,
      parents: snap.parents ?? [],
      chunks: snap.chunks ?? [],
      deleted: snap.deleted === true,
    });
  }

  const sizeOf = (chunkId) => chunkSizes[chunkId] ?? 0;

  // 有效引用计数：已删除快照的引用被释放。
  const effectiveRefCounts = computeEffectiveRefCounts(snapshotById, providedRefCounts);

  // 访问历史统计：频次 + 最近访问时间。
  const accessStats = new Map();
  for (const record of accessHistory) {
    const stat = accessStats.get(record.chunkId) ?? { frequency: 0, lastAccess: -Infinity };
    stat.frequency += 1;
    if (record.timestamp !== undefined && record.timestamp > stat.lastAccess) {
      stat.lastAccess = record.timestamp;
    }
    accessStats.set(record.chunkId, stat);
  }
  const statOf = (chunkId) =>
    accessStats.get(chunkId) ?? { frequency: 0, lastAccess: -Infinity };

  // 恢复一个快照需要沿 DAG 父链读取的块闭包（含已删除祖先，数据仍需可读）。
  const closureCache = new Map();
  function snapshotClosure(snapshotId) {
    if (closureCache.has(snapshotId)) return closureCache.get(snapshotId);
    const snap = snapshotById.get(snapshotId);
    if (!snap) throw new Error(`unknown snapshot: ${snapshotId}`);
    const ordered = [];
    const seen = new Set();
    const stack = [snap];
    const visited = new Set();
    while (stack.length > 0) {
      const current = stack.pop();
      if (visited.has(current.id)) continue;
      visited.add(current.id);
      for (const chunkId of current.chunks) {
        if (!seen.has(chunkId)) {
          seen.add(chunkId);
          ordered.push(chunkId);
        }
      }
      for (const parentId of current.parents) {
        const parent = snapshotById.get(parentId);
        if (!parent) throw new Error(`unknown parent snapshot: ${parentId}`);
        stack.push(parent);
      }
    }
    closureCache.set(snapshotId, ordered);
    return ordered;
  }

  // 按最早 deadline 优先（EDF）调度，稳定排序保证可复现。
  const scheduledTasks = [...tasks].sort((a, b) =>
    a.deadline - b.deadline || a.startTime - b.startTime || (a.id < b.id ? -1 : 1),
  );

  const taskRequirements = new Map();
  for (const task of scheduledTasks) {
    taskRequirements.set(task.id, snapshotClosure(task.snapshotId));
  }

  // 某块是否仍被后续未执行任务需要（用于淘汰打分，不是硬保护）。
  function neededByFutureTask(chunkId, fromIndex) {
    for (let i = fromIndex; i < scheduledTasks.length; i += 1) {
      if (taskRequirements.get(scheduledTasks[i].id).includes(chunkId)) return true;
    }
    return false;
  }

  const cache = new Map(); // chunkId -> { size, lastUsed }
  let cacheUsed = 0;
  for (const chunkId of initialCache) {
    if (!cache.has(chunkId)) {
      cache.set(chunkId, { size: sizeOf(chunkId), lastUsed: -Infinity });
      cacheUsed += sizeOf(chunkId);
    }
  }

  const prefetchPlan = [];
  const evictionOrder = [];
  const lateTasks = [];
  const capacityShortfalls = [];
  let estimatedRefetchBytes = 0;
  let clock = 0; // 共享回源通道的时间轴

  const pinned = new Set(); // 当前已开始任务的依赖块，禁止淘汰

  function evictOne(taskIndex) {
    let best = null;
    let bestKey = null;
    for (const [chunkId, entry] of cache) {
      if (pinned.has(chunkId)) continue; // 任务开始后不能删除依赖块
      const refCount = effectiveRefCounts.get(chunkId) ?? 0;
      const future = neededByFutureTask(chunkId, taskIndex);
      const stat = statOf(chunkId);
      // 类别越小越先淘汰：
      // 0 = 引用计数为 0 且未来不需要（典型：快照已删除）
      // 1 = 引用计数为 0
      // 2 = 未来不需要
      // 3 = 共享/未来仍需要（最后手段）
      const category = refCount === 0 ? (future ? 1 : 0) : future ? 3 : 2;
      const key = [category, stat.frequency, stat.lastAccess, entry.lastUsed, chunkId];
      if (
        bestKey === null ||
        key[0] < bestKey[0] ||
        (key[0] === bestKey[0] &&
          (key[1] < bestKey[1] ||
            (key[1] === bestKey[1] &&
              (key[2] < bestKey[2] ||
                (key[2] === bestKey[2] &&
                  (key[3] < bestKey[3] ||
                    (key[3] === bestKey[3] && key[4] < bestKey[4])))))))
      ) {
        best = chunkId;
        bestKey = key;
      }
    }
    if (best === null) return null;
    const entry = cache.get(best);
    cache.delete(best);
    cacheUsed -= entry.size;
    const refCount = effectiveRefCounts.get(best) ?? 0;
    evictionOrder.push({
      chunkId: best,
      bytes: entry.size,
      reason:
        refCount === 0
          ? 'snapshot-deleted'
          : bestKey[0] === 3
            ? 'capacity-pressure-shared'
            : 'capacity-pressure',
    });
    return best;
  }

  scheduledTasks.forEach((task, taskIndex) => {
    const required = taskRequirements.get(task.id);
    for (const chunkId of required) pinned.add(chunkId); // 任务开始：锁定依赖块

    let cursor = Math.max(clock, task.startTime);
    let missBytes = 0;

    for (const chunkId of required) {
      if (cache.has(chunkId)) continue; // 缓存命中，无回源
      const size = sizeOf(chunkId);
      missBytes += size;
      estimatedRefetchBytes += size;

      let cached = true;
      if (size > cacheCapacity) {
        cached = false; // 单块超过容量，永不缓存
      } else {
        while (cacheUsed + size > cacheCapacity) {
          if (evictOne(taskIndex) === null) {
            cached = false; // 全部剩余块都被钉住，容量不足
            break;
          }
        }
      }

      const start = cursor;
      const end = start + size / bandwidth;
      cursor = end;
      prefetchPlan.push({ chunkId, taskId: task.id, start, end, bytes: size, cached });

      if (cached) {
        cache.set(chunkId, { size, lastUsed: end });
        cacheUsed += size;
      } else {
        capacityShortfalls.push({
          taskId: task.id,
          chunkId,
          bytes: size,
          reason: size > cacheCapacity ? 'chunk-exceeds-capacity' : 'all-blocks-pinned',
        });
      }
    }

    const completion = cursor;
    for (const chunkId of required) {
      const entry = cache.get(chunkId);
      if (entry) entry.lastUsed = completion;
    }
    if (completion > task.deadline) {
      lateTasks.push({
        taskId: task.id,
        deadline: task.deadline,
        estimatedCompletion: completion,
        missBytes,
      });
    }

    for (const chunkId of required) pinned.delete(chunkId); // 任务完成：解除锁定
    clock = cursor;
  });

  return {
    prefetchPlan,
    evictionOrder,
    estimatedRefetchBytes,
    lateTasks,
    capacityShortfalls,
    effectiveRefCounts: Object.fromEntries(effectiveRefCounts),
    finalCache: [...cache.keys()],
  };
}

function computeEffectiveRefCounts(snapshotById, providedRefCounts) {
  const counts = new Map();
  if (providedRefCounts) {
    for (const [chunkId, count] of Object.entries(providedRefCounts)) {
      counts.set(chunkId, count);
    }
    // 快照删除：释放其对各块持有的引用。
    for (const snap of snapshotById.values()) {
      if (!snap.deleted) continue;
      for (const chunkId of snap.chunks) {
        counts.set(chunkId, Math.max(0, (counts.get(chunkId) ?? 0) - 1));
      }
    }
    return counts;
  }
  for (const snap of snapshotById.values()) {
    for (const chunkId of snap.chunks) {
      if (!counts.has(chunkId)) counts.set(chunkId, 0);
    }
    if (snap.deleted) continue;
    for (const chunkId of snap.chunks) {
      counts.set(chunkId, (counts.get(chunkId) ?? 0) + 1);
    }
  }
  return counts;
}
