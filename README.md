# storage-snapshot-access-cache-planner
规划不可变快照读取缓存、引用和回源预算

> 本工具**只处理存储访问元数据**（快照 DAG、块引用计数、访问历史、deadline、
> 容量与带宽），用于离线计算缓存计划；不读取、传输或修改任何真实数据块。

## 背景

存储基础设施中的不可变快照会被多个恢复任务共享读取。缓存可以降低对象存储
回源，但错误淘汰会删除另一个恢复任务仍依赖的数据块。本规划器根据快照 DAG、
数据块引用计数、访问历史、恢复 deadline、缓存容量和回源带宽，计算预取与
淘汰顺序。

核心约束：

- 共享块按引用计数保护，引用计数归零的块优先淘汰；
- 任务一旦开始，其依赖块在任务完成前被钉住（pin），禁止淘汰；
- 快照删除会释放其持有的引用，独有块成为优先淘汰候选。

## 使用

```js
import { planCache } from './src/planner.js';

const plan = planCache({
  snapshots: [
    { id: 's1', chunks: ['A', 'B'] },
    { id: 's2', parents: ['s1'], chunks: ['C'] },
    { id: 'old', chunks: ['D'], deleted: true }, // 已删除快照
  ],
  chunkSizes: { A: 100, B: 100, C: 100, D: 100 },
  accessHistory: [{ chunkId: 'A', timestamp: 1 }],
  tasks: [
    { id: 't1', snapshotId: 's2', startTime: 0, deadline: 10 },
  ],
  cacheCapacity: 200,
  bandwidth: 100, // 字节 / 单位时间
});
```

### 输入

| 字段 | 说明 |
| --- | --- |
| `snapshots` | 快照 DAG：`parents` 指向父快照，`chunks` 为直接引用块，`deleted` 标记已删除 |
| `chunkSizes` | 各数据块大小（字节） |
| `refCounts` | 可选，外部观测的引用计数；提供后按已删除快照扣减，否则从存活 DAG 推导 |
| `accessHistory` | 历史访问记录，用于淘汰打分（频次 + 最近访问） |
| `tasks` | 恢复任务：`snapshotId`、`startTime`、`deadline` |
| `cacheCapacity` | 缓存容量（字节） |
| `bandwidth` | 回源带宽，所有任务共享一条串行回源通道 |
| `initialCache` | 可选，初始已在缓存中的块 |

### 输出

| 字段 | 说明 |
| --- | --- |
| `prefetchPlan` | 预取计划：块、所属任务、起止时间、字节数、是否入缓存 |
| `evictionOrder` | 待淘汰块顺序及原因（`snapshot-deleted` / `capacity-pressure` / `capacity-pressure-shared`） |
| `estimatedRefetchBytes` | 预计回源量（字节），含被淘汰后的重复回源 |
| `lateTasks` | 无法按时恢复的任务：deadline、预计完成时间、缺失字节数 |
| `capacityShortfalls` | 容量不足事件（块超过容量或所有块均被钉住） |
| `effectiveRefCounts` | 快照删除后的有效引用计数 |

## 调度与淘汰策略

- 任务按最早 deadline 优先（EDF）调度，共享一条串行回源通道；
- 恢复一个快照需要沿 DAG 父链读取块闭包（含已删除祖先，数据仍需可读）；
- 淘汰候选按类别排序：引用计数为 0 且未来不需要 → 引用计数为 0 →
  未来不需要 → 共享/未来仍需要（最后手段）；类别内按访问频次与最近时间升序；
- 运行中任务的依赖块永不进入淘汰候选。

## 测试

```sh
node --test
```

覆盖共享块保护、容量不足、快照删除（DAG 推导与外部引用计数两条路径）、
deadline 冲突和 DAG 父链闭包读取。
