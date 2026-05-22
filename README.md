# ThreadPool v2

C++17 线程池，支持有界队列背压、优先级调度、运行时统计。

## 接口

```cpp
// 构造
ThreadPool pool(4);                        // 4线程，无界队列
ThreadPool pool(4, 100);                   // 4线程，队列上限100，队满阻塞
ThreadPool pool(4, 100, false);            // 4线程，队列上限100，队满抛异常

// 提交（优先级默认0）
auto f = pool.submit(func, arg1, arg2);
int result = f.get();

// 指定优先级（数值越大越先执行，同优先级保持提交顺序FIFO）
auto f = pool.submitWithPriority(10, func, arg);

// 运行时统计
PoolStats s = pool.stats();
// s.totalThreads / s.activeThreads / s.pendingTasks / s.completedTasks
```

## 特性

| 特性 | 说明 |
|------|------|
| 有界队列 + 背压 | `maxQueueSize` 控制上限；`blockOnFull=true` 阻塞等待，`false` 抛异常 |
| 优先级调度 | `submitWithPriority(prio, ...)` ；同优先级保持 FIFO |
| 运行时统计 | `stats()` 返回活跃线程数 / 队列长度 / 完成任务数 |
| 异常安全 | 任务异常自动传播到 `future.get()`，工作线程不崩溃 |
| 优雅关闭 | 析构时排空队列，已提交任务全部完成再退出 |

## 编译 & 运行

```bash
g++ -std=c++17 -pthread -O2 -o threadpool_test main.cpp
./threadpool_test
```

## 测试结果

| 测试 | 结果 |
|------|------|
| 基础功能 & 返回值 | ✅ PASS |
| 异常传播 | ✅ PASS |
| 有界队列 – 队满拒绝 | ✅ PASS |
| 有界队列 – 背压阻塞（无死锁）| ✅ PASS |
| 优先级：高优先级先执行 | ✅ PASS |
| 优先级：同优先级 FIFO | ✅ PASS |
| 运行时统计 | ✅ PASS |
| 性能对比（4线程 ~4x 加速）| ✅ PASS |

## 设计说明

- **优先队列**：`std::priority_queue`（max-heap），`operator<` 定义优先级；seq 计数保证同优先级 FIFO
- **背压**：`notFull_` 条件变量；任务完成后 `notify_one` 唤醒等待的 submit 调用
- **统计**：`activeThreads_` 和 `completedTasks_` 用 `std::atomic`，无额外加锁开销
- **stopped 标志**：析构设置后同时 `notify_all` 两个条件变量，防止 submit/worker 双向死锁
