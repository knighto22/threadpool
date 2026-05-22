#pragma once
/**
 * 线程池实现 v2
 * 新增：有界队列+背压 / 优先级任务 / 运行时统计
 */
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>
#include <stdexcept>
#include <atomic>
#include <chrono>

 // ─── 优先级任务包装 ────────────────────────────────────────────────────────────
struct PriorityTask {
    int priority;                    // 数值越大越先执行
    uint64_t seq;                    // 同优先级按提交顺序（FIFO）
    std::function<void()> fn;

    // max-heap: 此运算符定义"更小"（即更后执行）
    // priority 大 → 先执行；同优先级 seq 小（提交早）→ 先执行
    bool operator<(const PriorityTask& o) const {
        if (priority != o.priority) return priority < o.priority;
        return seq > o.seq;  // seq 越大越晚提交，越后执行
    }
};

// ─── 运行时统计快照 ────────────────────────────────────────────────────────────
struct PoolStats {
    size_t totalThreads;    // 线程总数
    size_t activeThreads;   // 当前正在执行任务的线程数
    size_t pendingTasks;    // 队列中待执行的任务数
    uint64_t completedTasks;// 已完成任务总数
};

// ─── 线程池主体 ────────────────────────────────────────────────────────────────
class ThreadPool {
public:
    // maxQueueSize = 0 表示无界（兼容旧行为）
    // blockOnFull  = true  → 队满时 submit() 阻塞等待
    //              = false → 队满时 submit() 抛 runtime_error
    explicit ThreadPool(size_t threadCount,
        size_t maxQueueSize = 0,
        bool   blockOnFull = true)
        : stopped_(false),
        maxQueueSize_(maxQueueSize),
        blockOnFull_(blockOnFull),
        seqCounter_(0),
        activeThreads_(0),
        completedTasks_(0)
    {
        if (threadCount == 0)
            throw std::invalid_argument("线程数必须大于 0");

        for (size_t i = 0; i < threadCount; ++i) {
            workers_.emplace_back([this] { workerLoop(); });
        }
    }

    // ── submit：优先级默认 0，委托给 submitWithPriority ─────────────────────
    template<typename F, typename... Args>
    auto submit(F&& f, Args&&... args)
        -> std::future<typename std::invoke_result<F, Args...>::type>
    {
        return submitWithPriority(0, std::forward<F>(f), std::forward<Args>(args)...);
    }

    // ── submitWithPriority：显式指定优先级，数值越大越先执行 ─────────────────
    template<typename F, typename... Args>
    auto submitWithPriority(int priority, F&& f, Args&&... args)
        -> std::future<typename std::invoke_result<F, Args...>::type>
    {
        using Ret = typename std::invoke_result<F, Args...>::type;
        auto task = std::make_shared<std::packaged_task<Ret()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
        );
        std::future<Ret> result = task->get_future();

        std::unique_lock<std::mutex> lock(mutex_);

        if (maxQueueSize_ > 0) {
            if (blockOnFull_) {
                notFull_.wait(lock, [this] {
                    return stopped_ || queue_.size() < maxQueueSize_;
                    });
            }
            else {
                if (queue_.size() >= maxQueueSize_)
                    throw std::runtime_error("任务队列已满，提交被拒绝");
            }
        }

        if (stopped_)
            throw std::runtime_error("线程池已停止，无法提交任务");

        queue_.push(PriorityTask{
            priority,
            seqCounter_++,
            [task] { (*task)(); }
            });

        lock.unlock();
        notEmpty_.notify_one();
        return result;
    }

    // ── 运行时统计快照（线程安全） ───────────────────────────────────────────
    PoolStats stats() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return PoolStats{
            workers_.size(),
            activeThreads_.load(),
            queue_.size(),
            completedTasks_.load()
        };
    }

    // ── 优雅关闭 ─────────────────────────────────────────────────────────────
    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            stopped_ = true;
        }
        notEmpty_.notify_all();
        notFull_.notify_all();
        for (std::thread& w : workers_)
            if (w.joinable()) w.join();
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

private:
    void workerLoop() {
        while (true) {
            PriorityTask pt;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                notEmpty_.wait(lock, [this] {
                    return stopped_ || !queue_.empty();
                    });
                if (stopped_ && queue_.empty()) return;

                pt = std::move(const_cast<PriorityTask&>(queue_.top()));
                queue_.pop();
            }
            notFull_.notify_one();   // 通知可能阻塞在 submit 的调用方

            ++activeThreads_;
            try { pt.fn(); }
            catch (...) {}
            --activeThreads_;
            ++completedTasks_;
        }
    }

    // priority_queue 默认最大堆，operator> 让高优先级排前面
    // 默认 priority_queue 是 max-heap，配合 operator< 即可
    using PQueue = std::priority_queue<PriorityTask>;

    std::vector<std::thread> workers_;
    PQueue                   queue_;
    mutable std::mutex       mutex_;
    std::condition_variable  notEmpty_;
    std::condition_variable  notFull_;

    bool     stopped_;
    size_t   maxQueueSize_;
    bool     blockOnFull_;
    uint64_t seqCounter_;

    std::atomic<size_t>   activeThreads_;
    std::atomic<uint64_t> completedTasks_;
};