/**
 * 线程池 v2 测试程序
 * 覆盖：基础功能 / 异常传播 / 有界队列+背压 / 优先级 / 统计监控 / 性能对比
 */
#include <iostream>
#include <chrono>
#include <vector>
#include <future>
#include <stdexcept>
#include <atomic>
#include <thread>
#include <sstream>
#include "ThreadPool.h"

void printSection(const std::string& t) {
    std::cout << "\n========== " << t << " ==========" << std::endl;
}
void pass(const std::string& msg) { std::cout << "[PASS] " << msg << std::endl; }
void fail(const std::string& msg) { std::cout << "[FAIL] " << msg << std::endl; }

int heavyTask(int id, int ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    return id * id;
}

// ===== TEST 1：基础功能 & 返回值 =====
void test_basic() {
    printSection("TEST 1: 基础功能 & 返回值");
    ThreadPool pool(4);
    std::vector<std::future<int>> futs;
    for (int i = 0; i < 8; i++)
        futs.emplace_back(pool.submit(heavyTask, i, 30));
    bool ok = true;
    for (int i = 0; i < 8; i++) {
        int r = futs[i].get();
        std::cout << "  任务" << i << " = " << r << (r == i * i ? " ✓" : " ✗") << std::endl;
        if (r != i * i) ok = false;
    }
    ok ? pass("返回值全部正确") : fail("有结果错误");
}

// ===== TEST 2：异常传播 =====
void test_exception() {
    printSection("TEST 2: 异常传播");
    ThreadPool pool(2);
    auto f = pool.submit([] { throw std::runtime_error("任务内部异常"); return 0; });
    try {
        f.get();
        fail("未捕获到异常");
    }
    catch (const std::runtime_error& e) {
        pass(std::string("成功捕获异常: ") + e.what());
    }
    auto f2 = pool.submit([] { return 42; });
    (f2.get() == 42) ? pass("异常后线程池仍正常") : fail("异常后线程池损坏");
}

// ===== TEST 3：有界队列 – 非阻塞拒绝模式 =====
void test_bounded_queue_reject() {
    printSection("TEST 3a: 有界队列 – 队满拒绝");
    // 1线程，队列上限1，blockOnFull=false
    // 策略：让工作线程卡在第一个任务，再向队列里塞直到满
    ThreadPool pool(1, /*maxQueue=*/1, /*blockOnFull=*/false);

    std::promise<void> blocker_go;
    auto blocker_ready = blocker_go.get_future().share();

    // 占住线程，等外部信号才放行
    auto blocking = pool.submit([blocker_ready] {
        blocker_ready.wait();
        return 0;
        });

    // 给工作线程时间取走上面那个任务
    std::this_thread::sleep_for(std::chrono::milliseconds(30));

    // 队列现在空了，塞1个填满（maxQueue=1）
    auto queued = pool.submit([] { return 1; });

    // 再提交一个，应被拒绝
    bool rejected = false;
    try {
        pool.submit([] { return 2; });
        fail("队满应抛异常，但未抛");
    }
    catch (const std::runtime_error& e) {
        rejected = true;
        pass(std::string("队满正确拒绝: ") + e.what());
    }

    blocker_go.set_value(); // 放行工作线程
    blocking.get();
    queued.get();
    if (!rejected) fail("队满未触发拒绝");
}


// ===== TEST 3b：有界队列 – 阻塞背压模式 =====
void test_bounded_queue_block() {
    printSection("TEST 3b: 有界队列 – 背压阻塞");
    ThreadPool pool(2, /*maxQueue=*/3, /*blockOnFull=*/true);

    std::atomic<int> submitted{ 0 };
    std::atomic<bool> done{ false };

    // 后台线程不断提交，最多提交10个
    std::thread producer([&] {
        for (int i = 0; i < 10; i++) {
            pool.submit([i] { std::this_thread::sleep_for(std::chrono::milliseconds(50)); return i; });
            submitted++;
        }
        done = true;
        });

    // 等待生产者完成，最多3秒
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (!done && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(20));

    producer.join();
    done ? pass("背压模式：10个任务全部完成，无死锁") : fail("背压模式超时或死锁");
}

// ===== TEST 4：优先级队列 =====
void test_priority() {
    printSection("TEST 4: 优先级队列");

    // 1个线程，队列足够大
    // 先提交一个慢任务占住线程，让后续任务在队列里排好序
    ThreadPool pool(1, 20, false);

    std::vector<int> executionOrder;
    std::mutex orderMu;

    // 占住线程
    auto blocker = pool.submitWithPriority(0, [] {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        });

    // 提交不同优先级任务（线程被占，它们在队列里等待）
    std::vector<std::future<void>> futs;
    for (int prio : {1, 5, 3, 10, 2}) {
        int p = prio;
        futs.emplace_back(pool.submitWithPriority(prio, [p, &executionOrder, &orderMu] {
            std::lock_guard<std::mutex> lg(orderMu);
            executionOrder.push_back(p);
            }));
    }

    blocker.get();
    for (auto& f : futs) f.get();

    std::cout << "  执行顺序（优先级）: ";
    for (int p : executionOrder) std::cout << p << " ";
    std::cout << std::endl;

    // 验证降序执行
    bool sorted = true;
    for (size_t i = 1; i < executionOrder.size(); i++)
        if (executionOrder[i] > executionOrder[i - 1]) { sorted = false; break; }

    sorted ? pass("高优先级任务先执行") : fail("优先级顺序错误");
}

// ===== TEST 4b：同优先级 FIFO =====
void test_priority_fifo() {
    printSection("TEST 4b: 同优先级 FIFO");
    ThreadPool pool(1, 10, false);

    std::vector<int> order;
    std::mutex mu;

    auto blocker = pool.submitWithPriority(0, [] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        });

    std::vector<std::future<void>> futs;
    for (int i = 0; i < 5; i++) {
        int id = i;
        futs.emplace_back(pool.submitWithPriority(5, [id, &order, &mu] {
            std::lock_guard<std::mutex> lg(mu);
            order.push_back(id);
            }));
    }

    blocker.get();
    for (auto& f : futs) f.get();

    std::cout << "  同优先级执行顺序: ";
    for (int x : order) std::cout << x << " ";
    std::cout << std::endl;

    bool fifo = true;
    for (size_t i = 1; i < order.size(); i++)
        if (order[i] < order[i - 1]) { fifo = false; break; }
    fifo ? pass("同优先级保持提交顺序（FIFO）") : fail("FIFO 顺序错误");
}

// ===== TEST 5：运行时统计 =====
void test_stats() {
    printSection("TEST 5: 运行时统计");
    ThreadPool pool(4, 0, true);

    // 空闲时
    auto s0 = pool.stats();
    std::cout << "  空闲: 线程=" << s0.totalThreads
        << " 活跃=" << s0.activeThreads
        << " 队列=" << s0.pendingTasks
        << " 完成=" << s0.completedTasks << std::endl;
    (s0.totalThreads == 4 && s0.activeThreads == 0) ? pass("空闲状态正确") : fail("空闲状态错误");

    // 提交慢任务后采样
    std::vector<std::future<int>> futs;
    for (int i = 0; i < 8; i++)
        futs.emplace_back(pool.submit([i] { std::this_thread::sleep_for(std::chrono::milliseconds(200)); return i; }));

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto s1 = pool.stats();
    std::cout << "  运行中: 活跃=" << s1.activeThreads
        << " 队列=" << s1.pendingTasks << std::endl;
    (s1.activeThreads > 0) ? pass("运行中活跃线程数 > 0") : fail("活跃线程数应 > 0");

    for (auto& f : futs) f.get();

    auto s2 = pool.stats();
    std::cout << "  结束: 完成=" << s2.completedTasks
        << " 队列=" << s2.pendingTasks << std::endl;
    (s2.completedTasks >= 8 && s2.pendingTasks == 0) ? pass("完成计数正确，≥8 个任务已完成") : fail("完成计数错误，completedTasks=" + std::to_string(s2.completedTasks));
}

// ===== TEST 6：性能对比 =====
void test_performance() {
    printSection("TEST 6: 性能对比");
    const int N = 20, MS = 100;

    auto t0 = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < N; i++) heavyTask(i, MS);
    double serial = std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - t0).count();
    std::cout << "  串行: " << serial << " 秒" << std::endl;

    t0 = std::chrono::high_resolution_clock::now();
    {
        ThreadPool pool(4);
        std::vector<std::future<int>> futs;
        for (int i = 0; i < N; i++)
            futs.emplace_back(pool.submit(heavyTask, i, MS));
        for (auto& f : futs) f.get();
    }
    double pooled = std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - t0).count();
    std::cout << "  线程池(4线程): " << pooled << " 秒  加速比: " << serial / pooled << "x" << std::endl;
    (serial / pooled > 2.0) ? pass("加速效果显著") : fail("加速比偏低");
}

int main() {
    std::cout << "====== ThreadPool v2 测试套件 ======" << std::endl;
    test_basic();
    test_exception();
    test_bounded_queue_reject();
    test_bounded_queue_block();
    test_priority();
    test_priority_fifo();
    test_stats();
    test_performance();
    std::cout << "\n====== 全部测试完成 ======" << std::endl;
    return 0;
}