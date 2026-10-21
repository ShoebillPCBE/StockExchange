import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * FAULTY solution #2: deadlock, livelock, performance.
 * Each demo is a reduced model of the exchange: per-symbol books with a lock
 * and a volume counter; "swap" is a cross-symbol operation that needs BOTH
 * books (e.g. a pair/spread order).
 *
 * Usage: java FaultyDeadlock.java <mode>
 *   deadlock         lock-ordering deadlock (AAPL->GOOG vs GOOG->AAPL)
 *   deadlock-fixed   same, with a global lock order (alphabetical by symbol)
 *   livelock         tryLock + identical back-off in lock-step: threads run, no progress
 *   livelock-fixed   randomised back-off: completes
 *   performance      global lock + I/O inside lock vs per-symbol lock + I/O outside
 */
public class FaultyDeadlock {

    static class Book {
        final String symbol;
        final ReentrantLock lock = new ReentrantLock();
        long volume;
        Book(String symbol) { this.symbol = symbol; }
    }

    static void sleepQuiet(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    // ------------------------------------------------------------------
    // 1) DEADLOCK: Coffman conditions - mutual exclusion, hold-and-wait,
    //    no preemption, circular wait. Break circular wait with lock ordering.
    // ------------------------------------------------------------------
    static void swap(Book x, Book y, boolean ordered) {
        Book first = x, second = y;
        if (ordered && x.symbol.compareTo(y.symbol) > 0) { first = y; second = x; } // FIX
        first.lock.lock();
        try {
            sleepQuiet(1);                      // widen the race window
            second.lock.lock();                 // BUG (unordered): wait while holding 'first'
            try { x.volume++; y.volume++; }
            finally { second.lock.unlock(); }
        } finally { first.lock.unlock(); }
    }

    static void deadlock(boolean fixed) throws Exception {
        Book aapl = new Book("AAPL"), goog = new Book("GOOG");
        AtomicLong done = new AtomicLong();
        int iterations = 1000;
        Thread t1 = new Thread(() -> { for (int i = 0; i < iterations; i++) { swap(aapl, goog, fixed); done.incrementAndGet(); } },
                "client-1 (AAPL then GOOG)");
        Thread t2 = new Thread(() -> { for (int i = 0; i < iterations; i++) { swap(goog, aapl, fixed); done.incrementAndGet(); } },
                "client-2 (GOOG then AAPL)");
        t1.setDaemon(true); t2.setDaemon(true);
        t1.start(); t2.start();
        t1.join(3000); t2.join(3000);

        long[] dead = ManagementFactory.getThreadMXBean().findDeadlockedThreads();
        System.out.println("swaps completed: " + done.get() + " / " + (2 * iterations));
        if (dead == null) {
            System.out.println("No deadlock detected.");
        } else {
            System.out.println("DEADLOCK detected by ThreadMXBean:");
            for (ThreadInfo ti : ManagementFactory.getThreadMXBean().getThreadInfo(dead)) {
                System.out.println("  " + ti.getThreadName() + " is " + ti.getThreadState()
                        + ", waiting for " + ti.getLockName() + " held by " + ti.getLockOwnerName());
            }
        }
    }

    // ------------------------------------------------------------------
    // 2) LIVELOCK: threads are not blocked (they keep running and retrying)
    //    but make no progress. The CyclicBarrier models perfectly symmetric
    //    timing: both always hold their first lock when the other tries it.
    //    findDeadlockedThreads() returns null: the JVM does NOT see this.
    // ------------------------------------------------------------------
    static void livelock(boolean fixed) throws Exception {
        ReentrantLock a = new ReentrantLock(), b = new ReentrantLock();
        CyclicBarrier lockstep = fixed ? null : new CyclicBarrier(2);
        AtomicLong attempts = new AtomicLong(), successes = new AtomicLong();

        Thread w1 = worker("worker-1", a, b, lockstep, fixed, attempts, successes);
        Thread w2 = worker("worker-2", b, a, lockstep, fixed, attempts, successes);
        w1.start(); w2.start();
        w1.join(3000); w2.join(3000);

        System.out.println("failed attempts: " + attempts.get() + ", successes: " + successes.get() + " / 2");
        System.out.println("worker states: " + w1.getState() + ", " + w2.getState());
        System.out.println("findDeadlockedThreads(): " + ManagementFactory.getThreadMXBean().findDeadlockedThreads());
        if (successes.get() < 2) System.out.println("LIVELOCK: busy retrying, no progress.");
    }

    static Thread worker(String name, ReentrantLock first, ReentrantLock second, CyclicBarrier lockstep,
                         boolean fixed, AtomicLong attempts, AtomicLong successes) {
        Thread t = new Thread(() -> {
            try {
                while (true) {
                    first.lock();
                    try {
                        if (lockstep != null) lockstep.await();
                        boolean got = second.tryLock();           // politely back off instead of blocking...
                        if (lockstep != null) lockstep.await();
                        if (got) {
                            try { successes.incrementAndGet(); } finally { second.unlock(); }
                            return;
                        }
                    } finally { first.unlock(); }                 // ...release, then retry
                    attempts.incrementAndGet();
                    // BUG: identical, deterministic back-off => same collision again.
                    // FIX: randomised back-off breaks the symmetry.
                    sleepQuiet(fixed ? ThreadLocalRandom.current().nextInt(1, 10) : 1);
                }
            } catch (InterruptedException | BrokenBarrierException e) {
                Thread.currentThread().interrupt();
            }
        }, name);
        t.setDaemon(true);
        return t;
    }

    // ------------------------------------------------------------------
    // 3) PERFORMANCE: correct but needlessly serialised.
    //    I/O (audit log write simulated by sleep) happens inside the lock,
    //    and a single global lock serialises unrelated symbols.
    // ------------------------------------------------------------------
    enum Cfg { GLOBAL_LOCK_IO_INSIDE, PER_SYMBOL_IO_INSIDE, PER_SYMBOL_IO_OUTSIDE }

    static void performance() throws Exception {
        for (Cfg cfg : Cfg.values()) {
            Book[] books = {new Book("AAPL"), new Book("GOOG"), new Book("MSFT"), new Book("TSLA")};
            ReentrantLock global = new ReentrantLock();
            int threads = 8, perThread = 150;
            Thread[] ts = new Thread[threads];
            long t0 = System.nanoTime();
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                ts[t] = new Thread(() -> {
                    for (int i = 0; i < perThread; i++) {
                        Book b = books[(tid + i) % books.length];
                        ReentrantLock l = cfg == Cfg.GLOBAL_LOCK_IO_INSIDE ? global : b.lock;
                        l.lock();
                        try {
                            b.volume++;                                   // the real critical section
                            if (cfg != Cfg.PER_SYMBOL_IO_OUTSIDE) sleepQuiet(1); // I/O inside lock
                        } finally { l.unlock(); }
                        if (cfg == Cfg.PER_SYMBOL_IO_OUTSIDE) sleepQuiet(1);     // I/O after unlock
                    }
                });
                ts[t].start();
            }
            for (Thread t : ts) t.join();
            long ms = (System.nanoTime() - t0) / 1_000_000;
            long total = 0;
            for (Book b : books) total += b.volume;
            System.out.printf("%-26s %5d ms  %7.0f ops/s  (ops=%d)%n",
                    cfg, ms, total * 1000.0 / Math.max(ms, 1), total);
        }
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "";
        switch (mode) {
            case "deadlock" -> deadlock(false);
            case "deadlock-fixed" -> deadlock(true);
            case "livelock" -> livelock(false);
            case "livelock-fixed" -> livelock(true);
            case "performance" -> performance();
            default -> System.out.println("Usage: java FaultyDeadlock.java "
                    + "[deadlock|deadlock-fixed|livelock|livelock-fixed|performance]");
        }
        System.exit(0);
    }
}
