import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * CORRECT solution: thread-safe stock exchange.
 *
 * Concurrency design:
 *  - One OrderBook per symbol, each guarded by its own ReentrantLock.
 *    Symbols never interact, so different symbols trade in parallel.
 *  - Every read/write of a book's mutable state (price levels, order qty/price,
 *    id index) happens while holding that book's lock -> no data races.
 *  - Matching + insertion of one order is a single critical section (atomic).
 *  - IDs come from AtomicLong (lock-free, unique).
 *  - Only one lock is ever held at a time -> no lock ordering -> no deadlock.
 *  - Trade history is a ConcurrentLinkedQueue appended AFTER unlocking, so no
 *    I/O or slow work runs inside the critical section.
 *  - Prices are long cents (no floating-point comparison problems).

 */
public class StockExchangeCorrect {

    enum Side { BUY, SELL }

    /** Mutable; only touched while holding the owning book's lock. */
    static final class Order {
        final long id;
        final String trader, symbol;
        final Side side;
        int qty;
        long price;

        Order(long id, String trader, String symbol, Side side, int qty, long price) {
            this.id = id; this.trader = trader; this.symbol = symbol;
            this.side = side; this.qty = qty; this.price = price;
        }
    }

    record Trade(long id, String symbol, String buyer, String seller, int qty, long price) {
        @Override public String toString() {
            return String.format("Trade{id=%d %s buyer=%s seller=%s qty=%d price=%.2f}",
                    id, symbol, buyer, seller, qty, price / 100.0);
        }
    }

    static final class OrderBook {
        final String symbol;
        final ReentrantLock lock = new ReentrantLock();
        // price level -> FIFO queue (queue order = time priority)
        final TreeMap<Long, ArrayDeque<Order>> bids = new TreeMap<>(Comparator.reverseOrder());
        final TreeMap<Long, ArrayDeque<Order>> asks = new TreeMap<>();
        final Map<Long, Order> byId = new HashMap<>();

        OrderBook(String symbol) { this.symbol = symbol; }

        TreeMap<Long, ArrayDeque<Order>> sideMap(Side s) { return s == Side.BUY ? bids : asks; }

        /** Caller must hold lock. Matches against the opposite side, rests any remainder. */
        List<Trade> add(Order in, AtomicLong tradeIds) {
            List<Trade> trades = new ArrayList<>();
            TreeMap<Long, ArrayDeque<Order>> opp = sideMap(in.side == Side.BUY ? Side.SELL : Side.BUY);

            while (in.qty > 0 && !opp.isEmpty()) {
                Map.Entry<Long, ArrayDeque<Order>> level = opp.firstEntry();
                long best = level.getKey();
                boolean crosses = in.side == Side.BUY ? in.price >= best : in.price <= best;
                if (!crosses) break;

                ArrayDeque<Order> queue = level.getValue();
                Order resting = queue.peekFirst();
                int fill = Math.min(in.qty, resting.qty);
                in.qty -= fill;
                resting.qty -= fill;

                Order buy = in.side == Side.BUY ? in : resting;
                Order sell = in.side == Side.BUY ? resting : in;
                // Trade executes at the RESTING order's price.
                trades.add(new Trade(tradeIds.getAndIncrement(), symbol,
                        buy.trader, sell.trader, fill, best));

                if (resting.qty == 0) {
                    queue.pollFirst();
                    byId.remove(resting.id);
                    if (queue.isEmpty()) opp.remove(best);
                }
            }

            if (in.qty > 0) {
                sideMap(in.side).computeIfAbsent(in.price, k -> new ArrayDeque<>()).addLast(in);
                byId.put(in.id, in);
            }
            return trades;
        }

        /** Caller must hold lock. */
        void unlink(Order o) {
            TreeMap<Long, ArrayDeque<Order>> m = sideMap(o.side);
            ArrayDeque<Order> q = m.get(o.price);
            q.remove(o);
            if (q.isEmpty()) m.remove(o.price);
            byId.remove(o.id);
        }
    }

    static final class Exchange {
        private final ConcurrentHashMap<String, OrderBook> books = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Long, String> symbolOf = new ConcurrentHashMap<>();
        private final AtomicLong orderIds = new AtomicLong(1);
        private final AtomicLong tradeIds = new AtomicLong(1);
        private final ConcurrentLinkedQueue<Trade> history = new ConcurrentLinkedQueue<>();

        long submit(String trader, String symbol, Side side, int qty, long price) {
            validate(symbol, qty, price);
            long id = orderIds.getAndIncrement();
            symbolOf.put(id, symbol);
            OrderBook book = books.computeIfAbsent(symbol, OrderBook::new);
            List<Trade> trades;
            book.lock.lock();
            try {
                trades = book.add(new Order(id, trader, symbol, side, qty, price), tradeIds);
            } finally {
                book.lock.unlock();
            }
            history.addAll(trades); // outside the lock
            return id;
        }

        boolean cancel(long id, String trader) {
            OrderBook book = bookOf(id);
            if (book == null) return false;
            book.lock.lock();
            try {
                Order o = book.byId.get(id);
                if (o == null || !o.trader.equals(trader)) return false;
                book.unlink(o);
                return true;
            } finally {
                book.lock.unlock();
            }
        }

        /** Modify = remove + re-insert, so the order loses time priority and may trade immediately. */
        boolean modify(long id, String trader, int newQty, long newPrice) {
            validate("x", newQty, newPrice);
            OrderBook book = bookOf(id);
            if (book == null) return false;
            List<Trade> trades;
            book.lock.lock();
            try {
                Order o = book.byId.get(id);
                if (o == null || !o.trader.equals(trader)) return false;
                book.unlink(o);
                o.qty = newQty;
                o.price = newPrice;
                trades = book.add(o, tradeIds);
            } finally {
                book.lock.unlock();
            }
            history.addAll(trades);
            return true;
        }

        private OrderBook bookOf(long id) {
            String s = symbolOf.get(id);
            return s == null ? null : books.get(s);
        }

        private static void validate(String symbol, int qty, long price) {
            if (symbol == null || symbol.isBlank()) throw new IllegalArgumentException("empty symbol");
            if (qty <= 0) throw new IllegalArgumentException("qty must be > 0");
            if (price <= 0) throw new IllegalArgumentException("price must be > 0");
        }

        // ---- read-side helpers (all take the book lock) ----

        long restingQty(String symbol, Side side) {
            OrderBook b = books.get(symbol);
            if (b == null) return 0;
            b.lock.lock();
            try {
                long sum = 0;
                for (ArrayDeque<Order> q : b.sideMap(side).values())
                    for (Order o : q) sum += o.qty;
                return sum;
            } finally {
                b.lock.unlock();
            }
        }

        /** Returns {bestBid, bestAsk}; -1 when that side is empty. Read atomically. */
        long[] bestBidAsk(String symbol) {
            OrderBook b = books.get(symbol);
            if (b == null) return new long[]{-1, -1};
            b.lock.lock();
            try {
                return new long[]{
                        b.bids.isEmpty() ? -1 : b.bids.firstKey(),
                        b.asks.isEmpty() ? -1 : b.asks.firstKey()};
            } finally {
                b.lock.unlock();
            }
        }

        List<Trade> history() { return new ArrayList<>(history); }
    }

    // ------------------------------------------------------------------
    // Deterministic demo (single-threaded): checks matching rules
    // ------------------------------------------------------------------
    static void check(boolean cond, String msg) {
        if (!cond) throw new AssertionError(msg);
    }

    static void demo() {
        System.out.println("=== Deterministic demo ===");
        Exchange ex = new Exchange();
        long bob = ex.submit("Bob", "AAPL", Side.BUY, 100, 15500);
        ex.submit("Alice", "AAPL", Side.SELL, 60, 15000);   // hits Bob's resting 155.00
        long eric = ex.submit("Eric", "AAPL", Side.BUY, 70, 14900);
        ex.submit("Fiona", "AAPL", Side.SELL, 30, 14800);   // best bid is still Bob (price priority)
        ex.submit("Carol", "AAPL", Side.SELL, 20, 15200);   // 10 to Bob, 10 rests at 152.00

        check(!ex.modify(eric, "Mallory", 70, 15300), "non-owner must not modify");
        check(ex.modify(eric, "Eric", 70, 15300), "owner modify");  // crosses Carol's ask
        check(ex.cancel(eric, "Eric"), "cancel");
        check(!ex.cancel(bob, "Bob"), "Bob already fully filled");

        // FIFO time priority on same price
        ex.submit("A", "GOOG", Side.BUY, 10, 10000);
        ex.submit("B", "GOOG", Side.BUY, 10, 10000);
        ex.submit("S", "GOOG", Side.SELL, 10, 10000);

        List<Trade> h = ex.history();
        h.forEach(System.out::println);

        long[] prices = {15500, 15500, 15500, 15200, 10000};
        int[] qtys = {60, 30, 10, 10, 10};
        check(h.size() == 5, "trade count " + h.size());
        for (int i = 0; i < 5; i++) {
            check(h.get(i).price() == prices[i] && h.get(i).qty() == qtys[i], "trade " + i);
        }
        check(h.get(4).buyer().equals("A"), "FIFO: A before B");
        System.out.println("Demo checks passed.\n");
    }

    // ------------------------------------------------------------------
    // Concurrent stress test with invariants
    // ------------------------------------------------------------------
    static void stress() throws Exception {
        System.out.println("=== Concurrent stress test ===");
        String[] symbols = {"AAPL", "GOOG", "MSFT", "TSLA"};
        int threads = 8, perThread = 25_000;
        Exchange ex = new Exchange();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        long[][] subBuy = new long[threads][symbols.length];   // per-thread: no sharing
        long[][] subSell = new long[threads][symbols.length];
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            futures.add(pool.submit(() -> {
                Random rnd = new Random(1000 + tid);           // fixed seed: reproducible inputs
                try { start.await(); } catch (InterruptedException e) { return; }
                for (int i = 0; i < perThread; i++) {
                    int s = rnd.nextInt(symbols.length);
                    Side side = rnd.nextBoolean() ? Side.BUY : Side.SELL;
                    int qty = 1 + rnd.nextInt(100);
                    long price = 10_000 + rnd.nextInt(41) - 20;
                    ex.submit("T" + tid, symbols[s], side, qty, price);
                    if (side == Side.BUY) subBuy[tid][s] += qty; else subSell[tid][s] += qty;
                }
            }));
        }

        long t0 = System.nanoTime();
        start.countDown();                                     // all clients start together
        for (Future<?> f : futures) f.get();                   // rethrows worker exceptions
        pool.shutdown();
        long ms = (System.nanoTime() - t0) / 1_000_000;

        List<Trade> history = ex.history();
        Set<Long> tradeIds = new HashSet<>();
        for (Trade tr : history) check(tradeIds.add(tr.id()), "duplicate trade id " + tr.id());

        for (int s = 0; s < symbols.length; s++) {
            long buy = 0, sell = 0, traded = 0;
            for (int t = 0; t < threads; t++) { buy += subBuy[t][s]; sell += subSell[t][s]; }
            for (Trade tr : history) if (tr.symbol().equals(symbols[s])) traded += tr.qty();
            long restBuy = ex.restingQty(symbols[s], Side.BUY);
            long restSell = ex.restingQty(symbols[s], Side.SELL);
            long[] ba = ex.bestBidAsk(symbols[s]);

            check(buy == traded + restBuy, symbols[s] + ": buy qty not conserved");
            check(sell == traded + restSell, symbols[s] + ": sell qty not conserved");
            check(ba[0] == -1 || ba[1] == -1 || ba[0] < ba[1], symbols[s] + ": book is crossed");
            System.out.printf("%s  submitted buy=%d sell=%d  traded=%d  resting buy=%d sell=%d  OK%n",
                    symbols[s], buy, sell, traded, restBuy, restSell);
        }
        int n = threads * perThread;
        System.out.printf("%d orders, %d trades in %d ms (%.0f orders/s). All invariants hold.%n",
                n, history.size(), ms, n * 1000.0 / Math.max(ms, 1));
    }

    public static void main(String[] args) throws Exception {
        demo();
        stress();
    }
}
