import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * FAULTY solution #1: data races and data inconsistency.
 *
 * Same matching logic as the correct version, but with NO synchronisation.
 * Intentional bugs:
 *  BUG 1  orderId++ / tradeId++ on plain longs: read-modify-write is not atomic
 *         -> duplicate IDs (lost updates).
 *  BUG 2  buys/sells/history are plain ArrayLists shared by all threads
 *         -> lost adds, null slots, ConcurrentModificationException,
 *            IndexOutOfBounds, NPE while sorting/iterating.
 *  BUG 3  match() is a check-then-act sequence (read qty, compute fill, write qty)
 *         with no mutual exclusion -> two threads fill the same order
 *         (over-fill, negative quantities, quantity not conserved).
 *  BUG 4  modify() writes qty/price while another thread is matching the order
 *         -> lost updates, price/qty torn relative to each other.
 *
 * Fix: one lock per book around submit/modify/cancel (see StockExchangeCorrect).
 *
 * Results vary by run: races are nondeterministic
 */
public class FaultyRaces {

    enum Side { BUY, SELL }

    static class Order {
        long id; String trader, symbol; Side side; int qty; long price;
        Order(long id, String trader, String symbol, Side side, int qty, long price) {
            this.id = id; this.trader = trader; this.symbol = symbol;
            this.side = side; this.qty = qty; this.price = price;
        }
    }

    static class Trade {
        long id; String symbol; int qty; long price;
        Trade(long id, String symbol, int qty, long price) {
            this.id = id; this.symbol = symbol; this.qty = qty; this.price = price;
        }
    }

    static class RacyExchange {
        final List<Order> buys = new ArrayList<>();      // BUG 2
        final List<Order> sells = new ArrayList<>();     // BUG 2
        final List<Trade> history = new ArrayList<>();   // BUG 2
        long orderId = 1;                                // BUG 1
        long tradeId = 1;                                // BUG 1

        long submit(String trader, String symbol, Side side, int qty, long price) {
            Order o = new Order(orderId++, trader, symbol, side, qty, price); // BUG 1
            if (side == Side.BUY) buys.add(o); else sells.add(o);             // BUG 2
            match();
            return o.id;
        }

        boolean modify(long id, int qty, long price) {   // BUG 4: no lock
            for (Order o : buys)  if (o != null && o.id == id) { o.qty = qty; o.price = price; return true; }
            for (Order o : sells) if (o != null && o.id == id) { o.qty = qty; o.price = price; return true; }
            return false;
        }

        void match() {
            buys.sort(Comparator.comparingLong((Order o) -> o.price).reversed().thenComparingLong(o -> o.id));
            sells.sort(Comparator.comparingLong((Order o) -> o.price).thenComparingLong(o -> o.id));

            for (int i = 0; i < buys.size(); i++) {
                Order b = buys.get(i);
                for (int j = 0; j < sells.size(); j++) {
                    Order s = sells.get(j);
                    if (!b.symbol.equals(s.symbol)) continue;
                    if (b.price < s.price) break;

                    int q = Math.min(b.qty, s.qty);       // BUG 3: check ...
                    b.qty -= q;                           //        ... then act, not atomic
                    s.qty -= q;
                    history.add(new Trade(tradeId++, b.symbol, q, s.price));

                    if (s.qty == 0) { sells.remove(j); j--; }
                    if (b.qty == 0) { buys.remove(i); i--; break; }
                }
            }
        }
    }

    static void round(int round) throws Exception {
        String[] symbols = {"AAPL", "GOOG", "MSFT", "TSLA"};
        int threads = 8, perThread = 2_000;
        RacyExchange ex = new RacyExchange();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        LongAdder[] subBuy = new LongAdder[symbols.length];
        LongAdder[] subSell = new LongAdder[symbols.length];
        for (int s = 0; s < symbols.length; s++) { subBuy[s] = new LongAdder(); subSell[s] = new LongAdder(); }
        ConcurrentHashMap<String, LongAdder> errors = new ConcurrentHashMap<>();
        List<Future<List<Long>>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            futures.add(pool.submit(() -> {
                Random rnd = new Random(1000 + tid);
                List<Long> ids = new ArrayList<>();
                start.await();
                for (int i = 0; i < perThread; i++) {
                    int s = rnd.nextInt(symbols.length);
                    Side side = rnd.nextBoolean() ? Side.BUY : Side.SELL;
                    int qty = 1 + rnd.nextInt(100);
                    long price = 10_000 + rnd.nextInt(41) - 20;
                    if (side == Side.BUY) subBuy[s].add(qty); else subSell[s].add(qty);
                    try {
                        ids.add(ex.submit("T" + tid, symbols[s], side, qty, price));
                    } catch (Throwable e) {
                        errors.computeIfAbsent(e.getClass().getSimpleName(), k -> new LongAdder()).increment();
                    }
                    if (i % 50 == 0) {                                  // race modify() with matching
                        try { ex.modify(1 + rnd.nextInt(Math.max(1, i + 1)), 5, price); }
                        catch (Throwable e) {
                            errors.computeIfAbsent(e.getClass().getSimpleName(), k -> new LongAdder()).increment();
                        }
                    }
                }
                return ids;
            }));
        }
        start.countDown();
        List<Long> allIds = new ArrayList<>();
        for (Future<List<Long>> f : futures) allIds.addAll(f.get());
        pool.shutdown();

        System.out.println("--- round " + round + " ---");
        int dup = allIds.size() - new HashSet<>(allIds).size();
        System.out.println("orders accepted: " + allIds.size() + "   DUPLICATE ids: " + dup
                + (dup > 0 ? "   <-- BUG 1" : ""));
        System.out.println("exceptions thrown: " + (errors.isEmpty() ? "none" : errors)
                + (errors.isEmpty() ? "" : "   <-- BUG 2"));

        int nulls = 0, negative = 0;
        long[] restBuy = new long[symbols.length], restSell = new long[symbols.length];
        long[] traded = new long[symbols.length];
        Map<String, Integer> idx = new HashMap<>();
        for (int s = 0; s < symbols.length; s++) idx.put(symbols[s], s);
        for (Order o : ex.buys)  { if (o == null) nulls++; else { if (o.qty < 0) negative++; restBuy[idx.get(o.symbol)] += o.qty; } }
        for (Order o : ex.sells) { if (o == null) nulls++; else { if (o.qty < 0) negative++; restSell[idx.get(o.symbol)] += o.qty; } }
        for (Trade tr : ex.history) if (tr != null) traded[idx.get(tr.symbol)] += tr.qty;

        System.out.println("null slots in lists: " + nulls + "   negative-qty orders: " + negative
                + (nulls + negative > 0 ? "   <-- BUG 2/3" : ""));
        int broken = 0;
        for (int s = 0; s < symbols.length; s++) {
            boolean buyOk = subBuy[s].sum() == traded[s] + restBuy[s];
            boolean sellOk = subSell[s].sum() == traded[s] + restSell[s];
            if (!buyOk || !sellOk) broken++;
            System.out.printf("%s buy: submitted=%d traded+resting=%d %s | sell: submitted=%d traded+resting=%d %s%n",
                    symbols[s], subBuy[s].sum(), traded[s] + restBuy[s], buyOk ? "ok" : "MISMATCH",
                    subSell[s].sum(), traded[s] + restSell[s], sellOk ? "ok" : "MISMATCH");
        }
        System.out.println(broken > 0
                ? "RESULT: " + broken + "/4 symbols violate quantity conservation (data inconsistency)"
                : "RESULT: no violation this round (races are intermittent - run again)");
    }

    public static void main(String[] args) throws Exception {
        for (int r = 1; r <= 3; r++) round(r);
    }
}
