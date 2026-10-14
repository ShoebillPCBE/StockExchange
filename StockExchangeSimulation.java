
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class StockExchangeSimulation {

    enum OrderType {
        BUY, SELL
    }

    static class Order {
        final long id;
        final String trader;
        final String symbol;
        final OrderType type;
        int quantity;
        double price;

        Order(long id, String trader, String symbol,
              OrderType type, int quantity, double price) {
            this.id = id;
            this.trader = trader;
            this.symbol = symbol;
            this.type = type;
            this.quantity = quantity;
            this.price = price;
        }

        @Override
        public String toString() {
            return String.format(
                "Order{id=%d, trader='%s', stock='%s', type=%s, "
                + "quantity=%d, price=%.2f}",
                id, trader, symbol, type, quantity, price
            );
        }
    }

    static class Transaction {
        final long id;
        final String symbol;
        final String buyer;
        final String seller;
        final int quantity;
        final double price;
        final LocalDateTime time;

        Transaction(long id, String symbol, String buyer,
                    String seller, int quantity, double price) {
            this.id = id;
            this.symbol = symbol;
            this.buyer = buyer;
            this.seller = seller;
            this.quantity = quantity;
            this.price = price;
            this.time = LocalDateTime.now();
        }

        @Override
        public String toString() {
            return String.format(
                "Trade{id=%d, stock='%s', buyer='%s', seller='%s', "
                + "quantity=%d, price=%.2f, time=%s}",
                id, symbol, buyer, seller, quantity, price, time
            );
        }
    }

    static class StockExchange {
        private final List<Order> buyOrders = new ArrayList<>();
        private final List<Order> sellOrders = new ArrayList<>();
        private final List<Transaction> history = new ArrayList<>();

        private final ReentrantLock lock = new ReentrantLock();
        private final AtomicLong orderIds = new AtomicLong(1);
        private final AtomicLong transactionIds = new AtomicLong(1);

        // Submit a new order and immediately try to match it.
        public long submitOrder(String trader, String symbol,
                                OrderType type, int quantity,
                                double price) {
            validateOrder(symbol, quantity, price);

            lock.lock();
            try {
                Order order = new Order(
                    orderIds.getAndIncrement(), trader, symbol,
                    type, quantity, price
                );

                System.out.println("NEW: " + order);

                if (type == OrderType.BUY) {
                    buyOrders.add(order);
                } else {
                    sellOrders.add(order);
                }

                matchOrders();
                return order.id;
            } finally {
                lock.unlock();
            }
        }

        private void validateOrder(String symbol,
                                   int quantity, double price) {
            if (symbol == null || symbol.trim().isEmpty()) {
                throw new IllegalArgumentException(
                    "Stock symbol cannot be empty");
            }
            if (quantity <= 0) {
                throw new IllegalArgumentException(
                    "Quantity must be positive");
            }
            if (!Double.isFinite(price) || price <= 0) {
                throw new IllegalArgumentException(
                    "Price must be positive and finite");
            }
        }

        // Called only while holding the exchange lock.
        private void matchOrders() {
            // Best-priced buy orders first.
            buyOrders.sort(
                Comparator.comparingDouble(
                    (Order o) -> o.price
                ).reversed().thenComparingLong(o -> o.id)
            );

            // Lowest-priced sell orders first.
            sellOrders.sort(
                Comparator.comparingDouble(
                    (Order o) -> o.price
                ).thenComparingLong(o -> o.id)
            );

            for (int i = 0; i < buyOrders.size(); ) {
                Order buy = buyOrders.get(i);
                boolean traded = false;

                for (int j = 0; j < sellOrders.size(); j++) {
                    Order sell = sellOrders.get(j);

                    if (!buy.symbol.equals(sell.symbol)) {
                        continue;
                    }

                    if (buy.price < sell.price) {
                        // Sell orders are sorted by increasing price.
                        // No later sell order can match this buyer.
                        break;
                    }

                    int quantity = Math.min(
                        buy.quantity, sell.quantity
                    );

                    // The trade executes at the seller's limit price.
                    Transaction trade = new Transaction(
                        transactionIds.getAndIncrement(),
                        buy.symbol, buy.trader, sell.trader,
                        quantity, sell.price
                    );

                    // All changes occur under the same lock.
                    buy.quantity -= quantity;
                    sell.quantity -= quantity;
                    history.add(trade);

                    System.out.println("TRADE: " + trade);
                    traded = true;

                    if (sell.quantity == 0) {
                        sellOrders.remove(j);
                        j--;
                    }

                    if (buy.quantity == 0) {
                        buyOrders.remove(i);
                        i--;
                        break;
                    }
                }

                i++;
            }
        }

        // Modify an existing, unfilled or partially filled order.
        // The lock prevents a concurrent trade during modification.
        public boolean modifyOrder(long orderId,
                                   int newQuantity,
                                   double newPrice) {
            if (newQuantity <= 0 || !Double.isFinite(newPrice)
                    || newPrice <= 0) {
                throw new IllegalArgumentException(
                    "Invalid quantity or price");
            }

            lock.lock();
            try {
                Order order = findOrder(orderId);

                if (order == null) {
                    return false; // Already filled or not found.
                }

                order.quantity = newQuantity;
                order.price = newPrice;

                System.out.println("MODIFIED: " + order);

                // Price changes can make the order immediately tradable.
                matchOrders();
                return true;
            } finally {
                lock.unlock();
            }
        }

        private Order findOrder(long id) {
            for (Order o : buyOrders) {
                if (o.id == id) {
                    return o;
                }
            }

            for (Order o : sellOrders) {
                if (o.id == id) {
                    return o;
                }
            }

            return null;
        }

        // Return snapshots instead of exposing mutable internal lists.
        public List<Order> getOrders() {
            lock.lock();
            try {
                List<Order> result = new ArrayList<>();

                for (Order o : buyOrders) {
                    result.add(new Order(
                        o.id, o.trader, o.symbol,
                        o.type, o.quantity, o.price
                    ));
                }

                for (Order o : sellOrders) {
                    result.add(new Order(
                        o.id, o.trader, o.symbol,
                        o.type, o.quantity, o.price
                    ));
                }

                return result;
            } finally {
                lock.unlock();
            }
        }

        public List<Transaction> getHistory() {
            lock.lock();
            try {
                return new ArrayList<>(history);
            } finally {
                lock.unlock();
            }
        }

        public void printOrderBook() {
            lock.lock();
            try {
                System.out.println("\n=== ORDER BOOK ===");

                System.out.println("BUY ORDERS:");
                for (Order o : buyOrders) {
                    System.out.println(o);
                }

                System.out.println("SELL ORDERS:");
                for (Order o : sellOrders) {
                    System.out.println(o);
                }
            } finally {
                lock.unlock();
            }
        }

        public void printHistory() {
            lock.lock();
            try {
                System.out.println("\n=== TRANSACTION HISTORY ===");
                for (Transaction t : history) {
                    System.out.println(t);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    static class Buyer implements Runnable {
        private final StockExchange exchange;
        private final String name;
        private final String symbol;
        private final int quantity;
        private final double price;

        Buyer(StockExchange exchange, String name, String symbol,
              int quantity, double price) {
            this.exchange = exchange;
            this.name = name;
            this.symbol = symbol;
            this.quantity = quantity;
            this.price = price;
        }

        @Override
        public void run() {
            exchange.submitOrder(
                name, symbol, OrderType.BUY, quantity, price
            );
        }
    }

    static class Seller implements Runnable {
        private final StockExchange exchange;
        private final String name;
        private final String symbol;
        private final int quantity;
        private final double price;

        Seller(StockExchange exchange, String name, String symbol,
               int quantity, double price) {
            this.exchange = exchange;
            this.name = name;
            this.symbol = symbol;
            this.quantity = quantity;
            this.price = price;
        }

        @Override
        public void run() {
            exchange.submitOrder(
                name, symbol, OrderType.SELL, quantity, price
            );
        }
    }

    public static void main(String[] args)
            throws InterruptedException {

        StockExchange exchange = new StockExchange();

        // Standard Java thread pool simulates concurrent clients.
        ExecutorService clients = Executors.newFixedThreadPool(6);

        clients.submit(new Seller(
            exchange, "Seller-Alice", "AAPL", 100, 150.00
        ));

        clients.submit(new Buyer(
            exchange, "Buyer-Bob", "AAPL", 60, 155.00
        ));

        clients.submit(new Seller(
            exchange, "Seller-Charlie", "GOOG", 80, 140.00
        ));

        clients.submit(new Buyer(
            exchange, "Buyer-Diana", "GOOG", 50, 145.00
        ));

        clients.submit(new Buyer(
            exchange, "Buyer-Eric", "AAPL", 70, 149.00
        ));

        clients.submit(new Seller(
            exchange, "Seller-Fiona", "AAPL", 30, 148.00
        ));

        clients.shutdown();

        if (!clients.awaitTermination(30, TimeUnit.SECONDS)) {
            clients.shutdownNow();
        }

        // Demonstrate modifying an outstanding order.
        List<Order> remaining = exchange.getOrders();

        if (!remaining.isEmpty()) {
            Order order = remaining.get(0);
            exchange.modifyOrder(
                order.id, order.quantity, order.price
            );
        }

        exchange.printOrderBook();
        exchange.printHistory();
    }
}
