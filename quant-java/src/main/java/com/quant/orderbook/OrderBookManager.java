package com.quant.orderbook;

import com.quant.ingestion.TickData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory order book manager.
 * Maintains best bid/ask and computes microstructure features
 * used by the Python signal engine.
 */
public class OrderBookManager {

    private static final Logger log = LoggerFactory.getLogger(OrderBookManager.class);

    private final Map<String, OrderBook> books = new ConcurrentHashMap<>();

    public void update(TickData tick) {
        books.computeIfAbsent(tick.symbol, OrderBook::new).update(tick);
    }

    public OrderBook getBook(String symbol) {
        return books.get(symbol);
    }

    public Map<String, OrderBook> getAllBooks() {
        return books;
    }

    /**
     * Single symbol order book — tracks bid, ask, spread, and VWAP.
     */
    public static class OrderBook {
        public final String symbol;
        public volatile double bestBid;
        public volatile double bestAsk;
        public volatile double lastPrice;
        public volatile double spread;
        public volatile double midPrice;
        public volatile double volume;
        public volatile long   lastUpdateMs;

        // Microstructure
        private double vwapNumerator;
        private double vwapDenominator;
        public volatile double vwap;

        // Momentum
        private double prevMid;
        public volatile double tickDirection; // +1 / -1 / 0

        public OrderBook(String symbol) {
            this.symbol = symbol;
        }

        public synchronized void update(TickData tick) {
            prevMid    = midPrice;
            bestBid    = tick.bid;
            bestAsk    = tick.ask;
            lastPrice  = tick.last;
            spread     = tick.ask - tick.bid;
            midPrice   = (tick.bid + tick.ask) / 2.0;
            volume     += tick.volume;
            lastUpdateMs = tick.timestamp;

            // VWAP
            if (tick.volume > 0) {
                vwapNumerator   += tick.last * tick.volume;
                vwapDenominator += tick.volume;
                vwap = vwapDenominator > 0 ? vwapNumerator / vwapDenominator : midPrice;
            }

            // Tick direction
            if (prevMid > 0) {
                tickDirection = midPrice > prevMid ? 1.0
                              : midPrice < prevMid ? -1.0 : 0.0;
            }
        }

        public double getSpreadBps() {
            return midPrice > 0 ? (spread / midPrice) * 10_000 : 0;
        }

        public boolean isStale(long maxAgeMs) {
            return (System.currentTimeMillis() - lastUpdateMs) > maxAgeMs;
        }

        @Override
        public String toString() {
            return String.format("OrderBook[%s bid=%.5f ask=%.5f spread=%.1fbps vwap=%.5f]",
                symbol, bestBid, bestAsk, getSpreadBps(), vwap);
        }
    }
}
