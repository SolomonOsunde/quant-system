package com.quant.ingestion;

import com.quant.kafka.QuantKafkaProducer;
import com.quant.orderbook.OrderBookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Manages all market data feed connections across asset classes.
 * Each feed runs on its own thread and publishes ticks to Kafka.
 */
public class MarketDataManager {

    private static final Logger log = LoggerFactory.getLogger(MarketDataManager.class);

    private final QuantKafkaProducer kafkaProducer;
    private final OrderBookManager orderBookManager;
    private final ExecutorService executor;
    private final List<BaseMarketFeed> feeds = new ArrayList<>();

    public MarketDataManager(QuantKafkaProducer kafkaProducer,
                             OrderBookManager orderBookManager) {
        this.kafkaProducer    = kafkaProducer;
        this.orderBookManager = orderBookManager;
        this.executor         = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
    }

    public void startAll() {
        log.info("Starting market data feeds...");

        // Crypto-only mode — Alpaca crypto WebSocket (24/7, no market hours restriction)
        CryptoFeed cryptoFeed = new CryptoFeed(kafkaProducer, orderBookManager);
        feeds.add(cryptoFeed);
        executor.submit(cryptoFeed);

        log.info("Started {} market data feed(s).", feeds.size());
    }

    public void shutdown() {
        log.info("Stopping {} feeds...", feeds.size());
        feeds.forEach(BaseMarketFeed::stop);
        executor.shutdownNow();
    }
}
