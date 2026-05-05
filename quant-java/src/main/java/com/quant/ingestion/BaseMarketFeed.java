package com.quant.ingestion;

import com.quant.kafka.QuantKafkaProducer;
import com.quant.orderbook.OrderBookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for all market data feeds.
 * Subclasses implement connect() and the WebSocket message handler.
 */
public abstract class BaseMarketFeed implements Runnable {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected final QuantKafkaProducer kafkaProducer;
    protected final OrderBookManager orderBookManager;
    protected final AtomicBoolean running = new AtomicBoolean(false);

    // Reconnect config
    protected static final int MAX_RECONNECT_ATTEMPTS = 10;
    protected static final long RECONNECT_DELAY_MS    = 3_000;

    protected BaseMarketFeed(QuantKafkaProducer kafkaProducer,
                              OrderBookManager orderBookManager) {
        this.kafkaProducer    = kafkaProducer;
        this.orderBookManager = orderBookManager;
    }

    @Override
    public void run() {
        running.set(true);
        int attempts = 0;
        while (running.get() && attempts < MAX_RECONNECT_ATTEMPTS) {
            try {
                log.info("[{}] Connecting... (attempt {})", feedName(), attempts + 1);
                connect();
                attempts = 0; // reset on successful connection
            } catch (Exception e) {
                attempts++;
                log.warn("[{}] Connection error (attempt {}): {}", feedName(), attempts, e.getMessage());
                if (running.get() && attempts < MAX_RECONNECT_ATTEMPTS) {
                    try {
                        Thread.sleep(RECONNECT_DELAY_MS * attempts);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        if (attempts >= MAX_RECONNECT_ATTEMPTS) {
            log.error("[{}] Max reconnect attempts reached. Feed stopped.", feedName());
        }
    }

    /**
     * Called on each received tick — updates order book and publishes to Kafka.
     */
    protected void onTick(TickData tick) {
        try {
            orderBookManager.update(tick);
            kafkaProducer.publishTick(tick);
            log.debug("[{}] {}", feedName(), tick);
        } catch (Exception e) {
            log.error("[{}] Error processing tick: {}", feedName(), e.getMessage());
        }
    }

    public void stop() {
        running.set(false);
        log.info("[{}] Feed stopped.", feedName());
    }

    protected abstract void connect() throws Exception;
    protected abstract String feedName();
}
