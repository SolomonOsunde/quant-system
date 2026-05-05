package com.quant.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quant.kafka.QuantKafkaProducer;
import com.quant.risk.RiskEngine;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Execution engine — consumes trade signals from Python via Kafka,
 * runs pre-trade risk checks, and routes orders to brokers.
 *
 * Signal format (JSON):
 * {
 *   "symbol":     "BTCUSDT",
 *   "side":       "BUY",
 *   "quantity":   0.01,
 *   "price":      42000.0,
 *   "orderType":  "LIMIT",
 *   "strategy":   "ML_ENSEMBLE",
 *   "confidence": 0.82,
 *   "spreadBps":  1.2
 * }
 */
public class ExecutionEngine {

    private static final Logger log = LoggerFactory.getLogger(ExecutionEngine.class);

    private final RiskEngine       riskEngine;
    private final QuantKafkaProducer kafkaProducer;
    private final ObjectMapper     mapper   = new ObjectMapper();
    private final AtomicBoolean    running  = new AtomicBoolean(false);
    private final ExecutorService  executor = Executors.newSingleThreadExecutor();

    public ExecutionEngine(RiskEngine riskEngine, QuantKafkaProducer kafkaProducer) {
        this.riskEngine    = riskEngine;
        this.kafkaProducer = kafkaProducer;
    }

    public void start() {
        running.set(true);
        executor.submit(this::consumeSignals);
        log.info("Execution engine started — listening for Python signals on {}",
            QuantKafkaProducer.TOPIC_SIGNALS);
    }

    private void consumeSignals() {
        try (KafkaConsumer<String, String> consumer =
                 kafkaProducer.createSignalConsumer("quant-execution-group")) {

            while (running.get()) {
                ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    processSignal(record.value());
                }
            }
        } catch (Exception e) {
            log.error("Signal consumer error: {}", e.getMessage(), e);
        }
    }

    private void processSignal(String signalJson) {
        try {
            JsonNode signal = mapper.readTree(signalJson);

            String symbol     = signal.path("symbol").asText();
            String side       = signal.path("side").asText();
            double quantity   = signal.path("quantity").asDouble();
            double price      = signal.path("price").asDouble();
            String orderType  = signal.path("orderType").asText("MARKET");
            String strategy   = signal.path("strategy").asText("UNKNOWN");
            double confidence = signal.path("confidence").asDouble(0.5);
            double spreadBps  = signal.path("spreadBps").asDouble(0);

            log.info("Signal received: {} {} {} @ {} | strategy={} confidence={}",
                side, quantity, symbol, price, strategy, confidence);

            // Pre-trade risk check
            RiskEngine.CheckResult result =
                riskEngine.check(symbol, side, quantity, price, spreadBps);

            if (result == RiskEngine.CheckResult.APPROVED) {
                String orderId = executeOrder(symbol, side, quantity, price, orderType);
                riskEngine.updatePosition(symbol, side, quantity, price);
                kafkaProducer.publishExecution(symbol, side, quantity, price, orderId);
                log.info("Order executed: {} orderId={}", symbol, orderId);
            } else {
                log.warn("Order rejected [{}]: {} {} {} @ {}", result, side, quantity, symbol, price);
            }

        } catch (Exception e) {
            log.error("Signal processing error: {}", e.getMessage());
        }
    }

    /**
     * Route order to broker. Stub — replace with real broker API calls.
     * Options: Alpaca REST API, OANDA v20, Binance API, IB TWS API.
     */
    private String executeOrder(String symbol, String side, double qty,
                                 double price, String orderType) {
        String orderId = UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        // TODO: Replace with real broker API call per market
        // Example for Alpaca:
        //   AlpacaAPI.submitOrder(symbol, qty, side, orderType, "day");
        // Example for Binance:
        //   BinanceAPI.newOrder(symbol, side, orderType, qty);
        log.info("[EXECUTION] {} {} {} qty={} price={} orderId={}",
            orderType, side, symbol, qty, price, orderId);
        return orderId;
    }

    public void shutdown() {
        running.set(false);
        executor.shutdownNow();
        log.info("Execution engine stopped.");
    }
}
