package com.quant;

import com.quant.ingestion.MarketDataManager;
import com.quant.kafka.QuantKafkaProducer;
import com.quant.orderbook.OrderBookManager;
import com.quant.risk.RiskEngine;
import com.quant.execution.ExecutionEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuantSystem {

    private static final Logger log = LoggerFactory.getLogger(QuantSystem.class);

    public static void main(String[] args) throws Exception {
        log.info("=== Polyglot Quant System — Java Core Starting ===");

        // Core components
        QuantKafkaProducer kafkaProducer = new QuantKafkaProducer();
        OrderBookManager orderBookManager = new OrderBookManager();
        RiskEngine riskEngine = new RiskEngine();
        ExecutionEngine executionEngine = new ExecutionEngine(riskEngine, kafkaProducer);
        MarketDataManager marketDataManager = new MarketDataManager(kafkaProducer, orderBookManager);

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down Quant System...");
            marketDataManager.shutdown();
            executionEngine.shutdown();
            kafkaProducer.close();
            log.info("Shutdown complete.");
        }));

        // Start all feeds
        marketDataManager.startAll();

        // Start execution engine (listens for Python signals via Kafka)
        executionEngine.start();

        log.info("All systems running. Kafka UI: http://localhost:8080");

        // Keep alive
        Thread.currentThread().join();
    }
}
