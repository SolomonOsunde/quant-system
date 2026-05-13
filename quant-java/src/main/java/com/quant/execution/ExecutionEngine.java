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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
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
 *
 * Live order routing uses the Alpaca paper trading REST API when the
 * environment variables ALPACA_API_KEY and ALPACA_API_SECRET are set.
 * If either variable is absent a UUID stub is used instead so the
 * system can run without credentials.
 */
public class ExecutionEngine {

    private static final Logger log = LoggerFactory.getLogger(ExecutionEngine.class);

    private static final String ALPACA_BASE_URL = "https://paper-api.alpaca.markets";
    private static final String ALPACA_ORDERS_PATH = "/v2/orders";

    /**
     * Commodity futures → Alpaca-tradeable ETF ticker mapping.
     * Symbols that are not in this map pass through unchanged
     * (equities and crypto tickers are used as-is).
     */
    private static final Map<String, String> SYMBOL_MAP = new HashMap<>();
    static {
        SYMBOL_MAP.put("CL=F",  "USO");
        SYMBOL_MAP.put("GC=F",  "GLD");
        SYMBOL_MAP.put("SI=F",  "SLV");
        SYMBOL_MAP.put("NG=F",  "UNG");
        SYMBOL_MAP.put("ZW=F",  "WEAT");
        SYMBOL_MAP.put("ES=F",  "SPY");
    }

    private final RiskEngine        riskEngine;
    private final QuantKafkaProducer kafkaProducer;
    private final ObjectMapper      mapper   = new ObjectMapper();
    private final AtomicBoolean     running  = new AtomicBoolean(false);
    private final ExecutorService   executor = Executors.newSingleThreadExecutor();

    /** Alpaca credentials — null when not configured. */
    private final String alpacaApiKey;
    private final String alpacaApiSecret;

    public ExecutionEngine(RiskEngine riskEngine, QuantKafkaProducer kafkaProducer) {
        this.riskEngine     = riskEngine;
        this.kafkaProducer  = kafkaProducer;
        this.alpacaApiKey    = System.getenv("ALPACA_API_KEY");
        this.alpacaApiSecret = System.getenv("ALPACA_API_SECRET");

        if (alpacaApiKey == null || alpacaApiKey.isEmpty()) {
            log.warn("ALPACA_API_KEY not set — execution engine will use stub order IDs");
        } else {
            log.info("Alpaca paper trading enabled (key={}...)", alpacaApiKey.substring(0, Math.min(4, alpacaApiKey.length())));
        }
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
            boolean simulation = signal.path("simulation").asBoolean(true);

            log.info("Signal received: {} {} {} @ {} | strategy={} confidence={} simulation={}",
                side, quantity, symbol, price, strategy, confidence, simulation);

            if (simulation) {
                log.info("[EXECUTION] Simulation signal — skipping live order for {}", symbol);
                return;
            }

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
     * Route order to the appropriate broker.
     * Uses Alpaca paper trading when credentials are available,
     * otherwise falls back to {@link #executeOrderStub}.
     */
    private String executeOrder(String symbol, String side, double qty,
                                double price, String orderType) {
        if (alpacaApiKey == null || alpacaApiKey.isEmpty()
                || alpacaApiSecret == null || alpacaApiSecret.isEmpty()) {
            log.warn("Alpaca credentials not configured — falling back to stub execution");
            return executeOrderStub(symbol, side, qty, price, orderType);
        }
        return executeOrderAlpaca(symbol, side, qty, price, orderType);
    }

    /**
     * Submit a market order to the Alpaca paper trading REST API.
     *
     * <p>The symbol is translated via {@link #SYMBOL_MAP} before submission
     * so that commodity futures tickers (e.g. {@code CL=F}) are mapped to the
     * corresponding ETF tickers that Alpaca accepts (e.g. {@code USO}).
     *
     * <p>On any HTTP error the response body is logged and a stub order ID is
     * returned so that the downstream pipeline remains unaffected.
     */
    private String executeOrderAlpaca(String symbol, String side, double qty,
                                      double price, String orderType) {
        String alpacaSymbol = SYMBOL_MAP.getOrDefault(symbol, symbol);
        String alpacaSide   = side.equalsIgnoreCase("BUY") ? "buy" : "sell";
        long   wholeQty     = Math.max(1L, Math.round(qty));

        // Build request body
        String body = String.format(
            "{\"symbol\":\"%s\",\"qty\":\"%d\",\"side\":\"%s\",\"type\":\"market\",\"time_in_force\":\"day\"}",
            alpacaSymbol, wholeQty, alpacaSide
        );

        log.info("[ALPACA] Submitting order: symbol={} (raw={}) side={} qty={} (raw={:.4f})",
            alpacaSymbol, symbol, alpacaSide, wholeQty, qty);

        HttpURLConnection conn = null;
        try {
            URL url = new URL(ALPACA_BASE_URL + ALPACA_ORDERS_PATH);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("APCA-API-KEY-ID", alpacaApiKey);
            conn.setRequestProperty("APCA-API-SECRET-KEY", alpacaApiSecret);
            conn.setDoOutput(true);
            conn.setConnectTimeout(5_000);
            conn.setReadTimeout(10_000);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }

            int statusCode = conn.getResponseCode();

            if (statusCode >= 200 && statusCode < 300) {
                // Success — parse the Alpaca order ID from the response
                String responseBody = readStream(conn.getInputStream());
                JsonNode response = mapper.readTree(responseBody);
                String alpacaOrderId = response.path("id").asText();
                log.info("[ALPACA] Order accepted: alpacaOrderId={} symbol={} side={} qty={}",
                    alpacaOrderId, alpacaSymbol, alpacaSide, wholeQty);
                return alpacaOrderId;
            } else {
                // HTTP error — log response body and fall back to stub
                String errorBody = readStream(conn.getErrorStream());
                log.error("[ALPACA] Order rejected: HTTP {} for {} {} {} — response: {}",
                    statusCode, alpacaSide, wholeQty, alpacaSymbol, errorBody);
                return executeOrderStub(symbol, side, qty, price, orderType);
            }

        } catch (IOException e) {
            log.error("[ALPACA] Network error submitting order for {} {} {}: {}",
                alpacaSide, wholeQty, alpacaSymbol, e.getMessage(), e);
            return executeOrderStub(symbol, side, qty, price, orderType);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    /**
     * Stub execution — generates a local UUID order ID.
     * Used when Alpaca credentials are absent or when the Alpaca call fails.
     */
    private String executeOrderStub(String symbol, String side, double qty,
                                    double price, String orderType) {
        String orderId = UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        log.info("[EXECUTION-STUB] {} {} {} qty={} price={} orderId={}",
            orderType, side, symbol, qty, price, orderId);
        return orderId;
    }

    /**
     * Drain an {@link InputStream} into a UTF-8 string.
     * Handles a null stream gracefully (returns empty string).
     */
    private String readStream(InputStream stream) throws IOException {
        if (stream == null) {
            return "";
        }
        return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }

    public void shutdown() {
        running.set(false);
        executor.shutdownNow();
        log.info("Execution engine stopped.");
    }
}
