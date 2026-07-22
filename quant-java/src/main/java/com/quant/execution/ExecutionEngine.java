package com.quant.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quant.SymbolMapper;
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
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consumes Python signals, risk-checks, routes to Alpaca paper, publishes fills.
 *
 * Book updates only happen after a confirmed broker accept/fill (or local stub).
 * On startup, open Alpaca positions are hydrated into {@link RiskEngine}.
 */
public class ExecutionEngine {

    private static final Logger log = LoggerFactory.getLogger(ExecutionEngine.class);

    private static final String ALPACA_BASE_URL = "https://paper-api.alpaca.markets";
    private static final String ALPACA_ORDERS_PATH = "/v2/orders";
    private static final String ALPACA_POSITIONS_PATH = "/v2/positions";
    private static final String ALPACA_ACCOUNT_PATH = "/v2/account";
    private static final double MIN_CRYPTO_NOTIONAL_USD = 1.0;
    private static final int FILL_POLL_ATTEMPTS = 20;  // ~5s at 250ms
    private static final long FILL_POLL_MS = 250;
    private static final long RECONCILE_INTERVAL_MS = 30_000;

    private static final Map<String, String> SYMBOL_MAP = new HashMap<>();
    static {
        SYMBOL_MAP.put("CL=F", "USO");
        SYMBOL_MAP.put("GC=F", "GLD");
        SYMBOL_MAP.put("SI=F", "SLV");
        SYMBOL_MAP.put("NG=F", "UNG");
        SYMBOL_MAP.put("ZW=F", "WEAT");
        SYMBOL_MAP.put("ES=F", "SPY");
    }

    /** Result of a broker submit — qty/price are what should hit the risk book. */
    public static final class OrderResult {
        public final String orderId;
        public final double qty;
        public final double price;
        public final String status;

        public OrderResult(String orderId, double qty, double price, String status) {
            this.orderId = orderId;
            this.qty = qty;
            this.price = price;
            this.status = status;
        }
    }

    private final RiskEngine riskEngine;
    private final QuantKafkaProducer kafkaProducer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ExecutorService executor = Executors.newFixedThreadPool(2);

    private final String alpacaApiKey;
    private final String alpacaApiSecret;
    private final boolean alpacaEnabled;
    private final OandaExecutionClient oandaClient;

    public ExecutionEngine(RiskEngine riskEngine, QuantKafkaProducer kafkaProducer) {
        this.riskEngine = riskEngine;
        this.kafkaProducer = kafkaProducer;
        this.alpacaApiKey = System.getenv("ALPACA_API_KEY");
        this.alpacaApiSecret = System.getenv("ALPACA_API_SECRET");
        this.alpacaEnabled = alpacaApiKey != null && !alpacaApiKey.isEmpty()
                && alpacaApiSecret != null && !alpacaApiSecret.isEmpty();
        this.oandaClient = new OandaExecutionClient();

        if (!alpacaEnabled) {
            log.warn("ALPACA credentials not set — crypto/equity broker disabled (fail-closed)");
        } else {
            log.info("Alpaca paper trading enabled (key={}...)",
                    alpacaApiKey.substring(0, Math.min(4, alpacaApiKey.length())));
        }
    }

    public void start() {
        if (alpacaEnabled) {
            hydrateAndPublishPositions();
        }
        running.set(true);
        executor.submit(this::consumeSignals);
        if (alpacaEnabled) {
            executor.submit(this::reconcileLoop);
        }
        log.info("Execution engine started — listening on {}", QuantKafkaProducer.TOPIC_SIGNALS);
    }

    private void reconcileLoop() {
        while (running.get()) {
            try {
                Thread.sleep(RECONCILE_INTERVAL_MS);
                if (running.get()) {
                    hydrateAndPublishPositions();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("Position reconcile error: {}", e.getMessage());
            }
        }
    }

    /**
     * Load open Alpaca positions into the risk book and publish snapshot for Python.
     */
    private void hydrateAndPublishPositions() {
        HttpURLConnection conn = null;
        try {
            URL url = new URL(ALPACA_BASE_URL + ALPACA_POSITIONS_PATH);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("APCA-API-KEY-ID", alpacaApiKey);
            conn.setRequestProperty("APCA-API-SECRET-KEY", alpacaApiSecret);
            conn.setConnectTimeout(5_000);
            conn.setReadTimeout(10_000);

            int status = conn.getResponseCode();
            if (status < 200 || status >= 300) {
                log.warn("[ALPACA] Position sync failed HTTP {} — {}", status,
                        readStream(conn.getErrorStream()));
                return;
            }

            JsonNode arr = mapper.readTree(readStream(conn.getInputStream()));
            Map<String, double[]> cryptoSnap = new HashMap<>();
            StringBuilder json = new StringBuilder("{\"positions\":[");
            int n = 0;
            if (arr.isArray()) {
                for (JsonNode pos : arr) {
                    String alpacaSym = pos.path("symbol").asText("");
                    double qty = parseDouble(pos.path("qty").asText("0"), 0);
                    double avg = parseDouble(pos.path("avg_entry_price").asText("0"), 0);
                    if (alpacaSym.isEmpty() || Math.abs(qty) < 1e-12) continue;
                    String internal = SymbolMapper.toInternal(alpacaSym);
                    SymbolMapper.register(internal, alpacaSym);
                    if (avg <= 0) {
                        avg = parseDouble(pos.path("current_price").asText("0"), 0);
                    }
                    if (internal.contains("/")) {
                        cryptoSnap.put(internal, new double[]{qty, avg});
                    } else {
                        riskEngine.hydratePosition(internal, qty, avg);
                    }
                    if (n > 0) json.append(",");
                    json.append(String.format(Locale.US,
                            "{\"symbol\":\"%s\",\"qty\":%.8f,\"avgPrice\":%.8f,\"side\":\"%s\"}",
                            internal, qty, avg, qty >= 0 ? "BUY" : "SELL"));
                    n++;
                }
            }
            double[] acct = fetchAccountEquity();
            double equity = acct[0];
            double buyingPower = acct[1];
            json.append("]");
            json.append(String.format(Locale.US,
                    ",\"equity\":%.2f,\"buyingPower\":%.2f,\"killSwitch\":%s,\"dailyPnl\":%.2f,\"ts\":%d}",
                    equity, buyingPower,
                    riskEngine.isKillSwitchActive() ? "true" : "false",
                    riskEngine.getDailyPnl(),
                    System.currentTimeMillis()));
            riskEngine.syncCryptoFromBroker(cryptoSnap);
            kafkaProducer.publishPositions(json.toString());
            log.info("[ALPACA] Synced {} position(s) equity=${} → risk + quant.positions",
                    n, String.format(Locale.US, "%.0f", equity));
        } catch (Exception e) {
            log.error("[ALPACA] Position sync error: {}", e.getMessage(), e);
        } finally {
            if (conn != null) conn.disconnect();
        }
    }

    /** Returns [equity, buyingPower]; zeros if account fetch fails. */
    private double[] fetchAccountEquity() {
        HttpURLConnection conn = null;
        try {
            URL url = new URL(ALPACA_BASE_URL + ALPACA_ACCOUNT_PATH);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("APCA-API-KEY-ID", alpacaApiKey);
            conn.setRequestProperty("APCA-API-SECRET-KEY", alpacaApiSecret);
            conn.setConnectTimeout(5_000);
            conn.setReadTimeout(10_000);
            int status = conn.getResponseCode();
            if (status < 200 || status >= 300) {
                log.warn("[ALPACA] Account fetch failed HTTP {}", status);
                return new double[]{0, 0};
            }
            JsonNode acct = mapper.readTree(readStream(conn.getInputStream()));
            double equity = parseDouble(acct.path("equity").asText("0"), 0);
            double bp = parseDouble(acct.path("buying_power").asText("0"), 0);
            return new double[]{equity, bp};
        } catch (Exception e) {
            log.warn("[ALPACA] Account fetch error: {}", e.getMessage());
            return new double[]{0, 0};
        } finally {
            if (conn != null) conn.disconnect();
        }
    }

    private void consumeSignals() {
        try (KafkaConsumer<String, String> consumer =
                     kafkaProducer.createSignalConsumer("quant-execution-group")) {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
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

            String symbol = signal.path("symbol").asText("").trim();
            String side = signal.path("side").asText("").trim().toUpperCase(Locale.ROOT);
            double quantity = signal.path("quantity").asDouble(0);
            double price = signal.path("price").asDouble(0);
            String orderType = signal.path("orderType").asText("MARKET");
            String strategy = signal.path("strategy").asText("UNKNOWN");
            double confidence = signal.path("confidence").asDouble(0.5);
            double spreadBps = signal.path("spreadBps").asDouble(0);
            boolean simulation = signal.path("simulation").asBoolean(true);
            boolean isExit = signal.path("isExit").asBoolean(false);

            if (symbol.isEmpty() || (!"BUY".equals(side) && !"SELL".equals(side))) {
                log.warn("Invalid signal — missing symbol/side");
                return;
            }
            if (!(quantity > 0) || !(price > 0)
                    || Double.isNaN(quantity) || Double.isNaN(price)
                    || Double.isInfinite(quantity) || Double.isInfinite(price)) {
                log.warn("Invalid signal — bad qty/price for {}: qty={} price={}", symbol, quantity, price);
                return;
            }

            log.info("Signal: {} {} {} @ {} | strategy={} conf={} sim={} exit={}",
                    side, quantity, symbol, price, strategy, confidence, simulation, isExit);

            if (simulation) {
                log.info("[EXECUTION] Simulation — skip broker for {}", symbol);
                return;
            }

            boolean isCrypto = symbol.contains("/") && !OandaExecutionClient.isForexSymbol(symbol);
            boolean isForex = OandaExecutionClient.isForexSymbol(symbol);

            if (isCrypto && "SELL".equals(side)) {
                double heldQty = riskEngine.getPositionQty(symbol);
                if (heldQty <= 1e-12) {
                    log.info("[EXECUTION] Skip crypto SELL {} — no long held", symbol);
                    publishFail(symbol, side, quantity, price, "skipped_flat", isExit);
                    return;
                }
                if (quantity > heldQty) {
                    log.info("[EXECUTION] Cap crypto SELL {} {} → {}", symbol, quantity, heldQty);
                    quantity = heldQty;
                }
            }

            if (isCrypto && quantity * price < MIN_CRYPTO_NOTIONAL_USD) {
                log.warn("Crypto notional ${} below minimum — rejected", quantity * price);
                publishFail(symbol, side, quantity, price, "rejected_min_notional", isExit);
                return;
            }

            RiskEngine.CheckResult result =
                    riskEngine.check(symbol, side, quantity, price, spreadBps);
            if (result != RiskEngine.CheckResult.APPROVED) {
                log.warn("Order rejected [{}]: {} {} {} @ {}", result, side, quantity, symbol, price);
                publishFail(symbol, side, quantity, price, "rejected_" + result.name().toLowerCase(Locale.ROOT), isExit);
                return;
            }

            OrderResult fill = executeOrder(symbol, side, quantity, price, orderType, isForex);
            if (fill == null || fill.orderId == null || fill.orderId.isEmpty()) {
                log.error("Broker order failed for {} — risk book NOT updated", symbol);
                publishFail(symbol, side, quantity, price, "broker_failed", isExit);
                return;
            }

            double prevQty = riskEngine.getPositionQty(symbol);
            double entryPx = signal.path("entryPrice").asDouble(riskEngine.getAvgPrice(symbol));
            if ("SELL".equals(side) && prevQty > 0 && entryPx > 0) {
                double closed = Math.min(fill.qty, prevQty);
                double pnl = signal.has("pnl")
                        ? signal.path("pnl").asDouble(closed * (fill.price - entryPx))
                        : closed * (fill.price - entryPx);
                riskEngine.updatePnl(pnl);
            } else if ("BUY".equals(side) && prevQty < 0 && entryPx > 0) {
                double closed = Math.min(fill.qty, Math.abs(prevQty));
                riskEngine.updatePnl(closed * (entryPx - fill.price));
            }

            riskEngine.updatePosition(symbol, side, fill.qty, fill.price);
            kafkaProducer.publishExecution(
                    symbol, side, fill.qty, fill.price, fill.orderId, fill.status, isExit, price);
            log.info("Filled: {} {} {} @ {} id={} status={}",
                    side, fill.qty, symbol, fill.price, fill.orderId, fill.status);

        } catch (Exception e) {
            log.error("Signal processing error: {}", e.getMessage(), e);
        }
    }

    private void publishFail(String symbol, String side, double qty, double price,
                             String status, boolean isExit) {
        kafkaProducer.publishExecution(symbol, side, 0, price, "", status, isExit, price);
    }

    private OrderResult executeOrder(String symbol, String side, double qty,
                                     double price, String orderType, boolean isForex) {
        if (isForex) {
            if (!oandaClient.isEnabled()) {
                log.error("OANDA not configured — refusing forex order for {}", symbol);
                return null;
            }
            return oandaClient.placeMarket(symbol, side, qty, price);
        }

        if (!alpacaEnabled) {
            log.error("Alpaca not configured — refusing order for {}", symbol);
            return null;
        }
        return executeOrderAlpaca(symbol, side, qty, price, orderType);
    }

    private OrderResult executeOrderAlpaca(String symbol, String side, double qty,
                                           double price, String orderType) {
        String alpacaSymbol = SymbolMapper.toAlpaca(
                SYMBOL_MAP.getOrDefault(symbol, symbol));
        if (alpacaSymbol.isEmpty()) {
            alpacaSymbol = SYMBOL_MAP.getOrDefault(symbol, symbol).replace("/", "");
        }
        SymbolMapper.register(symbol, alpacaSymbol);
        String alpacaSide = side.equalsIgnoreCase("BUY") ? "buy" : "sell";
        boolean isCrypto = symbol.contains("/") && !OandaExecutionClient.isForexSymbol(symbol);

        double submitQty = qty;
        String qtyField;
        if (isCrypto) {
            qtyField = String.format(Locale.US, "%.8f", qty);
        } else {
            long wholeQty = Math.max(1L, Math.round(qty));
            qtyField = Long.toString(wholeQty);
            submitQty = wholeQty;
        }

        String tif = isCrypto ? "gtc" : "day";
        String clientOrderId = "q-" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        String body = String.format(Locale.US,
                "{\"symbol\":\"%s\",\"qty\":\"%s\",\"side\":\"%s\",\"type\":\"market\","
                        + "\"time_in_force\":\"%s\",\"client_order_id\":\"%s\"}",
                alpacaSymbol, qtyField, alpacaSide, tif, clientOrderId);

        log.info("[ALPACA] Submit: {} {} qty={} (signal={})", alpacaSide, alpacaSymbol, qtyField, qty);

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
            if (statusCode < 200 || statusCode >= 300) {
                log.error("[ALPACA] Rejected HTTP {} — {}", statusCode, readStream(conn.getErrorStream()));
                return null;
            }

            JsonNode response = mapper.readTree(readStream(conn.getInputStream()));
            String orderId = response.path("id").asText("");
            if (orderId.isEmpty()) {
                log.error("[ALPACA] Accept without order id — treating as failure");
                return null;
            }

            OrderResult polled = pollUntilFilled(orderId, submitQty, price);
            if (polled != null) {
                return polled;
            }
            // Timeout: cancel residual, book any partial that did fill
            log.warn("[ALPACA] Order {} not filled in window — canceling", orderId);
            cancelOrder(orderId);
            JsonNode after = getOrder(orderId);
            if (after != null) {
                double filledQty = parseDouble(after.path("filled_qty").asText("0"), 0);
                double avgPrice = parseDouble(after.path("filled_avg_price").asText("0"), 0);
                String st = after.path("status").asText("canceled");
                if (filledQty > 0 && avgPrice > 0) {
                    log.info("[ALPACA] Booking partial after cancel: qty={} @ {}", filledQty, avgPrice);
                    return new OrderResult(orderId, filledQty, avgPrice, "partial");
                }
                log.error("[ALPACA] Order {} ended status={} with no fill — NOT booking", orderId, st);
            }
            return null;
        } catch (IOException e) {
            log.error("[ALPACA] Network error: {}", e.getMessage(), e);
            return null;
        } finally {
            if (conn != null) conn.disconnect();
        }
    }

    private OrderResult pollUntilFilled(String orderId, double fallbackQty, double fallbackPrice) {
        for (int i = 0; i < FILL_POLL_ATTEMPTS; i++) {
            try {
                Thread.sleep(FILL_POLL_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return null;
            }
            JsonNode order = getOrder(orderId);
            if (order == null) continue;

            String status = order.path("status").asText("");
            double filledQty = parseDouble(order.path("filled_qty").asText("0"), 0);
            double avgPrice = parseDouble(order.path("filled_avg_price").asText("0"), 0);

            if ("filled".equalsIgnoreCase(status) && filledQty > 0) {
                double px = avgPrice > 0 ? avgPrice : fallbackPrice;
                log.info("[ALPACA] Filled {} qty={} @ {}", orderId, filledQty, px);
                return new OrderResult(orderId, filledQty, px, "filled");
            }
            if ("canceled".equalsIgnoreCase(status) || "rejected".equalsIgnoreCase(status)
                    || "expired".equalsIgnoreCase(status)) {
                if (filledQty > 0 && avgPrice > 0) {
                    return new OrderResult(orderId, filledQty, avgPrice, "partial");
                }
                log.error("[ALPACA] Order {} terminal status={}", orderId, status);
                return null;
            }
            if (filledQty > 0 && avgPrice > 0 && filledQty >= fallbackQty * 0.99) {
                return new OrderResult(orderId, filledQty, avgPrice, "filled");
            }
        }
        return null;
    }

    private void cancelOrder(String orderId) {
        HttpURLConnection conn = null;
        try {
            URL url = new URL(ALPACA_BASE_URL + ALPACA_ORDERS_PATH + "/" + orderId);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("DELETE");
            conn.setRequestProperty("APCA-API-KEY-ID", alpacaApiKey);
            conn.setRequestProperty("APCA-API-SECRET-KEY", alpacaApiSecret);
            conn.setConnectTimeout(3_000);
            conn.setReadTimeout(5_000);
            int status = conn.getResponseCode();
            log.info("[ALPACA] Cancel {} → HTTP {}", orderId, status);
        } catch (Exception e) {
            log.warn("[ALPACA] Cancel {} failed: {}", orderId, e.getMessage());
        } finally {
            if (conn != null) conn.disconnect();
        }
    }

    private JsonNode getOrder(String orderId) {
        HttpURLConnection conn = null;
        try {
            URL url = new URL(ALPACA_BASE_URL + ALPACA_ORDERS_PATH + "/" + orderId);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("APCA-API-KEY-ID", alpacaApiKey);
            conn.setRequestProperty("APCA-API-SECRET-KEY", alpacaApiSecret);
            conn.setConnectTimeout(3_000);
            conn.setReadTimeout(5_000);
            int status = conn.getResponseCode();
            if (status < 200 || status >= 300) return null;
            return mapper.readTree(readStream(conn.getInputStream()));
        } catch (Exception e) {
            log.debug("[ALPACA] getOrder {}: {}", orderId, e.getMessage());
            return null;
        } finally {
            if (conn != null) conn.disconnect();
        }
    }

    private static double parseDouble(String s, double def) {
        try {
            if (s == null || s.isEmpty() || "null".equalsIgnoreCase(s)) return def;
            return Double.parseDouble(s);
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private String readStream(InputStream stream) throws IOException {
        if (stream == null) return "";
        return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }

    public void shutdown() {
        running.set(false);
        executor.shutdownNow();
        log.info("Execution engine stopped.");
    }
}
