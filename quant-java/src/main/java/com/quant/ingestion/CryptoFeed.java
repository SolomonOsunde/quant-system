package com.quant.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quant.SymbolMapper;
import com.quant.config.InstrumentRepository;
import com.quant.kafka.QuantKafkaProducer;
import com.quant.orderbook.OrderBookManager;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Alpaca crypto WebSocket feed — live only.
 * Universe = enabled CRYPTO rows in Postgres {@code instruments},
 * intersected with Alpaca tradable assets (cap {@link #MAX_SYMBOLS}).
 */
public class CryptoFeed extends BaseMarketFeed {

    private static final String WS_URL     = "wss://stream.data.alpaca.markets/v1beta3/crypto/us";
    private static final String ASSETS_URL = "https://paper-api.alpaca.markets/v2/assets?asset_class=crypto&status=active";
    private static final int MAX_SYMBOLS = 15;

    private static final java.util.Set<String> STABLECOINS = java.util.Set.of(
        "USDT/USD", "USDC/USD", "USDG/USD", "BUSD/USD", "DAI/USD", "TUSD/USD",
        "USDT/USDC", "USDC/USDT"
    );

    private final ObjectMapper mapper = new ObjectMapper();
    private WebSocketClient    wsClient;
    private List<String>       symbols;
    private final Map<String, double[]> lastQuote = new ConcurrentHashMap<>();

    public CryptoFeed(QuantKafkaProducer producer, OrderBookManager obm) {
        super(producer, obm);
    }

    @Override
    protected void connect() throws Exception {
        String apiKey    = System.getenv("ALPACA_API_KEY");
        String apiSecret = System.getenv("ALPACA_API_SECRET");

        if (apiKey == null || apiKey.isEmpty() || apiSecret == null || apiSecret.isEmpty()) {
            throw new IllegalStateException(
                    "[CryptoFeed] ALPACA_API_KEY / ALPACA_API_SECRET required — simulation removed");
        }

        List<String> dbUniverse = InstrumentRepository.symbols("CRYPTO");
        if (dbUniverse.isEmpty()) {
            throw new IllegalStateException(
                    "[CryptoFeed] No enabled CRYPTO rows in instruments table — seed DB first");
        }

        symbols = resolveUniverse(apiKey, apiSecret, dbUniverse);
        if (symbols.isEmpty()) {
            throw new IllegalStateException(
                    "[CryptoFeed] DB crypto symbols not tradable on Alpaca — check instruments table");
        }
        log.info("[CryptoFeed] Trading universe: {} symbols → {}", symbols.size(), symbols);
        for (String sym : symbols) {
            SymbolMapper.registerSlash(sym);
        }

        CountDownLatch authLatch = new CountDownLatch(1);

        wsClient = new WebSocketClient(new URI(WS_URL)) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                String auth = String.format(
                    "{\"action\":\"auth\",\"key\":\"%s\",\"secret\":\"%s\"}",
                    apiKey, apiSecret);
                send(auth);
            }

            @Override
            public void onMessage(String message) {
                try {
                    JsonNode arr = mapper.readTree(message);
                    for (JsonNode node : arr) {
                        String msgType = node.path("T").asText();
                        if ("success".equals(msgType)) {
                            String msg = node.path("msg").asText();
                            log.info("[CryptoFeed] Alpaca: {}", msg);
                            if ("authenticated".equals(msg)) {
                                subscribeToUniverse();
                                authLatch.countDown();
                            }
                        } else if ("error".equals(msgType)) {
                            log.error("[CryptoFeed] Alpaca error: code={} msg={}",
                                node.path("code").asInt(), node.path("msg").asText());
                            authLatch.countDown();
                        } else if ("q".equals(msgType)) {
                            handleQuote(node);
                        } else if ("t".equals(msgType)) {
                            handleTrade(node);
                        }
                    }
                } catch (Exception e) {
                    log.error("[CryptoFeed] Message error: {}", e.getMessage());
                }
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                log.warn("[CryptoFeed] Closed: {} (code={})", reason, code);
                authLatch.countDown();
            }

            @Override
            public void onError(Exception ex) {
                log.error("[CryptoFeed] Error: {}", ex.getMessage());
            }
        };

        wsClient.connectBlocking();
        boolean authed = authLatch.await(30, TimeUnit.SECONDS);
        if (!authed || !wsClient.isOpen()) {
            throw new Exception("Alpaca auth failed or connection closed before authentication");
        }

        log.info("[CryptoFeed] Connected to Alpaca crypto stream ({} symbols).", symbols.size());
        while (running.get() && wsClient.isOpen()) {
            Thread.sleep(1_000);
        }
        throw new Exception("Alpaca WebSocket disconnected — will retry");
    }

    /**
     * Prefer DB order; keep only symbols Alpaca reports as tradable; cap at MAX_SYMBOLS.
     */
    private List<String> resolveUniverse(String apiKey, String apiSecret, List<String> dbUniverse)
            throws Exception {
        LinkedHashSet<String> available = fetchAlpacaAvailable(apiKey, apiSecret);
        if (available.isEmpty()) {
            throw new IllegalStateException("[CryptoFeed] Alpaca assets API returned no crypto symbols");
        }
        List<String> resolved = new ArrayList<>();
        for (String sym : dbUniverse) {
            if (STABLECOINS.contains(sym)) continue;
            if (!sym.endsWith("/USD")) continue;
            if (available.contains(sym)) {
                resolved.add(sym);
            } else {
                log.warn("[CryptoFeed] DB symbol {} not tradable on Alpaca — skipped", sym);
            }
            if (resolved.size() >= MAX_SYMBOLS) break;
        }
        return resolved;
    }

    private LinkedHashSet<String> fetchAlpacaAvailable(String apiKey, String apiSecret)
            throws Exception {
        LinkedHashSet<String> available = new LinkedHashSet<>();
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) URI.create(ASSETS_URL).toURL().openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("APCA-API-KEY-ID", apiKey);
            conn.setRequestProperty("APCA-API-SECRET-KEY", apiSecret);
            conn.setConnectTimeout(10_000);
            conn.setReadTimeout(10_000);
            int status = conn.getResponseCode();
            if (status != 200) {
                throw new IllegalStateException("Alpaca assets HTTP " + status);
            }
            JsonNode assets = mapper.readTree(readStream(conn.getInputStream()));
            for (JsonNode asset : assets) {
                if (!asset.path("tradable").asBoolean(false)) continue;
                if (!"active".equals(asset.path("status").asText())) continue;
                String sym = toSlashFormat(asset.path("symbol").asText());
                if (sym != null && !STABLECOINS.contains(sym) && sym.endsWith("/USD")) {
                    available.add(sym);
                }
            }
        } finally {
            if (conn != null) conn.disconnect();
        }
        return available;
    }

    private static String toSlashFormat(String raw) {
        if (raw == null || raw.isEmpty()) return null;
        raw = raw.toUpperCase();
        if (raw.contains("/")) return raw;
        for (String quote : new String[]{"USDT", "USDC", "USD", "BTC", "ETH"}) {
            if (raw.endsWith(quote) && raw.length() > quote.length()) {
                return raw.substring(0, raw.length() - quote.length()) + "/" + quote;
            }
        }
        return null;
    }

    private void subscribeToUniverse() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < symbols.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("\"").append(symbols.get(i)).append("\"");
        }
        wsClient.send("{\"action\":\"subscribe\",\"quotes\":[" + sb + "],\"trades\":[" + sb + "]}");
        log.info("[CryptoFeed] Subscribed to {} symbols.", symbols.size());
    }

    private void handleQuote(JsonNode node) {
        String symbol = node.path("S").asText();
        double bid    = node.path("bp").asDouble();
        double ask    = node.path("ap").asDouble();
        double bidSz  = node.path("bs").asDouble();
        if (bid <= 0 || ask <= 0 || ask < bid) return;
        lastQuote.put(symbol, new double[]{bid, ask});
        onTick(new TickData(symbol, TickData.Market.CRYPTO,
                            bid, ask, (bid + ask) / 2.0, bidSz, "ALPACA"));
    }

    private void handleTrade(JsonNode node) {
        String symbol = node.path("S").asText();
        double price  = node.path("p").asDouble();
        double size   = node.path("s").asDouble();
        if (price <= 0) return;
        double[] q = lastQuote.get(symbol);
        double bid = q != null ? q[0] : price;
        double ask = q != null ? q[1] : price;
        if (q == null) {
            double half = price * 0.00005;
            bid = price - half;
            ask = price + half;
        }
        onTick(new TickData(symbol, TickData.Market.CRYPTO,
                            bid, ask, price, size, "ALPACA"));
    }

    private String readStream(InputStream stream) throws Exception {
        if (stream == null) return "";
        return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }

    @Override
    public void stop() {
        super.stop();
        if (wsClient != null && wsClient.isOpen()) wsClient.close();
    }

    @Override
    protected String feedName() { return "CryptoFeed"; }
}
