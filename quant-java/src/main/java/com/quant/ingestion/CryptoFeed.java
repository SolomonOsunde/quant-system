package com.quant.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quant.kafka.QuantKafkaProducer;
import com.quant.orderbook.OrderBookManager;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Alpaca crypto WebSocket feed for real-time crypto quote data.
 * Uses the Alpaca crypto/us stream (24/7, no market hours restriction).
 * Falls back to simulation when credentials are absent.
 */
public class CryptoFeed extends BaseMarketFeed {

    private static final String WS_URL = "wss://stream.data.alpaca.markets/v1beta3/crypto/us";

    private static final List<String> SYMBOLS = List.of(
        "BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD",
        "AVAX/USD", "DOGE/USD", "LINK/USD", "LTC/USD"
    );

    private static final double[] BASE_PRICES = {
        79000.0, 2265.0, 91.0, 1.43,
        35.0, 0.18, 14.0, 85.0
    };

    private final ObjectMapper mapper = new ObjectMapper();
    private WebSocketClient wsClient;

    public CryptoFeed(QuantKafkaProducer producer, OrderBookManager obm) {
        super(producer, obm);
    }

    @Override
    protected void connect() throws Exception {
        String apiKey    = System.getenv("ALPACA_API_KEY");
        String apiSecret = System.getenv("ALPACA_API_SECRET");

        if (apiKey == null || apiKey.isEmpty() || apiSecret == null || apiSecret.isEmpty()) {
            log.warn("[CryptoFeed] Alpaca credentials not set. Running in simulation mode.");
            simulateFeed();
            return;
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
                                subscribeToQuotes();
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
            log.warn("[CryptoFeed] Auth failed or connection closed. Falling back to simulation.");
            simulateFeed();
            return;
        }

        log.info("[CryptoFeed] Connected to Alpaca crypto stream.");
        while (running.get() && wsClient.isOpen()) {
            Thread.sleep(1000);
        }
        log.warn("[CryptoFeed] Disconnected. Falling back to simulation.");
        simulateFeed();
    }

    private void subscribeToQuotes() {
        String syms = "\"" + String.join("\",\"", SYMBOLS) + "\"";
        // Subscribe to both quotes and trades for higher tick frequency
        wsClient.send("{\"action\":\"subscribe\",\"quotes\":[" + syms + "],\"trades\":[" + syms + "]}");
        log.info("[CryptoFeed] Subscribed to Alpaca crypto quotes+trades: {}", SYMBOLS);
    }

    private void handleQuote(JsonNode node) {
        String symbol = node.path("S").asText();
        double bid    = node.path("bp").asDouble();
        double ask    = node.path("ap").asDouble();
        double bidSz  = node.path("bs").asDouble();

        if (bid <= 0 || ask <= 0) return;

        TickData tick = new TickData(
            symbol, TickData.Market.CRYPTO,
            bid, ask, (bid + ask) / 2.0, bidSz, "ALPACA"
        );
        onTick(tick);
    }

    private void handleTrade(JsonNode node) {
        String symbol = node.path("S").asText();
        double price  = node.path("p").asDouble();
        double size   = node.path("s").asDouble();

        if (price <= 0) return;

        // Use trade price as both bid and ask (no spread for trade ticks)
        TickData tick = new TickData(
            symbol, TickData.Market.CRYPTO,
            price, price, price, size, "ALPACA"
        );
        onTick(tick);
    }

    private void simulateFeed() throws InterruptedException {
        log.info("[CryptoFeed] Simulation mode active.");
        double[] prices = BASE_PRICES.clone();
        while (running.get()) {
            for (int i = 0; i < SYMBOLS.size(); i++) {
                prices[i] *= (1 + (Math.random() - 0.499) * 0.0005);
                double spread = prices[i] * 0.0002;
                TickData tick = new TickData(
                    SYMBOLS.get(i), TickData.Market.CRYPTO,
                    prices[i] - spread, prices[i] + spread,
                    prices[i], 0.1 + Math.random() * 5, "SIMULATION"
                );
                onTick(tick);
            }
            Thread.sleep(200);
        }
    }

    @Override
    public void stop() {
        super.stop();
        if (wsClient != null && wsClient.isOpen()) wsClient.close();
    }

    @Override
    protected String feedName() { return "CryptoFeed"; }
}
