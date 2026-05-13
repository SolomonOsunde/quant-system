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
 * Commodity feed backed by commodity-tracking ETFs via Alpaca WebSocket.
 *
 * ETF → Commodity mapping:
 *   GLD  → Gold       (replaces GC=F)
 *   USO  → Crude Oil  (replaces CL=F)
 *   SLV  → Silver     (replaces SI=F)
 *   UNG  → Natural Gas (replaces NG=F)
 *   WEAT → Wheat      (replaces ZW=F)
 *   SPY  → S&P 500    (replaces ES=F)
 *
 * Set ALPACA_API_KEY and ALPACA_API_SECRET to enable live mode.
 * Falls back to random-walk simulation when credentials are absent.
 */
public class CommodityFeed extends BaseMarketFeed {

    private static final String WS_URL =
        "wss://stream.data.alpaca.markets/v2/iex";

    private static final List<String> SYMBOLS = List.of(
        "GLD",   // Gold
        "USO",   // Crude Oil
        "SLV",   // Silver
        "UNG",   // Natural Gas
        "WEAT",  // Wheat
        "SPY"    // S&P 500
    );

    private static final double[] BASE_PRICES = {
        185.0,  // GLD  (~Gold)
        73.0,   // USO  (~Crude Oil)
        21.0,   // SLV  (~Silver)
        5.50,   // UNG  (~Natural Gas)
        5.80,   // WEAT (~Wheat)
        470.0   // SPY  (~S&P 500)
    };

    private final ObjectMapper mapper = new ObjectMapper();
    private WebSocketClient wsClient;

    public CommodityFeed(QuantKafkaProducer producer, OrderBookManager obm) {
        super(producer, obm);
    }

    @Override
    protected void connect() throws Exception {
        String apiKey    = System.getenv("ALPACA_API_KEY");
        String apiSecret = System.getenv("ALPACA_API_SECRET");

        if (apiKey == null || apiSecret == null) {
            log.warn("[CommodityFeed] Alpaca credentials not set. Running simulation mode.");
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
                        if ("authenticated".equals(msgType)) {
                            subscribeToQuotes();
                            authLatch.countDown();
                        } else if ("q".equals(msgType)) {
                            handleQuote(node);
                        }
                    }
                } catch (Exception e) {
                    log.error("[CommodityFeed] Message error: {}", e.getMessage());
                }
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                log.warn("[CommodityFeed] Closed: {}", reason);
            }

            @Override
            public void onError(Exception ex) {
                log.error("[CommodityFeed] Error: {}", ex.getMessage());
            }
        };

        wsClient.connectBlocking();
        boolean authed = authLatch.await(30, TimeUnit.SECONDS);
        if (!authed || !wsClient.isOpen()) {
            log.warn("[CommodityFeed] Alpaca auth timed out or connection closed (market may be closed). Falling back to simulation.");
            simulateFeed();
            return;
        }

        while (running.get() && wsClient.isOpen()) {
            Thread.sleep(1000);
        }
        log.warn("[CommodityFeed] Alpaca WebSocket disconnected. Falling back to simulation.");
        simulateFeed();
    }

    private void subscribeToQuotes() {
        String sub = String.format(
            "{\"action\":\"subscribe\",\"quotes\":[%s]}",
            "\"" + String.join("\",\"", SYMBOLS) + "\"");
        wsClient.send(sub);
        log.info("[CommodityFeed] Subscribed to quotes for {} ETF symbols.", SYMBOLS.size());
    }

    private void handleQuote(JsonNode node) {
        String symbol = node.path("S").asText();
        double bid    = node.path("bp").asDouble();
        double ask    = node.path("ap").asDouble();
        double bidSz  = node.path("bs").asDouble();

        TickData tick = new TickData(
            symbol, TickData.Market.COMMODITY,
            bid, ask, (bid + ask) / 2.0, bidSz, "ALPACA"
        );
        onTick(tick);
    }

    private void simulateFeed() throws InterruptedException {
        log.info("[CommodityFeed] Simulation mode active.");
        double[] prices = BASE_PRICES.clone();

        while (running.get()) {
            for (int i = 0; i < SYMBOLS.size(); i++) {
                // Realistic mean-reverting random walk
                prices[i] *= (1 + (Math.random() - 0.499) * 0.0008);
                double spread = prices[i] * 0.0002;

                TickData tick = new TickData(
                    SYMBOLS.get(i), TickData.Market.COMMODITY,
                    prices[i] - spread, prices[i] + spread,
                    prices[i], 10 + Math.random() * 100,
                    "SIMULATION"
                );
                onTick(tick);
            }
            Thread.sleep(500);
        }
    }

    @Override
    public void stop() {
        super.stop();
        if (wsClient != null && wsClient.isOpen()) wsClient.close();
    }

    @Override
    protected String feedName() { return "CommodityFeed"; }
}
