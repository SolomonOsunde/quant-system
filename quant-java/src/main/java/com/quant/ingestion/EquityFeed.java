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
 * Alpaca WebSocket feed for US equity tick data.
 * Docs: https://docs.alpaca.markets/reference/market-data-streaming
 *
 * Set ALPACA_API_KEY and ALPACA_API_SECRET in environment variables.
 */
public class EquityFeed extends BaseMarketFeed {

    private static final String WS_URL =
        "wss://stream.data.alpaca.markets/v2/iex";

    private static final List<String> SYMBOLS = List.of(
        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
        "TSLA", "META", "JPM", "GS", "SPY"
    );

    private final ObjectMapper mapper = new ObjectMapper();
    private WebSocketClient wsClient;

    public EquityFeed(QuantKafkaProducer producer, OrderBookManager obm) {
        super(producer, obm);
    }

    @Override
    protected void connect() throws Exception {
        String apiKey    = System.getenv("ALPACA_API_KEY");
        String apiSecret = System.getenv("ALPACA_API_SECRET");

        if (apiKey == null || apiSecret == null) {
            log.warn("[EquityFeed] Alpaca credentials not set. Running in simulation mode.");
            simulateFeed();
            return;
        }

        CountDownLatch authLatch = new CountDownLatch(1);

        wsClient = new WebSocketClient(new URI(WS_URL)) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                // Authenticate
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
                        if ("success".equals(msgType) && "authenticated".equals(node.path("msg").asText())) {
                            subscribeToQuotes();
                            authLatch.countDown();
                        } else if ("q".equals(msgType)) {
                            handleQuote(node);
                        }
                    }
                } catch (Exception e) {
                    log.error("[EquityFeed] Message error: {}", e.getMessage());
                }
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                log.warn("[EquityFeed] Closed: {}", reason);
            }

            @Override
            public void onError(Exception ex) {
                log.error("[EquityFeed] Error: {}", ex.getMessage());
            }
        };

        wsClient.connectBlocking();
        boolean authed = authLatch.await(30, TimeUnit.SECONDS);
        if (!authed || !wsClient.isOpen()) {
            log.warn("[EquityFeed] Alpaca auth timed out or connection closed (market may be closed). Falling back to simulation.");
            simulateFeed();
            return;
        }

        while (running.get() && wsClient.isOpen()) {
            Thread.sleep(1000);
        }
        log.warn("[EquityFeed] Alpaca WebSocket disconnected. Falling back to simulation.");
        simulateFeed();
    }

    private void subscribeToQuotes() {
        String sub = String.format(
            "{\"action\":\"subscribe\",\"quotes\":[%s]}",
            "\"" + String.join("\",\"", SYMBOLS) + "\"");
        wsClient.send(sub);
        log.info("[EquityFeed] Subscribed to quotes for {} symbols.", SYMBOLS.size());
    }

    private void handleQuote(JsonNode node) {
        String symbol = node.path("S").asText();
        double bid    = node.path("bp").asDouble();
        double ask    = node.path("ap").asDouble();
        double bidSz  = node.path("bs").asDouble();

        TickData tick = new TickData(
            symbol, TickData.Market.EQUITY,
            bid, ask, (bid + ask) / 2.0, bidSz, "ALPACA"
        );
        onTick(tick);
    }

    private void simulateFeed() throws InterruptedException {
        log.info("[EquityFeed] Simulation mode active.");
        double[] prices = {182.0, 375.0, 140.0, 178.0, 495.0,
                           240.0, 350.0, 198.0, 380.0, 470.0};
        int i = 0;
        while (running.get()) {
            for (String sym : SYMBOLS) {
                double mid    = prices[i % prices.length] * (1 + (Math.random() - 0.5) * 0.001);
                double spread = mid * 0.0001;
                TickData tick = new TickData(
                    sym, TickData.Market.EQUITY,
                    mid - spread, mid + spread, mid,
                    100 + Math.random() * 1000, "SIMULATION"
                );
                onTick(tick);
                i++;
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
    protected String feedName() { return "EquityFeed"; }
}
