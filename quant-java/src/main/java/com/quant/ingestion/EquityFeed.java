package com.quant.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quant.config.InstrumentRepository;
import com.quant.kafka.QuantKafkaProducer;
import com.quant.orderbook.OrderBookManager;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Alpaca equity feed — symbols from DB {@code instruments} (market=EQUITY).
 * Live-only; no simulation fallback.
 */
public class EquityFeed extends BaseMarketFeed {

    private static final String WS_URL = "wss://stream.data.alpaca.markets/v2/iex";

    private final ObjectMapper mapper = new ObjectMapper();
    private WebSocketClient wsClient;
    private List<String> symbols = List.of();

    public EquityFeed(QuantKafkaProducer producer, OrderBookManager obm) {
        super(producer, obm);
    }

    @Override
    protected void connect() throws Exception {
        String apiKey    = System.getenv("ALPACA_API_KEY");
        String apiSecret = System.getenv("ALPACA_API_SECRET");
        if (apiKey == null || apiKey.isEmpty() || apiSecret == null || apiSecret.isEmpty()) {
            throw new IllegalStateException(
                    "[EquityFeed] ALPACA credentials required — simulation removed");
        }

        symbols = InstrumentRepository.symbols("EQUITY");
        if (symbols.isEmpty()) {
            throw new IllegalStateException(
                    "[EquityFeed] No enabled EQUITY instruments in DB");
        }

        CountDownLatch authLatch = new CountDownLatch(1);
        wsClient = new WebSocketClient(new URI(WS_URL)) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                send(String.format(
                        "{\"action\":\"auth\",\"key\":\"%s\",\"secret\":\"%s\"}",
                        apiKey, apiSecret));
            }

            @Override
            public void onMessage(String message) {
                try {
                    JsonNode arr = mapper.readTree(message);
                    for (JsonNode node : arr) {
                        String msgType = node.path("T").asText();
                        if ("success".equals(msgType)
                                && "authenticated".equals(node.path("msg").asText())) {
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

            @Override public void onClose(int code, String reason, boolean remote) {
                log.warn("[EquityFeed] Closed: {}", reason);
                authLatch.countDown();
            }

            @Override public void onError(Exception ex) {
                log.error("[EquityFeed] Error: {}", ex.getMessage());
            }
        };

        wsClient.connectBlocking();
        boolean authed = authLatch.await(30, TimeUnit.SECONDS);
        if (!authed || !wsClient.isOpen()) {
            throw new Exception("[EquityFeed] Alpaca auth failed (market may be closed) — no simulation");
        }

        log.info("[EquityFeed] Live — {} symbols from DB", symbols.size());
        while (running.get() && wsClient.isOpen()) {
            Thread.sleep(1000);
        }
        throw new Exception("[EquityFeed] WebSocket disconnected — will retry");
    }

    private void subscribeToQuotes() {
        String sub = String.format(
            "{\"action\":\"subscribe\",\"quotes\":[%s]}",
            "\"" + String.join("\",\"", symbols) + "\"");
        wsClient.send(sub);
        log.info("[EquityFeed] Subscribed to {} symbols.", symbols.size());
    }

    private void handleQuote(JsonNode node) {
        String symbol = node.path("S").asText();
        double bid    = node.path("bp").asDouble();
        double ask    = node.path("ap").asDouble();
        double bidSz  = node.path("bs").asDouble();
        if (bid <= 0 || ask <= 0) return;
        onTick(new TickData(symbol, TickData.Market.EQUITY,
                bid, ask, (bid + ask) / 2.0, bidSz, "ALPACA"));
    }

    @Override
    public void stop() {
        super.stop();
        if (wsClient != null && wsClient.isOpen()) wsClient.close();
    }

    @Override
    protected String feedName() { return "EquityFeed"; }
}
