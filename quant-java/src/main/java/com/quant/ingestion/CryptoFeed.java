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

/**
 * Binance WebSocket feed for crypto tick data.
 * Subscribes to combined stream for multiple symbols.
 * Docs: https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams
 */
public class CryptoFeed extends BaseMarketFeed {

    private static final String WS_URL = "wss://stream.binance.com:9443/stream?streams=";

    private static final List<String> SYMBOLS = List.of(
        "btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt"
    );

    private final ObjectMapper mapper = new ObjectMapper();
    private WebSocketClient wsClient;

    public CryptoFeed(QuantKafkaProducer producer, OrderBookManager obm) {
        super(producer, obm);
    }

    @Override
    protected void connect() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        // Build combined stream URL — e.g. btcusdt@bookTicker/ethusdt@bookTicker
        String streams = String.join("/",
            SYMBOLS.stream().map(s -> s + "@bookTicker").toList());
        URI uri = new URI(WS_URL + streams);

        wsClient = new WebSocketClient(uri) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                log.info("[CryptoFeed] Connected to Binance WebSocket");
                latch.countDown();
            }

            @Override
            public void onMessage(String message) {
                handleMessage(message);
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                log.warn("[CryptoFeed] Connection closed: {} (code={})", reason, code);
            }

            @Override
            public void onError(Exception ex) {
                log.error("[CryptoFeed] WebSocket error: {}", ex.getMessage());
            }
        };

        wsClient.connectBlocking();
        latch.await();

        // Block until stopped
        while (running.get() && wsClient.isOpen()) {
            Thread.sleep(1000);
        }
    }

    private void handleMessage(String raw) {
        try {
            JsonNode root = mapper.readTree(raw);
            JsonNode data = root.path("data");
            if (data.isMissingNode()) return;

            // bookTicker: { s: symbol, b: bestBid, B: bidQty, a: bestAsk, A: askQty }
            String symbol = data.path("s").asText();
            double bid    = data.path("b").asDouble();
            double ask    = data.path("a").asDouble();
            double bidQty = data.path("B").asDouble();

            TickData tick = new TickData(
                symbol,
                TickData.Market.CRYPTO,
                bid, ask,
                (bid + ask) / 2.0,
                bidQty,
                "BINANCE"
            );
            onTick(tick);

        } catch (Exception e) {
            log.error("[CryptoFeed] Parse error: {}", e.getMessage());
        }
    }

    @Override
    public void stop() {
        super.stop();
        if (wsClient != null && wsClient.isOpen()) {
            wsClient.close();
        }
    }

    @Override
    protected String feedName() { return "CryptoFeed"; }
}
