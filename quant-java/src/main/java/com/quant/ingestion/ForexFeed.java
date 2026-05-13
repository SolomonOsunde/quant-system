package com.quant.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quant.kafka.QuantKafkaProducer;
import com.quant.orderbook.OrderBookManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

/**
 * OANDA v20 streaming API feed for Forex tick data.
 * Uses HTTP chunked streaming (not WebSocket).
 * Docs: https://developer.oanda.com/rest-live-v20/pricing-ep/
 *
 * Set OANDA_API_KEY and OANDA_ACCOUNT_ID in application.properties.
 */
public class ForexFeed extends BaseMarketFeed {

    private static final String BASE_URL =
        "https://stream-fxtrade.oanda.com/v3/accounts/%s/pricing/stream?instruments=%s";

    private static final List<String> PAIRS = List.of(
        "EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD",
        "USD_CHF", "EUR_GBP", "EUR_JPY"
    );

    private final ObjectMapper mapper = new ObjectMapper();
    private volatile HttpURLConnection conn;

    public ForexFeed(QuantKafkaProducer producer, OrderBookManager obm) {
        super(producer, obm);
    }

    @Override
    protected void connect() throws Exception {
        String apiKey    = System.getenv("OANDA_API_KEY");
        String accountId = System.getenv("OANDA_ACCOUNT_ID");

        if (apiKey == null || apiKey.isEmpty() || accountId == null || accountId.isEmpty()) {
            log.warn("[ForexFeed] OANDA_API_KEY or OANDA_ACCOUNT_ID not set. " +
                     "Running in simulation mode.");
            simulateFeed();
            return;
        }

        String instruments = String.join("%2C", PAIRS);
        String urlStr = String.format(BASE_URL, accountId, instruments);

        conn = (HttpURLConnection) new URL(urlStr).openConnection();
        conn.setRequestProperty("Authorization", "Bearer " + apiKey);
        conn.setRequestProperty("Accept-Encoding", "identity");
        conn.connect();

        log.info("[ForexFeed] Connected to OANDA streaming.");

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(conn.getInputStream()))) {
            String line;
            while (running.get() && (line = reader.readLine()) != null) {
                if (!line.isBlank()) handleMessage(line);
            }
        }
    }

    private void handleMessage(String raw) {
        try {
            JsonNode node = mapper.readTree(raw);
            if (!"PRICE".equals(node.path("type").asText())) return;

            String symbol = node.path("instrument").asText();
            double bid    = node.path("bids").get(0).path("price").asDouble();
            double ask    = node.path("asks").get(0).path("price").asDouble();

            TickData tick = new TickData(
                symbol, TickData.Market.FOREX,
                bid, ask, (bid + ask) / 2.0, 0, "OANDA"
            );
            onTick(tick);

        } catch (Exception e) {
            log.error("[ForexFeed] Parse error: {}", e.getMessage());
        }
    }

    /** Simulates tick data when no API key is set — useful for development. */
    private void simulateFeed() throws InterruptedException {
        log.info("[ForexFeed] Simulation mode active.");
        double[] prices = {1.08500, 1.27200, 149.500, 0.64800,
                           1.36200, 0.89500, 0.84700, 161.200};
        int i = 0;
        while (running.get()) {
            for (String pair : PAIRS) {
                double mid    = prices[i % prices.length] * (1 + (Math.random() - 0.5) * 0.0002);
                double spread = 0.00010 + Math.random() * 0.00005;
                TickData tick = new TickData(
                    pair, TickData.Market.FOREX,
                    mid - spread / 2, mid + spread / 2, mid, 0, "SIMULATION"
                );
                onTick(tick);
                i++;
            }
            Thread.sleep(500);
        }
    }

    @Override
    public void stop() {
        super.stop();
        if (conn != null) conn.disconnect();
    }

    @Override
    protected String feedName() { return "ForexFeed"; }
}
