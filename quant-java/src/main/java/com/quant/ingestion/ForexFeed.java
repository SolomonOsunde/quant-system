package com.quant.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quant.SymbolMapper;
import com.quant.config.InstrumentRepository;
import com.quant.kafka.QuantKafkaProducer;
import com.quant.orderbook.OrderBookManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

/**
 * OANDA v20 streaming API feed for Forex tick data.
 * Pair list comes from Postgres {@code instruments} (market=FOREX).
 * Live-only — no simulation fallback.
 */
public class ForexFeed extends BaseMarketFeed {

    private final ObjectMapper mapper = new ObjectMapper();
    private volatile HttpURLConnection conn;

    public ForexFeed(QuantKafkaProducer producer, OrderBookManager obm) {
        super(producer, obm);
    }

    @Override
    protected void connect() throws Exception {
        String apiKey    = System.getenv("OANDA_API_KEY");
        String accountId = System.getenv("OANDA_ACCOUNT_ID");
        boolean practice = !"false".equalsIgnoreCase(
                System.getenv().getOrDefault("OANDA_PRACTICE", "true"));

        if (apiKey == null || apiKey.isEmpty() || accountId == null || accountId.isEmpty()) {
            throw new IllegalStateException(
                    "[ForexFeed] OANDA_API_KEY / OANDA_ACCOUNT_ID required for live testing — simulation removed");
        }

        List<InstrumentRepository.Instrument> instruments =
                InstrumentRepository.loadEnabled("FOREX");
        if (instruments.isEmpty()) {
            throw new IllegalStateException(
                    "[ForexFeed] No enabled FOREX rows in instruments table — seed DB first");
        }

        List<String> brokerPairs = instruments.stream()
                .map(InstrumentRepository.Instrument::brokerSymbol)
                .collect(Collectors.toList());
        for (InstrumentRepository.Instrument inst : instruments) {
            SymbolMapper.register(inst.symbol(), inst.brokerSymbol().replace("_", ""));
        }

        String host = practice
                ? "https://stream-fxpractice.oanda.com"
                : "https://stream-fxtrade.oanda.com";
        String joined = String.join("%2C", brokerPairs);
        String urlStr = String.format(
                "%s/v3/accounts/%s/pricing/stream?instruments=%s",
                host, accountId, joined);

        conn = (HttpURLConnection) new URL(urlStr).openConnection();
        conn.setRequestProperty("Authorization", "Bearer " + apiKey);
        conn.setRequestProperty("Accept-Encoding", "identity");
        conn.connect();

        log.info("[ForexFeed] Connected to OANDA {} — {} pairs from DB: {}",
                practice ? "practice" : "live", brokerPairs.size(), brokerPairs);

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(conn.getInputStream()))) {
            String line;
            while (running.get() && (line = reader.readLine()) != null) {
                if (!line.isBlank()) handleMessage(line);
            }
        }
        throw new Exception("OANDA stream ended — will retry");
    }

    private void handleMessage(String raw) {
        try {
            JsonNode node = mapper.readTree(raw);
            if (!"PRICE".equals(node.path("type").asText())) return;

            String oandaSym = node.path("instrument").asText();
            String symbol = oandaSym.replace('_', '/');
            SymbolMapper.register(symbol, oandaSym.replace("_", ""));
            double bid = node.path("bids").get(0).path("price").asDouble();
            double ask = node.path("asks").get(0).path("price").asDouble();

            TickData tick = new TickData(
                symbol, TickData.Market.FOREX,
                bid, ask, (bid + ask) / 2.0, 0, "OANDA"
            );
            onTick(tick);
        } catch (Exception e) {
            log.error("[ForexFeed] Parse error: {}", e.getMessage());
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
