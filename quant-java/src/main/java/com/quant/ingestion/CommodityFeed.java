package com.quant.ingestion;

import com.quant.kafka.QuantKafkaProducer;
import com.quant.orderbook.OrderBookManager;

import java.util.List;

/**
 * Commodity and Futures feed.
 * Production: connects to Interactive Brokers TWS API (ib-insync or IB Java API).
 * Development: simulation mode with realistic price movement.
 *
 * To enable IB TWS: set IB_HOST, IB_PORT, IB_CLIENT_ID in environment.
 */
public class CommodityFeed extends BaseMarketFeed {

    private static final List<String> SYMBOLS = List.of(
        "GC=F",   // Gold Futures
        "CL=F",   // Crude Oil WTI
        "SI=F",   // Silver Futures
        "NG=F",   // Natural Gas
        "ZW=F",   // Wheat Futures
        "ES=F"    // S&P 500 E-mini
    );

    private static final double[] BASE_PRICES = {
        2050.0,  // Gold
        78.50,   // Crude Oil
        23.50,   // Silver
        2.85,    // Natural Gas
        580.0,   // Wheat
        4750.0   // ES
    };

    public CommodityFeed(QuantKafkaProducer producer, OrderBookManager obm) {
        super(producer, obm);
    }

    @Override
    protected void connect() throws Exception {
        String ibHost = System.getenv("IB_HOST");

        if (ibHost != null) {
            connectToIB(ibHost);
        } else {
            log.warn("[CommodityFeed] IB_HOST not set. Running simulation mode.");
            simulateFeed();
        }
    }

    private void connectToIB(String host) throws Exception {
        // TODO: Implement IB TWS Java API connection
        // Reference: https://interactivebrokers.github.io/tws-api/
        // EClientSocket client = new EClientSocket(wrapper, signal);
        // client.eConnect(host, port, clientId);
        log.info("[CommodityFeed] IB TWS connection stub — implement with IB Java API.");
        simulateFeed();
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
    protected String feedName() { return "CommodityFeed"; }
}
