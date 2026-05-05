package com.quant.ingestion;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TickData {

    public enum Market { FOREX, EQUITY, CRYPTO, COMMODITY }

    @JsonProperty("symbol")    public String symbol;
    @JsonProperty("market")    public Market market;
    @JsonProperty("bid")       public double bid;
    @JsonProperty("ask")       public double ask;
    @JsonProperty("last")      public double last;
    @JsonProperty("volume")    public double volume;
    @JsonProperty("timestamp") public long timestamp;
    @JsonProperty("source")    public String source;

    public TickData() {}

    public TickData(String symbol, Market market, double bid, double ask,
                    double last, double volume, String source) {
        this.symbol    = symbol;
        this.market    = market;
        this.bid       = bid;
        this.ask       = ask;
        this.last      = last;
        this.volume    = volume;
        this.timestamp = System.currentTimeMillis();
        this.source    = source;
    }

    public double getMidPrice() {
        return (bid + ask) / 2.0;
    }

    public double getSpread() {
        return ask - bid;
    }

    @Override
    public String toString() {
        return String.format("Tick[%s|%s bid=%.5f ask=%.5f last=%.5f vol=%.2f]",
            market, symbol, bid, ask, last, volume);
    }
}
