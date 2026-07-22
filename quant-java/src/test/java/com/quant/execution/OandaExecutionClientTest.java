package com.quant.execution;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OandaExecutionClientTest {

    @Test
    void detectsForexNotCrypto() {
        assertTrue(OandaExecutionClient.isForexSymbol("EUR/USD"));
        assertTrue(OandaExecutionClient.isForexSymbol("EUR_USD"));
        assertTrue(OandaExecutionClient.isForexSymbol("GBP/JPY"));
        assertFalse(OandaExecutionClient.isForexSymbol("BTC/USD"));
        assertFalse(OandaExecutionClient.isForexSymbol("ETH/USD"));
        assertFalse(OandaExecutionClient.isForexSymbol("SOL/USD"));
    }

    @Test
    void instrumentRoundTrip() {
        assertEquals("EUR_USD", OandaExecutionClient.toOandaInstrument("EUR/USD"));
        assertEquals("EUR/USD", OandaExecutionClient.toInternal("EUR_USD"));
    }
}
