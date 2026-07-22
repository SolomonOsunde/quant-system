package com.quant.risk;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RiskEngineTest {

    @Test
    void approvesSmallCryptoOrder() {
        RiskEngine eng = new RiskEngine();
        RiskEngine.CheckResult r = eng.check("BTC/USD", "BUY", 0.01, 50_000, 5.0);
        assertEquals(RiskEngine.CheckResult.APPROVED, r);
    }

    @Test
    void rejectsOversizedOrder() {
        RiskEngine eng = new RiskEngine();
        RiskEngine.CheckResult r = eng.check("BTC/USD", "BUY", 10, 50_000, 5.0);
        assertEquals(RiskEngine.CheckResult.REJECTED_ORDER_SIZE, r);
    }

    @Test
    void killSwitchBlocks() {
        RiskEngine eng = new RiskEngine();
        eng.activateKillSwitch();
        RiskEngine.CheckResult r = eng.check("ETH/USD", "BUY", 1, 100, 5.0);
        assertEquals(RiskEngine.CheckResult.REJECTED_KILL_SWITCH, r);
    }
}
