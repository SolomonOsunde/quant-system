package com.quant.risk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pre-trade risk engine — all checks run synchronously before order submission.
 * Kill switch halts all trading immediately.
 */
public class RiskEngine {

    private static final Logger log = LoggerFactory.getLogger(RiskEngine.class);

    // --- Risk limits (load from config in production) ---
    private static final double MAX_POSITION_USD   = 100_000;
    private static final double MAX_ORDER_USD      = 10_000;
    private static final double MAX_DAILY_LOSS_USD = 5_000;
    private static final int    MAX_ORDERS_PER_MIN = 60;
    private static final double MAX_SPREAD_BPS     = 5.0;

    // --- State ---
    private final AtomicBoolean killSwitch    = new AtomicBoolean(false);
    private final AtomicInteger ordersThisMin = new AtomicInteger(0);
    private volatile double dailyPnl          = 0.0;
    private volatile long   minuteStart       = System.currentTimeMillis();

    private final Map<String, Double> positions = new ConcurrentHashMap<>();

    public enum CheckResult { APPROVED, REJECTED_KILL_SWITCH, REJECTED_POSITION_LIMIT,
        REJECTED_ORDER_SIZE, REJECTED_DAILY_LOSS, REJECTED_RATE_LIMIT, REJECTED_SPREAD }

    /**
     * Run all pre-trade checks. Returns APPROVED if trade can proceed.
     */
    public CheckResult check(String symbol, String side, double qty,
                              double price, double spreadBps) {

        if (killSwitch.get()) {
            log.warn("KILL SWITCH ACTIVE — order rejected for {}", symbol);
            return CheckResult.REJECTED_KILL_SWITCH;
        }

        double orderValueUsd = qty * price;
        if (orderValueUsd > MAX_ORDER_USD) {
            log.warn("Order size ${} exceeds limit ${}", orderValueUsd, MAX_ORDER_USD);
            return CheckResult.REJECTED_ORDER_SIZE;
        }

        double currentPos = positions.getOrDefault(symbol, 0.0);
        double newPos     = "BUY".equals(side) ? currentPos + orderValueUsd
                                               : currentPos - orderValueUsd;
        if (Math.abs(newPos) > MAX_POSITION_USD) {
            log.warn("Position limit breach for {} — current={} new={}", symbol, currentPos, newPos);
            return CheckResult.REJECTED_POSITION_LIMIT;
        }

        if (dailyPnl < -MAX_DAILY_LOSS_USD) {
            log.error("Daily loss limit reached: ${} — all trading halted.", dailyPnl);
            killSwitch.set(true);
            return CheckResult.REJECTED_DAILY_LOSS;
        }

        resetRateLimiterIfNeeded();
        if (ordersThisMin.incrementAndGet() > MAX_ORDERS_PER_MIN) {
            log.warn("Order rate limit hit ({}/min)", MAX_ORDERS_PER_MIN);
            return CheckResult.REJECTED_RATE_LIMIT;
        }

        if (spreadBps > MAX_SPREAD_BPS) {
            log.warn("Spread too wide for {}: {:.1f} bps > {:.1f}", symbol, spreadBps, MAX_SPREAD_BPS);
            return CheckResult.REJECTED_SPREAD;
        }

        return CheckResult.APPROVED;
    }

    public void updatePosition(String symbol, String side, double qty, double price) {
        double delta = qty * price * ("BUY".equals(side) ? 1 : -1);
        positions.merge(symbol, delta, Double::sum);
    }

    public void updatePnl(double pnlDelta) {
        dailyPnl += pnlDelta;
        if (dailyPnl < -MAX_DAILY_LOSS_USD) {
            log.error("DAILY LOSS LIMIT BREACHED: ${} — activating kill switch.", dailyPnl);
            killSwitch.set(true);
        }
    }

    public void activateKillSwitch() {
        log.error("KILL SWITCH MANUALLY ACTIVATED");
        killSwitch.set(true);
    }

    public void resetKillSwitch() {
        log.info("Kill switch reset by operator.");
        killSwitch.set(false);
    }

    private void resetRateLimiterIfNeeded() {
        long now = System.currentTimeMillis();
        if (now - minuteStart > 60_000) {
            ordersThisMin.set(0);
            minuteStart = now;
        }
    }

    public boolean isKillSwitchActive() { return killSwitch.get(); }
    public double getDailyPnl()          { return dailyPnl; }
    public Map<String, Double> getPositions() { return positions; }
}
