package com.quant.risk;

import com.quant.QuantConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pre-trade risk engine — all checks run synchronously before order submission.
 * Limits are loaded from application.properties (env vars take precedence).
 * Kill switch halts all trading immediately.
 */
public class RiskEngine {

    private static final Logger log = LoggerFactory.getLogger(RiskEngine.class);

    // Risk limits — loaded from config at construction time
    private final double maxPositionUsd;
    private final double maxOrderUsd;
    private final double maxDailyLossUsd;
    private final int    maxOrdersPerMin;
    private final double maxSpreadBps;

    // State
    private final AtomicBoolean killSwitch    = new AtomicBoolean(false);
    private final AtomicInteger ordersThisMin = new AtomicInteger(0);
    private volatile double dailyPnl          = 0.0;
    private volatile long   minuteStart       = System.currentTimeMillis();

    private final Map<String, Double> positions = new ConcurrentHashMap<>();

    public enum CheckResult {
        APPROVED,
        REJECTED_KILL_SWITCH,
        REJECTED_POSITION_LIMIT,
        REJECTED_ORDER_SIZE,
        REJECTED_DAILY_LOSS,
        REJECTED_RATE_LIMIT,
        REJECTED_SPREAD
    }

    public RiskEngine() {
        this.maxPositionUsd  = QuantConfig.getDouble("risk.max.position.usd",    100_000);
        this.maxOrderUsd     = QuantConfig.getDouble("risk.max.order.usd",        10_000);
        this.maxDailyLossUsd = QuantConfig.getDouble("risk.max.daily.loss.usd",    5_000);
        this.maxOrdersPerMin = QuantConfig.getInt("risk.max.orders.per.minute",       60);
        this.maxSpreadBps    = QuantConfig.getDouble("risk.max.spread.bps",          5.0);

        log.info("RiskEngine limits — position=${} order=${} dailyLoss=${} rate={}/min spread={}bps",
            maxPositionUsd, maxOrderUsd, maxDailyLossUsd, maxOrdersPerMin, maxSpreadBps);
    }

    /**
     * Run all pre-trade checks. Returns APPROVED if the trade can proceed.
     */
    public CheckResult check(String symbol, String side, double qty,
                              double price, double spreadBps) {

        if (killSwitch.get()) {
            log.warn("KILL SWITCH ACTIVE — order rejected for {}", symbol);
            return CheckResult.REJECTED_KILL_SWITCH;
        }

        double orderValueUsd = qty * price;
        if (orderValueUsd > maxOrderUsd) {
            log.warn("Order size ${} exceeds limit ${}", orderValueUsd, maxOrderUsd);
            return CheckResult.REJECTED_ORDER_SIZE;
        }

        double currentPos = positions.getOrDefault(symbol, 0.0);
        double newPos     = "BUY".equals(side)
            ? currentPos + orderValueUsd
            : currentPos - orderValueUsd;
        if (Math.abs(newPos) > maxPositionUsd) {
            log.warn("Position limit breach for {} — current={} new={}", symbol, currentPos, newPos);
            return CheckResult.REJECTED_POSITION_LIMIT;
        }

        if (dailyPnl < -maxDailyLossUsd) {
            log.error("Daily loss limit reached: ${} — all trading halted.", dailyPnl);
            killSwitch.set(true);
            return CheckResult.REJECTED_DAILY_LOSS;
        }

        resetRateLimiterIfNeeded();
        if (ordersThisMin.incrementAndGet() > maxOrdersPerMin) {
            log.warn("Order rate limit hit ({}/min)", maxOrdersPerMin);
            return CheckResult.REJECTED_RATE_LIMIT;
        }

        if (spreadBps > maxSpreadBps) {
            log.warn("Spread too wide for {}: {:.1f} bps > {:.1f}", symbol, spreadBps, maxSpreadBps);
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
        if (dailyPnl < -maxDailyLossUsd) {
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
    public double  getDailyPnl()         { return dailyPnl; }
    public Map<String, Double> getPositions() { return positions; }
}
