package com.quant.risk;

import com.quant.QuantConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Pre-trade risk engine — synchronous checks before order submission.
 * Tracks both signed USD notional and signed quantity per symbol.
 * Daily PnL resets at UTC midnight; kill switch on breach.
 */
public class RiskEngine {

    private static final Logger log = LoggerFactory.getLogger(RiskEngine.class);

    private final double maxPositionUsd;
    private final double maxOrderUsd;
    private final double maxDailyLossUsd;
    private final int    maxOrdersPerMin;
    private final double maxSpreadBps;

    private final AtomicBoolean killSwitch = new AtomicBoolean(false);
    private final AtomicInteger ordersThisMin = new AtomicInteger(0);
    private final AtomicLong    minuteStartMs = new AtomicLong(System.currentTimeMillis());

    private volatile double dailyPnl = 0.0;
    private volatile LocalDate pnlDay = LocalDate.now(ZoneOffset.UTC);

    /** Signed USD notional: + long, − short */
    private final Map<String, Double> positionsUsd = new ConcurrentHashMap<>();
    /** Signed quantity: + long coins/shares, − short */
    private final Map<String, Double> positionsQty = new ConcurrentHashMap<>();
    /** Average entry price for open qty (absolute) */
    private final Map<String, Double> avgPrice = new ConcurrentHashMap<>();

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
        // Aligned with Python config.py soft limits
        this.maxPositionUsd  = QuantConfig.getDouble("risk.max.position.usd",  50_000);
        this.maxOrderUsd     = QuantConfig.getDouble("risk.max.order.usd",       5_000);
        this.maxDailyLossUsd = QuantConfig.getDouble("risk.max.daily.loss.usd",  5_000);
        this.maxOrdersPerMin = QuantConfig.getInt("risk.max.orders.per.minute",     60);
        this.maxSpreadBps    = QuantConfig.getDouble("risk.max.spread.bps",       200.0);

        log.info("RiskEngine limits — position=${} order=${} dailyLoss=${} rate={}/min spread={}bps",
            maxPositionUsd, maxOrderUsd, maxDailyLossUsd, maxOrdersPerMin, maxSpreadBps);
    }

    public CheckResult check(String symbol, String side, double qty,
                             double price, double spreadBps) {
        rollDailyPnlIfNeeded();

        if (killSwitch.get()) {
            log.warn("KILL SWITCH ACTIVE — order rejected for {}", symbol);
            return CheckResult.REJECTED_KILL_SWITCH;
        }

        double orderValueUsd = Math.abs(qty * price);
        if (orderValueUsd > maxOrderUsd) {
            log.warn("Order size ${} exceeds limit ${}", orderValueUsd, maxOrderUsd);
            return CheckResult.REJECTED_ORDER_SIZE;
        }

        double currentUsd = positionsUsd.getOrDefault(symbol, 0.0);
        double deltaUsd   = qty * price * ("BUY".equals(side) ? 1 : -1);
        double newUsd     = currentUsd + deltaUsd;
        if (Math.abs(newUsd) > maxPositionUsd) {
            log.warn("Position limit breach for {} — current={} new={}", symbol, currentUsd, newUsd);
            return CheckResult.REJECTED_POSITION_LIMIT;
        }

        if (dailyPnl < -maxDailyLossUsd) {
            log.error("Daily loss limit reached: ${} — all trading halted.", dailyPnl);
            killSwitch.set(true);
            return CheckResult.REJECTED_DAILY_LOSS;
        }

        if (spreadBps > maxSpreadBps) {
            log.warn("Spread too wide for {}: {} bps > {} bps",
                symbol, String.format("%.1f", spreadBps), maxSpreadBps);
            return CheckResult.REJECTED_SPREAD;
        }

        if (!tryAcquireRateSlot()) {
            log.warn("Order rate limit hit ({}/min)", maxOrdersPerMin);
            return CheckResult.REJECTED_RATE_LIMIT;
        }

        return CheckResult.APPROVED;
    }

    public synchronized void hydratePosition(String symbol, double qty, double avgPx) {
        if (Math.abs(qty) < 1e-12) {
            positionsQty.remove(symbol);
            positionsUsd.remove(symbol);
            avgPrice.remove(symbol);
            return;
        }
        positionsQty.put(symbol, qty);
        avgPrice.put(symbol, avgPx > 0 ? avgPx : 0.0);
        positionsUsd.put(symbol, qty * (avgPx > 0 ? avgPx : 0.0));
    }

    /**
     * Replace crypto (slash) positions with an Alpaca snapshot — broker is source of truth.
     */
    public synchronized void syncCryptoFromBroker(Map<String, double[]> brokerQtyAvg) {
        for (String sym : new java.util.ArrayList<>(positionsQty.keySet())) {
            if (sym.contains("/") && !brokerQtyAvg.containsKey(sym)) {
                positionsQty.remove(sym);
                positionsUsd.remove(sym);
                avgPrice.remove(sym);
            }
        }
        for (Map.Entry<String, double[]> e : brokerQtyAvg.entrySet()) {
            hydratePosition(e.getKey(), e.getValue()[0], e.getValue()[1]);
        }
    }

    public synchronized void updatePosition(String symbol, String side, double qty, double price) {
        double signedQty = "BUY".equals(side) ? qty : -qty;
        double prevQty   = positionsQty.getOrDefault(symbol, 0.0);
        double newQty    = prevQty + signedQty;

        if (Math.abs(newQty) < 1e-12) {
            positionsQty.remove(symbol);
            positionsUsd.remove(symbol);
            avgPrice.remove(symbol);
            return;
        }

        // Update average price on adding to same-direction position
        double prevAvg = avgPrice.getOrDefault(symbol, price);
        if (prevQty == 0 || Math.signum(prevQty) == Math.signum(newQty) && Math.abs(newQty) > Math.abs(prevQty)) {
            double added = Math.abs(signedQty);
            double kept  = Math.abs(prevQty);
            double newAvg = (kept * prevAvg + added * price) / (kept + added);
            avgPrice.put(symbol, newAvg);
        } else if (Math.signum(prevQty) != Math.signum(newQty)) {
            // Flipped — new side avg is fill price
            avgPrice.put(symbol, price);
        }
        // Reducing same side: keep prior avg

        positionsQty.put(symbol, newQty);
        positionsUsd.put(symbol, newQty * price);
    }

    public void updatePnl(double pnlDelta) {
        rollDailyPnlIfNeeded();
        dailyPnl += pnlDelta;
        if (dailyPnl < -maxDailyLossUsd) {
            log.error("DAILY LOSS LIMIT BREACHED: ${} — activating kill switch.", dailyPnl);
            killSwitch.set(true);
        }
    }

    private void rollDailyPnlIfNeeded() {
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        if (!today.equals(pnlDay)) {
            log.info("UTC day rollover — resetting daily PnL (was ${})", dailyPnl);
            dailyPnl = 0.0;
            pnlDay = today;
        }
    }

    /** Atomic-ish rate limit: reset window then increment under sync. */
    private synchronized boolean tryAcquireRateSlot() {
        long now = System.currentTimeMillis();
        long start = minuteStartMs.get();
        if (now - start > 60_000) {
            ordersThisMin.set(0);
            minuteStartMs.set(now);
        }
        return ordersThisMin.incrementAndGet() <= maxOrdersPerMin;
    }

    public void activateKillSwitch() {
        log.error("KILL SWITCH MANUALLY ACTIVATED");
        killSwitch.set(true);
    }

    public void resetKillSwitch() {
        log.info("Kill switch reset by operator.");
        killSwitch.set(false);
        rollDailyPnlIfNeeded();
    }

    public boolean isKillSwitchActive() { return killSwitch.get(); }
    public double  getDailyPnl()         { return dailyPnl; }

    /** @deprecated use getPositionsUsd — kept for callers expecting USD map */
    public Map<String, Double> getPositions() { return positionsUsd; }

    public Map<String, Double> getPositionsUsd() { return positionsUsd; }

    public double getPositionQty(String symbol) {
        return positionsQty.getOrDefault(symbol, 0.0);
    }

    public double getAvgPrice(String symbol) {
        return avgPrice.getOrDefault(symbol, 0.0);
    }
}
