package com.quant.config;

import com.quant.QuantConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Loads enabled instruments / stat-arb pairs from Timescale/Postgres.
 * Empty universe is a hard failure — live mode has no hardcoded fallback.
 */
public final class InstrumentRepository {

    private static final Logger log = LoggerFactory.getLogger(InstrumentRepository.class);

    public record Instrument(String symbol, String market, String brokerSymbol, int priority) {}

    public record StatArbPair(String leg1, String leg2, int priority) {}

    private InstrumentRepository() {}

    private static Connection open() throws Exception {
        String raw = System.getenv("DATABASE_URL");
        if (raw == null || raw.isBlank()) {
            raw = QuantConfig.get("timescale.url", "jdbc:postgresql://localhost:5432/quantdb");
        }
        if (raw.startsWith("postgresql://") || raw.startsWith("postgres://")) {
            raw = "jdbc:" + raw;
        }
        // URL already embeds credentials (postgres://user:pass@host/db)
        if (raw.contains("@") && raw.contains("://")
                && raw.indexOf('@') > raw.indexOf("://") + 3) {
            return DriverManager.getConnection(raw);
        }
        String user = QuantConfig.get("timescale.user",
                System.getenv().getOrDefault("POSTGRES_USER", "quant"));
        String pass = QuantConfig.get("timescale.password",
                System.getenv().getOrDefault("POSTGRES_PASSWORD", "quant123"));
        return DriverManager.getConnection(raw, user, pass);
    }

    public static List<Instrument> loadEnabled(String market) {
        String mkt = market.toUpperCase(Locale.ROOT);
        String sql = """
            SELECT symbol, market, broker_symbol, priority
            FROM instruments
            WHERE enabled = TRUE AND market = ?
            ORDER BY priority ASC, symbol ASC
            """;
        List<Instrument> out = new ArrayList<>();
        try (Connection c = open();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, mkt);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new Instrument(
                            rs.getString("symbol"),
                            rs.getString("market"),
                            rs.getString("broker_symbol"),
                            rs.getInt("priority")));
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to load instruments for market=" + mkt + ": " + e.getMessage(), e);
        }
        log.info("Loaded {} enabled {} instrument(s) from DB", out.size(), mkt);
        return out;
    }

    public static List<String> symbols(String market) {
        return loadEnabled(market).stream().map(Instrument::symbol).toList();
    }

    public static List<String> brokerSymbols(String market) {
        return loadEnabled(market).stream().map(Instrument::brokerSymbol).toList();
    }

    public static List<StatArbPair> loadStatArbPairs() {
        String sql = """
            SELECT leg1, leg2, priority
            FROM stat_arb_pairs
            WHERE enabled = TRUE
            ORDER BY priority ASC, leg1 ASC, leg2 ASC
            """;
        List<StatArbPair> out = new ArrayList<>();
        try (Connection c = open();
             PreparedStatement ps = c.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(new StatArbPair(
                        rs.getString("leg1"),
                        rs.getString("leg2"),
                        rs.getInt("priority")));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load stat_arb_pairs: " + e.getMessage(), e);
        }
        log.info("Loaded {} enabled stat-arb pair(s) from DB", out.size());
        return out;
    }
}
