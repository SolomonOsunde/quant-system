package com.quant;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bidirectional Alpaca ticker ↔ internal slash symbol map.
 * Registered by market feeds; used by execution / risk hydrate.
 */
public final class SymbolMapper {

    private static final Map<String, String> ALPACA_TO_SLASH = new ConcurrentHashMap<>();
    private static final Map<String, String> SLASH_TO_ALPACA = new ConcurrentHashMap<>();

    private SymbolMapper() {}

    public static void register(String slashSymbol, String alpacaSymbol) {
        if (slashSymbol == null || alpacaSymbol == null) return;
        String slash = slashSymbol.toUpperCase(Locale.ROOT);
        String alpaca = alpacaSymbol.toUpperCase(Locale.ROOT).replace("/", "");
        ALPACA_TO_SLASH.put(alpaca, slash);
        SLASH_TO_ALPACA.put(slash, alpaca);
    }

    public static void registerSlash(String slashSymbol) {
        if (slashSymbol == null) return;
        register(slashSymbol, slashSymbol.replace("/", ""));
    }

    /** Alpaca BTCUSD → BTC/USD when known; else best-effort. */
    public static String toInternal(String alpacaOrSlash) {
        if (alpacaOrSlash == null || alpacaOrSlash.isEmpty()) return "";
        String s = alpacaOrSlash.toUpperCase(Locale.ROOT);
        if (s.contains("/")) return s;
        String mapped = ALPACA_TO_SLASH.get(s);
        if (mapped != null) return mapped;
        if (s.endsWith("USD") && s.length() > 3) {
            return s.substring(0, s.length() - 3) + "/USD";
        }
        return s;
    }

    public static String toAlpaca(String internal) {
        if (internal == null || internal.isEmpty()) return "";
        String s = internal.toUpperCase(Locale.ROOT);
        String mapped = SLASH_TO_ALPACA.get(s);
        if (mapped != null) return mapped;
        return s.replace("/", "");
    }
}
