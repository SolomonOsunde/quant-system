package com.quant.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.UUID;

/**
 * OANDA v20 practice/live order client for forex.
 * Paper default: api-fxpractice.oanda.com
 */
public class OandaExecutionClient {

    private static final Logger log = LoggerFactory.getLogger(OandaExecutionClient.class);

    private final String apiKey;
    private final String accountId;
    private final String baseUrl;
    private final ObjectMapper mapper = new ObjectMapper();
    private final boolean enabled;

    public OandaExecutionClient() {
        this.apiKey = env("OANDA_API_KEY", "");
        this.accountId = env("OANDA_ACCOUNT_ID", "");
        boolean practice = !"false".equalsIgnoreCase(env("OANDA_PRACTICE", "true"));
        this.baseUrl = practice
                ? "https://api-fxpractice.oanda.com"
                : "https://api-fxtrade.oanda.com";
        this.enabled = apiKey != null && !apiKey.isEmpty()
                && accountId != null && !accountId.isEmpty();
        if (enabled) {
            log.info("OANDA execution enabled ({} account=...{})",
                    practice ? "practice" : "live",
                    accountId.substring(Math.max(0, accountId.length() - 4)));
        } else {
            log.warn("OANDA credentials not set — forex broker orders disabled");
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Place a market order. Units are signed: +buy / −sell.
     * Returns [orderId, fillQty, fillPrice, status] or null.
     */
    public ExecutionEngine.OrderResult placeMarket(String instrument, String side,
                                                   double unitsAbs, double fallbackPrice) {
        if (!enabled) return null;
        String oandaInst = toOandaInstrument(instrument);
        long units = Math.max(1L, Math.round(unitsAbs));
        if ("SELL".equalsIgnoreCase(side)) {
            units = -units;
        }
        String clientId = "q-" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        String body = String.format(Locale.US,
                "{\"order\":{\"type\":\"MARKET\",\"instrument\":\"%s\",\"units\":\"%d\","
                        + "\"timeInForce\":\"FOK\",\"positionFill\":\"DEFAULT\","
                        + "\"clientExtensions\":{\"id\":\"%s\"}}}",
                oandaInst, units, clientId);

        HttpURLConnection conn = null;
        try {
            URL url = new URL(baseUrl + "/v3/accounts/" + accountId + "/orders");
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Bearer " + apiKey);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept-Datetime-Format", "UNIX");
            conn.setDoOutput(true);
            conn.setConnectTimeout(5_000);
            conn.setReadTimeout(15_000);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }
            int status = conn.getResponseCode();
            String raw = readStream(status >= 200 && status < 300
                    ? conn.getInputStream() : conn.getErrorStream());
            if (status < 200 || status >= 300) {
                log.error("[OANDA] Order rejected HTTP {} — {}", status, raw);
                return null;
            }
            JsonNode resp = mapper.readTree(raw);
            JsonNode fill = resp.path("orderFillTransaction");
            if (fill.isMissingNode() || fill.isNull()) {
                // FOK may cancel
                JsonNode cancel = resp.path("orderCancelTransaction");
                if (!cancel.isMissingNode()) {
                    log.error("[OANDA] Order canceled: {}", cancel.path("reason").asText(""));
                    return null;
                }
                log.error("[OANDA] No fill transaction in response");
                return null;
            }
            String orderId = fill.path("id").asText(clientId);
            double fillUnits = Math.abs(parseDouble(fill.path("units").asText("0"), unitsAbs));
            double fillPx = parseDouble(fill.path("price").asText("0"), fallbackPrice);
            if (fillUnits <= 0 || fillPx <= 0) {
                return null;
            }
            log.info("[OANDA] Filled {} {} units={} @ {}", side, oandaInst, fillUnits, fillPx);
            return new ExecutionEngine.OrderResult(orderId, fillUnits, fillPx, "filled");
        } catch (Exception e) {
            log.error("[OANDA] Order error: {}", e.getMessage(), e);
            return null;
        } finally {
            if (conn != null) conn.disconnect();
        }
    }

    public static String toOandaInstrument(String symbol) {
        if (symbol == null) return "";
        String s = symbol.trim().toUpperCase(Locale.ROOT);
        if (s.contains("_")) return s;
        if (s.contains("/")) return s.replace("/", "_");
        return s;
    }

    public static String toInternal(String oandaInst) {
        if (oandaInst == null) return "";
        return oandaInst.trim().toUpperCase(Locale.ROOT).replace("_", "/");
    }

    private static final java.util.Set<String> FOREX_CCY = java.util.Set.of(
            "EUR", "GBP", "USD", "JPY", "AUD", "CAD", "CHF", "NZD"
    );

    public static boolean isForexSymbol(String symbol) {
        if (symbol == null) return false;
        String s = symbol.toUpperCase(Locale.ROOT).replace("_", "/");
        String[] parts = s.split("/");
        if (parts.length != 2) return false;
        return FOREX_CCY.contains(parts[0]) && FOREX_CCY.contains(parts[1]);
    }

    private static String env(String k, String def) {
        String v = System.getenv(k);
        return v == null || v.isEmpty() ? def : v;
    }

    private static double parseDouble(String s, double def) {
        try {
            if (s == null || s.isEmpty() || "null".equalsIgnoreCase(s)) return def;
            return Double.parseDouble(s);
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static String readStream(InputStream stream) throws IOException {
        if (stream == null) return "";
        return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }
}
