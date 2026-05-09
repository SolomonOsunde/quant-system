package com.quant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Loads application.properties from the classpath.
 * Environment variables take precedence (converted from dot.notation to UPPER_SNAKE_CASE).
 * Example: "kafka.bootstrap.servers" → env var "KAFKA_BOOTSTRAP_SERVERS".
 */
public class QuantConfig {

    private static final Logger log = LoggerFactory.getLogger(QuantConfig.class);
    private static final Properties props = new Properties();

    static {
        try (InputStream in = QuantConfig.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (in != null) {
                props.load(in);
                log.info("Loaded application.properties ({} keys)", props.size());
            } else {
                log.warn("application.properties not found on classpath — using defaults");
            }
        } catch (Exception e) {
            log.warn("Could not load application.properties: {}", e.getMessage());
        }
    }

    public static String get(String key, String defaultValue) {
        // Env var wins: kafka.bootstrap.servers → KAFKA_BOOTSTRAP_SERVERS
        String envKey = key.replace(".", "_").replace("-", "_").toUpperCase();
        String envVal = System.getenv(envKey);
        if (envVal != null && !envVal.isBlank()) return envVal;
        return props.getProperty(key, defaultValue);
    }

    public static double getDouble(String key, double defaultValue) {
        try {
            return Double.parseDouble(get(key, String.valueOf(defaultValue)));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static int getInt(String key, int defaultValue) {
        try {
            return Integer.parseInt(get(key, String.valueOf(defaultValue)));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
