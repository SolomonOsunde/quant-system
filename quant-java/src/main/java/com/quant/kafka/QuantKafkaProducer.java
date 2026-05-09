package com.quant.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.quant.QuantConfig;
import com.quant.ingestion.TickData;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Kafka producer/consumer for the Java core layer.
 *
 * Topics:
 *   quant.ticks         — raw tick data (Java → Python)
 *   quant.orderbook     — order book snapshots (Java → Python)
 *   quant.signals       — trade signals (Python → Java)
 *   quant.executions    — fill confirmations (Java → Python)
 */
public class QuantKafkaProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(QuantKafkaProducer.class);

    public static final String TOPIC_TICKS      = "quant.ticks";
    public static final String TOPIC_ORDERBOOK  = "quant.orderbook";
    public static final String TOPIC_SIGNALS    = "quant.signals";
    public static final String TOPIC_EXECUTIONS = "quant.executions";

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicLong tickCount = new AtomicLong(0);

    public QuantKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            QuantConfig.get("kafka.bootstrap.servers", "localhost:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");                    // balance speed/durability
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1");               // micro-batch
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");      // fast compression
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");

        this.producer = new KafkaProducer<>(props);
        log.info("Kafka producer initialized → {}", props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    /**
     * Publish a tick to quant.ticks topic.
     * Key = symbol (ensures ordering per symbol).
     */
    public void publishTick(TickData tick) {
        try {
            String json = mapper.writeValueAsString(tick);
            ProducerRecord<String, String> record =
                new ProducerRecord<>(TOPIC_TICKS, tick.symbol, json);

            producer.send(record, (metadata, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish tick for {}: {}", tick.symbol, ex.getMessage());
                } else if (tickCount.incrementAndGet() % 10_000 == 0) {
                    log.info("Published {} ticks. Last offset: {}/{}/{}",
                        tickCount.get(), metadata.topic(), metadata.partition(), metadata.offset());
                }
            });

        } catch (Exception e) {
            log.error("Tick serialization error: {}", e.getMessage());
        }
    }

    /**
     * Publish an execution confirmation back to Python.
     */
    public void publishExecution(String symbol, String side, double qty,
                                  double price, String orderId) {
        try {
            String json = String.format(
                "{\"symbol\":\"%s\",\"side\":\"%s\",\"qty\":%.4f," +
                "\"price\":%.5f,\"orderId\":\"%s\",\"ts\":%d}",
                symbol, side, qty, price, orderId, System.currentTimeMillis());
            producer.send(new ProducerRecord<>(TOPIC_EXECUTIONS, symbol, json));
        } catch (Exception e) {
            log.error("Execution publish error: {}", e.getMessage());
        }
    }

    /**
     * Create a Kafka consumer for reading Python trade signals.
     */
    public KafkaConsumer<String, String> createSignalConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            QuantConfig.get("kafka.bootstrap.servers", "localhost:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG,              groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,   "100");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(TOPIC_SIGNALS));
        return consumer;
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
        log.info("Kafka producer closed. Total ticks published: {}", tickCount.get());
    }
}
