package dev.devopsnote.kafkarunner.load;

import dev.devopsnote.kafkarunner.ledger.Ledger;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** 별도 그룹으로 토픽 전체를 소비해 수신 seq 를 Ledger 에 기록한다. */
public class VerifierConsumer implements AutoCloseable {
    private final KafkaConsumer<String, String> consumer;
    private final Ledger ledger;
    private final AtomicBoolean running = new AtomicBoolean();
    private Thread worker;

    public VerifierConsumer(String bootstrapServers, String topic, Ledger ledger) {
        var props = new java.util.Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "verifier-" + System.currentTimeMillis());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        this.consumer = new KafkaConsumer<>(props);
        this.ledger = ledger;
        consumer.subscribe(List.of(topic));
    }

    public void start() {
        running.set(true);
        worker = Thread.ofPlatform().name("verifier-consumer").start(() -> {
            while (running.get()) {
                var records = consumer.poll(Duration.ofMillis(500));
                records.forEach(record -> ledger.recordReceived(Long.parseLong(record.key())));
            }
        });
    }

    /** 신규 수신이 quietMillis 동안 없을 때까지 대기 (따라잡기 완료 판정). */
    public void awaitQuiet(long quietMillis, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        long lastCount = -1;
        long quietSince = System.currentTimeMillis();
        while (System.currentTimeMillis() < deadline) {
            long now = ledger.received().size();
            if (now != lastCount) { lastCount = now; quietSince = System.currentTimeMillis(); }
            if (System.currentTimeMillis() - quietSince >= quietMillis) return;
            Thread.sleep(300);
        }
    }

    @Override public void close() {
        running.set(false);
        if (worker != null) { try { worker.join(3000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }
        consumer.close(Duration.ofSeconds(5));
    }
}
