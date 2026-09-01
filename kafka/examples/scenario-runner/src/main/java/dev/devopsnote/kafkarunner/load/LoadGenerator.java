package dev.devopsnote.kafkarunner.load;

import dev.devopsnote.kafkarunner.ledger.Ledger;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/** seq 박힌 메시지를 연속 생산하고 성공/실패를 Ledger 에 기록한다. */
public class LoadGenerator implements AutoCloseable {
    private final Producer<String, String> producer;
    private final Ledger ledger;
    private final String topic;
    private final AtomicLong seq = new AtomicLong();
    private final AtomicBoolean running = new AtomicBoolean();
    private Thread worker;

    public LoadGenerator(String bootstrapServers, String topic, Ledger ledger) {
        var props = new java.util.Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000"); // 장애 중 빠른 실패 판정
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.ledger = ledger;
    }

    /** 초당 약 200건 백그라운드 전송 시작. */
    public void start() {
        running.set(true);
        worker = Thread.ofPlatform().name("load-generator").start(() -> {
            while (running.get()) {
                sendOne(seq.incrementAndGet());
                try { Thread.sleep(5); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            }
        });
    }

    /** 지정 건수만 전송하고 콜백 완료까지 대기 (normal-roundtrip 용). */
    public void sendExactly(int count) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            long s = seq.incrementAndGet();
            producer.send(new ProducerRecord<>(topic, Long.toString(s), payload(s)), (meta, ex) -> {
                if (ex == null) ledger.recordSentOk(s); else ledger.recordSentFail(s);
                latch.countDown();
            });
        }
        latch.await(120, TimeUnit.SECONDS);
    }

    /** 실패로 기록된 seq 들을 재전송 (복구 후 폴백 재전송 검증용). */
    public void resend(Set<Long> seqs) throws InterruptedException {
        if (seqs.isEmpty()) return;
        CountDownLatch latch = new CountDownLatch(seqs.size());
        for (long s : seqs) {
            producer.send(new ProducerRecord<>(topic, Long.toString(s), payload(s)), (meta, ex) -> {
                if (ex == null) ledger.recordSentOk(s); else ledger.recordSentFail(s);
                latch.countDown();
            });
        }
        latch.await(120, TimeUnit.SECONDS);
    }

    private void sendOne(long s) {
        try {
            producer.send(new ProducerRecord<>(topic, Long.toString(s), payload(s)), (meta, ex) -> {
                if (ex == null) ledger.recordSentOk(s); else ledger.recordSentFail(s);
            });
        } catch (Exception e) { ledger.recordSentFail(s); } // max.block.ms 초과 등 즉시 실패
    }

    private String payload(long s) {
        return "{\"seq\":" + s + ",\"sentAt\":\"" + java.time.Instant.now() + "\"}";
    }

    /** 전송 루프를 멈추고 in-flight 콜백까지 반영한다. */
    public void stopAndDrain() {
        running.set(false);
        if (worker != null) { try { worker.join(3000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }
        producer.flush();
    }

    @Override public void close() { producer.close(java.time.Duration.ofSeconds(10)); }
}
