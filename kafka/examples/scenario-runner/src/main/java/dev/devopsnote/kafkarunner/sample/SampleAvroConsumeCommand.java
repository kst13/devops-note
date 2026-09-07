package dev.devopsnote.kafkarunner.sample;

import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.sample.avro.OrderCreated;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** Avro 수신. deserializer 가 메시지 앞 5바이트에서 schema id 를 읽어 SR 에서 스키마를 받아온다(캐시됨). */
@Component
@Order(4)
public class SampleAvroConsumeCommand implements Command {
    public static final String LISTENER_ID = "sample-avro";
    public static final String GROUP_ID = "notification-service-avro";
    static final long IDLE_TIMEOUT_MS = 30_000;

    private final KafkaListenerEndpointRegistry registry;
    private final AtomicInteger received = new AtomicInteger();
    private volatile long lastReceivedAt;
    private volatile int target = Integer.MAX_VALUE;

    public SampleAvroConsumeCommand(KafkaListenerEndpointRegistry registry) { this.registry = registry; }

    @Override public String name() { return "sample-avro-consume"; }
    @Override public String description() { return "@KafkaListener 로 OrderCreated(Avro) N건 수신 — 기본 10건, 30초 무수신 시 종료"; }

    @KafkaListener(id = LISTENER_ID, topics = "${runner.sample-avro-topic}", groupId = GROUP_ID,
                   containerFactory = SampleKafkaConfig.AVRO_LISTENER_FACTORY, autoStartup = "false")
    public void onOrderCreated(ConsumerRecord<String, OrderCreated> record, Acknowledgment ack) {
        // 목표를 넘는 레코드는 ack 하지 않는다 → 커밋되지 않아 다음 실행에서 다시 읽힌다 (수동 커밋의 요점)
        // (리스너 concurrency 기본값이 1이라 check-then-increment 가 안전하다)
        if (received.get() >= target) return;
        OrderCreated event = record.value();
        System.out.printf("수신  partition=%d  offset=%d  key=%s  customer=%s  amount=%d%n",
            record.partition(), record.offset(), record.key(), event.getCustomerId(), event.getAmount());
        ack.acknowledge(); // 처리 완료 후 커밋 — 처리 전에 커밋하면 장애 시 메시지를 잃는다
        // 실제 서비스라면 ack 전에 멱등 판정을 둔다 (usage-guide 04 컨슈머 4장) — 재전송으로 인한 중복 수신은 정상 동작이다
        received.incrementAndGet();
        lastReceivedAt = System.currentTimeMillis();
    }

    @Override public int run(List<String> args) throws Exception {
        int count = SampleArgs.count(args, 10);
        received.set(0);
        target = count;
        MessageListenerContainer container = Objects.requireNonNull(
            registry.getListenerContainer(LISTENER_ID), "리스너 " + LISTENER_ID + " 가 등록되지 않았습니다");
        lastReceivedAt = System.currentTimeMillis();
        System.out.printf("group=%s 로 구독 시작 (목표 %d건)%n", GROUP_ID, count);
        container.start();
        try {
            while (received.get() < count) {
                if (System.currentTimeMillis() - lastReceivedAt > IDLE_TIMEOUT_MS) {
                    System.out.printf("%d초 동안 신규 메시지 없음 — 종료 (이미 커밋된 오프셋 이후만 읽습니다. 새로 보내려면 sample-avro-produce)%n",
                        IDLE_TIMEOUT_MS / 1000);
                    break;
                }
                Thread.sleep(200);
            }
        } finally {
            container.stop();
        }
        System.out.printf("완료: %d건 수신  (group=%s)%n", received.get(), GROUP_ID);
        // 0건 수신도 종료 코드 0 — "커밋된 오프셋 이후에 새 메시지가 없다"는 정상 상태이지 실패가 아니다
        return 0;
    }
}
