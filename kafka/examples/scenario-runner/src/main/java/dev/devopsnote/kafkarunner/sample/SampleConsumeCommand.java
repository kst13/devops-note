package dev.devopsnote.kafkarunner.sample;

import dev.devopsnote.kafkarunner.command.Command;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** usage-guide 04 의 @KafkaListener 를 CLI 로. 리스너는 autoStartup=false 라 이 명령이 켤 때만 그룹에 참여한다
 *  (장애 시나리오 실행 중 샘플 컨슈머가 브로커에 그룹을 만드는 것을 막는다). */
@Component
@Order(2)
public class SampleConsumeCommand implements Command {
    static final String LISTENER_ID = "sample-json";
    static final String GROUP_ID = "notification-service";
    static final long IDLE_TIMEOUT_MS = 30_000;

    private final KafkaListenerEndpointRegistry registry;
    private final AtomicInteger received = new AtomicInteger();
    private volatile long lastReceivedAt;

    public SampleConsumeCommand(KafkaListenerEndpointRegistry registry) { this.registry = registry; }

    @Override public String name() { return "sample-consume"; }
    @Override public String description() { return "@KafkaListener 로 N건 수신 (JSON, 수동 ack) — 기본 10건, 30초 무수신 시 종료"; }

    @KafkaListener(id = LISTENER_ID, topics = "${runner.sample-topic}", groupId = GROUP_ID,
                   containerFactory = SampleKafkaConfig.JSON_LISTENER_FACTORY, autoStartup = "false")
    public void onOrderCreated(ConsumerRecord<String, OrderCreatedEvent> record, Acknowledgment ack) {
        OrderCreatedEvent event = record.value();
        System.out.printf("수신  partition=%d  offset=%d  key=%s  customer=%s  amount=%d%n",
            record.partition(), record.offset(), record.key(), event.customerId(), event.amount());
        ack.acknowledge(); // 처리 완료 후 커밋 — 처리 전에 커밋하면 장애 시 메시지를 잃는다
        received.incrementAndGet();
        lastReceivedAt = System.currentTimeMillis();
    }

    @Override public int run(List<String> args) throws Exception {
        int count = SampleArgs.count(args, 10);
        MessageListenerContainer container = registry.getListenerContainer(LISTENER_ID);
        received.set(0);
        lastReceivedAt = System.currentTimeMillis();
        System.out.printf("group=%s 로 구독 시작 (목표 %d건)%n", GROUP_ID, count);
        container.start();
        try {
            while (received.get() < count) {
                if (System.currentTimeMillis() - lastReceivedAt > IDLE_TIMEOUT_MS) {
                    System.out.printf("%d초 동안 신규 메시지 없음 — 종료 (이미 커밋된 오프셋 이후만 읽습니다. 새로 보내려면 sample-produce)%n",
                        IDLE_TIMEOUT_MS / 1000);
                    break;
                }
                Thread.sleep(200);
            }
        } finally {
            container.stop();
        }
        System.out.printf("완료: %d건 수신  (group=%s)%n", received.get(), GROUP_ID);
        return 0;
    }
}
