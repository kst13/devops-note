package dev.devopsnote.kafkarunner.sample;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** usage-guide 03 의 OrderEventProducer 를 CLI 로: send(topic, key, value) + 비동기 콜백. */
@Component
@Order(1)
public class SampleProduceCommand implements Command {
    private static final Logger log = LoggerFactory.getLogger(SampleProduceCommand.class);
    private final KafkaTemplate<String, OrderCreatedEvent> template;
    private final RunnerProperties props;

    public SampleProduceCommand(KafkaTemplate<String, OrderCreatedEvent> sampleJsonTemplate, RunnerProperties props) {
        this.template = sampleJsonTemplate;
        this.props = props;
    }

    @Override public String name() { return "sample-produce"; }
    @Override public String description() { return "OrderCreatedEvent N건 전송 (JSON, KafkaTemplate) — 기본 10건"; }

    @Override public int run(List<String> args) throws Exception {
        int count = SampleArgs.count(args, 10);
        String topic = props.sampleTopic();
        // compose 가 자동 생성을 꺼 두었으므로(운영과 동일) 토픽을 먼저 만든다
        new FaultInjector(props.bootstrapServers(), props.containers()).ensureTopic(topic, props.partitions());

        var latch = new CountDownLatch(count);
        var failures = new AtomicInteger();
        // 실패해도 N건을 끝까지 보내 합계를 보여준다. 순서가 중요한 실제 프로듀서는 실패 감지 시 뒤 이벤트 전송을 멈춰야 한다 (usage-guide 03 프로듀서 3장)
        for (int i = 1; i <= count; i++) {
            OrderCreatedEvent event = SampleEvents.json(i);
            // key = orderId → 같은 주문은 항상 같은 파티션 (출력에서 확인)
            template.send(topic, event.orderId(), event).whenComplete((result, ex) -> {
                if (ex != null) {
                    failures.incrementAndGet();
                    log.error("전송 실패 key={}", event.orderId(), ex); // acks=all 이라 여기 오면 실제 실패
                } else {
                    var meta = result.getRecordMetadata();
                    System.out.printf("전송 OK  key=%s  partition=%d  offset=%d%n", event.orderId(), meta.partition(), meta.offset());
                }
                latch.countDown();
            });
        }
        if (!latch.await(30, TimeUnit.SECONDS)) {
            System.out.printf("30초 안에 전송 콜백이 모두 오지 않았습니다 — %d/%d건 완료, 실패 %d건. 브로커 상태를 확인하세요%n",
                count - latch.getCount(), count, failures.get());
            return 1;
        }
        System.out.printf("완료: %d건 전송, 실패 %d건  (topic=%s)%n", count, failures.get(), topic);
        return failures.get() == 0 ? 0 : 1;
    }
}
