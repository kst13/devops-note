package dev.devopsnote.kafkarunner.sample;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.sample.avro.OrderCreated;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Avro 전송. 코드는 JSON 단계와 같고 serializer 만 다르다 — SR 등록은 KafkaAvroSerializer 안에서 일어난다.
 *  전송이 끝난 뒤 출력하는 schemaId 는 이 실행이 실제로 쓴 writer 스키마의 id 다 — "최신" 이 아니다
 *  (sample-schema-evolution 이 v2 를 등록해 두면 최신과 달라질 수 있다). */
@Component
@Order(3)
public class SampleAvroProduceCommand implements Command {
    private static final Logger log = LoggerFactory.getLogger(SampleAvroProduceCommand.class);
    private final KafkaTemplate<String, Object> template;
    private final SchemaRegistryClient schemaRegistry;
    private final RunnerProperties props;

    public SampleAvroProduceCommand(KafkaTemplate<String, Object> sampleAvroTemplate,
                                    SchemaRegistryClient schemaRegistry, RunnerProperties props) {
        this.template = sampleAvroTemplate;
        this.schemaRegistry = schemaRegistry;
        this.props = props;
    }

    @Override public String name() { return "sample-avro-produce"; }
    @Override public String description() { return "OrderCreated(Avro) N건 전송 — serializer 가 SR 에 스키마 등록, 기본 10건"; }

    @Override public int run(List<String> args) throws Exception {
        int count = SampleArgs.count(args, 10);
        String topic = props.sampleAvroTopic();
        // compose 가 자동 생성을 꺼 두었으므로(운영과 동일) 토픽을 먼저 만든다
        new FaultInjector(props.bootstrapServers(), props.containers()).ensureTopic(topic, props.partitions());

        var latch = new CountDownLatch(count);
        var failures = new AtomicInteger();
        try {
            // 실패해도 N건을 끝까지 보내 합계를 보여준다. 순서가 중요한 실제 프로듀서는 실패 감지 시 뒤 이벤트 전송을 멈춰야 한다 (usage-guide 03 프로듀서 3장)
            for (int i = 1; i <= count; i++) {
                OrderCreated event = SampleEvents.avro(i);
                // key = orderId → 같은 주문은 항상 같은 파티션 (출력에서 확인)
                template.send(topic, event.getOrderId(), event).whenComplete((result, ex) -> {
                    if (ex != null) {
                        failures.incrementAndGet();
                        log.error("전송 실패 key={}", event.getOrderId(), ex); // acks=all 이라 여기 오면 실제 실패
                    } else {
                        var meta = result.getRecordMetadata();
                        System.out.printf("전송 OK  key=%s  partition=%d  offset=%d  serializedValueSize=%dB%n",
                            event.getOrderId(), meta.partition(), meta.offset(), meta.serializedValueSize());
                    }
                    latch.countDown();
                });
            }
        } catch (SerializationException e) {
            // KafkaAvroSerializer 는 SR 접속 실패·스키마 비호환을 send() 안에서 즉시 던진다 (콜백이 아니라 예외)
            System.out.printf("직렬화 실패 — Schema Registry 상태와 스키마 호환성을 확인하세요: %s%n", rootMessage(e));
            return 1;
        }
        if (!latch.await(30, TimeUnit.SECONDS)) {
            System.out.printf("30초 안에 전송 콜백이 모두 오지 않았습니다 — %d/%d건 완료, 실패 %d건. 브로커 상태를 확인하세요%n",
                count - latch.getCount(), count, failures.get());
            return 1;
        }

        // 앱 코드는 SR 을 호출한 적이 없지만 serializer 가 등록해 두었다 — 이 실행이 쓴 writer 스키마의 id·버전을 확인만 한다
        String subject = topic + "-value";
        try {
            var writerSchema = new AvroSchema(OrderCreated.getClassSchema());
            System.out.printf("SR 확인: subject=%s  version=%d  schemaId=%d  (메시지에는 magic 1B + 이 id 4B 만 실린다 — 스키마 본문은 없다)%n",
                subject, schemaRegistry.getVersion(subject, writerSchema), schemaRegistry.getId(subject, writerSchema));
        } catch (IOException | RestClientException e) {
            System.out.printf("SR 조회 실패 (전송은 완료됨): %s%n", rootMessage(e));
            return 1;
        }
        System.out.printf("완료: %d건 전송, 실패 %d건  (topic=%s)%n", count, failures.get(), topic);
        return failures.get() == 0 ? 0 : 1;
    }

    /** 원인 체인을 끝까지 따라가 근본 원인의 메시지를 돌려준다 — 래핑된 예외의 최상위 메시지는 대개 정보가 없다. */
    private static String rootMessage(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage();
    }
}
