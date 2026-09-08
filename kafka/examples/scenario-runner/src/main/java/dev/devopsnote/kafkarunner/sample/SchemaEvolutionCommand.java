package dev.devopsnote.kafkarunner.sample;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.sample.avro.OrderCreated;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** 스키마 진화를 한 번에 재현: v2 호환 등록 → v2 전송 → v1 클래스로 수신 → 비호환 등록 거부(409). */
@Component
@Order(5)
public class SchemaEvolutionCommand implements Command {
    /** 이번 실행이 보내고 받는 메시지의 key — 동시에 다른 sample 명령이 같은 토픽에 쓰고 있어도 ③ 이 그 메시지를 집지 않도록 고정한다. */
    private static final String EVOLUTION_KEY = "ORD-2001";

    private final SchemaRegistryClient schemaRegistry;
    private final KafkaTemplate<String, Object> template;
    private final KafkaProperties kafka;
    private final RunnerProperties props;

    public SchemaEvolutionCommand(SchemaRegistryClient schemaRegistry, KafkaTemplate<String, Object> sampleAvroTemplate,
                                  KafkaProperties kafka, RunnerProperties props) {
        this.schemaRegistry = schemaRegistry;
        this.template = sampleAvroTemplate;
        this.kafka = kafka;
        this.props = props;
    }

    @Override public String name() { return "sample-schema-evolution"; }
    @Override public String description() { return "v2 호환 등록 → v2 전송 → v1 클래스로 수신 → 비호환 스키마 409 거부 재현"; }

    @Override public int run(List<String> args) throws Exception {
        String topic = props.sampleAvroTopic();
        String subject = topic + "-value";
        new FaultInjector(props.bootstrapServers(), props.containers()).ensureTopic(topic, props.partitions());

        try {
            // sample-avro-produce 를 먼저 안 돌렸어도 되도록 v1 을 여기서도 등록 (같은 스키마면 기존 id 반환)
            int v1Id = schemaRegistry.register(subject, new AvroSchema(OrderCreated.getClassSchema()));
            System.out.printf("v1 등록 (이미 있으면 기존 id)  subject=%s  schemaId=%d%n", subject, v1Id);

            // ① 호환 스키마 등록 — 필드 추가 + 기본값 → BACKWARD 통과
            AvroSchema v2 = loadSchema("order-created-v2.avsc");
            int v2Id = schemaRegistry.register(subject, v2);
            System.out.printf("① v2 등록 성공  schemaId=%d  (couponCode 는 기본값 null → 옛 데이터를 읽을 수 있어 BACKWARD 호환)%n", v2Id);

            try (Consumer<String, Object> consumer = newConsumerAtEnd(topic)) {
                // ② v2 로 한 건 전송 — GenericRecord 라 생성 클래스 없이도 새 스키마로 보낼 수 있다
                GenericRecord record = new GenericData.Record(v2.rawSchema());
                record.put("orderId", EVOLUTION_KEY);
                record.put("customerId", 10_009L);
                record.put("amount", 55_000L);
                record.put("createdAt", Instant.now().toString());
                record.put("couponCode", "WELCOME10");
                try {
                    template.send(topic, EVOLUTION_KEY, record).get(30, TimeUnit.SECONDS);
                } catch (SerializationException e) {
                    System.out.printf("② 전송 실패 — Schema Registry 상태와 스키마 호환성을 확인하세요: %s%n", rootMessage(e));
                    return 1;
                } catch (ExecutionException e) {
                    System.out.printf("② 전송 실패 — Schema Registry 상태를 확인하세요: %s%n", rootMessage(e));
                    return 1;
                } catch (TimeoutException e) {
                    System.out.printf("② 전송 실패 — 30초 안에 전송이 끝나지 않았습니다: %s%n", rootMessage(e));
                    return 1;
                }
                System.out.printf("② v2 메시지 전송  key=%s  couponCode=WELCOME10%n", EVOLUTION_KEY);

                // ③ v1 생성 클래스(reader 스키마)로, 이번 실행이 보낸 메시지(key=EVOLUTION_KEY)만 골라 수신
                OrderCreated got;
                try {
                    got = pollOne(consumer, EVOLUTION_KEY, Duration.ofSeconds(30));
                } catch (SerializationException e) {
                    System.out.printf("③ 역직렬화 실패 — reader(v1)/writer 스키마 해석 실패: %s%n", rootMessage(e));
                    return 1;
                }
                if (got == null) {
                    System.out.println("③ 30초 안에 메시지를 받지 못했습니다 — FAIL");
                    return 1;
                }
                System.out.printf("③ v1 클래스로 수신 OK  key=%s  orderId=%s  amount=%d  (writer=v2 schemaId=%d, reader=v1 — couponCode 는 reader 스키마에 없어 무시됨)%n",
                    EVOLUTION_KEY, got.getOrderId(), got.getAmount(), v2Id);
                System.out.println("   주의: 이 방향(옛 reader × 새 데이터)은 FORWARD 호환이다. 이번 변경(기본값 있는 필드 추가)이 마침 FULL 이라 되는 것이지, SR 의 BACKWARD 설정만으로는 보장되지 않는다");
            }

            // ④ 비호환 스키마 등록 — 기본값 없는 필드 추가 → SR 이 거부
            AvroSchema incompatible = loadSchema("order-created-incompatible.avsc");
            try {
                int id = schemaRegistry.register(subject, incompatible);
                System.out.printf("④ 비호환 스키마가 등록되어 버렸습니다 (id=%d) — SR 호환성 설정을 확인하세요. FAIL%n", id);
                return 1;
            } catch (RestClientException e) {
                if (e.getStatus() != 409) throw e;
                System.out.printf("④ SR 거부  HTTP 409  (channel 에 기본값이 없어 옛 데이터를 읽을 수 없음)%n    %s%n", brief(e.getMessage()));
            }

            List<Integer> versions = schemaRegistry.getAllVersions(subject);
            System.out.printf("subject=%s 버전 목록: %s  (비호환 스키마는 버전에 남지 않는다)%n", subject, versions);
            return 0;
        } catch (IOException | RestClientException e) {
            System.out.printf("SR 호출 실패 — Schema Registry 상태를 확인하세요: %s%n", rootMessage(e));
            return 1;
        }
    }

    /** 전송 직전에 모든 파티션 끝에 위치시켜, 이번에 보내는 1건만 읽는다. */
    private Consumer<String, Object> newConsumerAtEnd(String topic) {
        var consumerProps = SampleKafkaConfig.avroConsumerProps(kafka);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "schema-evolution-" + System.currentTimeMillis());
        var consumer = new KafkaConsumer<String, Object>(consumerProps);
        try {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                .map(p -> new TopicPartition(topic, p.partition())).toList();
            if (partitions == null || partitions.isEmpty()) {
                throw new IllegalStateException("토픽 " + topic + " 의 파티션 정보를 얻지 못했습니다");
            }
            consumer.assign(partitions);
            consumer.seekToEnd(partitions);
            partitions.forEach(consumer::position); // seekToEnd 는 지연 평가라 position() 으로 확정
            return consumer;
        } catch (RuntimeException e) {
            consumer.close();
            throw e;
        }
    }

    /** key 가 일치하고 v1 클래스로 역직렬화되는 레코드만 인정 — 동시에 다른 sample 명령이 같은 토픽에 쓰고 있어도 그 메시지를 집지 않는다. */
    private static OrderCreated pollOne(Consumer<String, Object> consumer, String key, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            for (var record : consumer.poll(Duration.ofMillis(500))) {
                if (key.equals(record.key()) && record.value() instanceof OrderCreated event) return event;
            }
        }
        return null;
    }

    private AvroSchema loadSchema(String name) throws IOException {
        try (InputStream in = getClass().getResourceAsStream("/schemas/" + name)) {
            if (in == null) throw new IllegalStateException("스키마 리소스 없음: /schemas/" + name);
            return new AvroSchema(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    /** SR 예외 메시지는 스키마 본문을 통째로 담아 수백 자가 되기도 한다 — details 이후를 잘라내고 길이를 제한한다. */
    private static String brief(String s) {
        if (s == null) return "";
        int idx = s.indexOf(", details:");
        String cut = idx >= 0 ? s.substring(0, idx) : s;
        return cut.length() > 200 ? cut.substring(0, 200) + "…" : cut;
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
