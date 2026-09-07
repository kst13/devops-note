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

        // sample-avro-produce 를 먼저 안 돌렸어도 되도록 v1 을 여기서도 등록 (같은 스키마면 기존 id 반환)
        int v1Id;
        try {
            v1Id = schemaRegistry.register(subject, new AvroSchema(OrderCreated.getClassSchema()));
        } catch (IOException | RestClientException e) {
            System.out.printf("SR 호출 실패 — Schema Registry 상태를 확인하세요: %s%n", rootMessage(e));
            return 1;
        }
        System.out.printf("v1 등록 확인  subject=%s  schemaId=%d%n", subject, v1Id);

        // ① 호환 스키마 등록 — 필드 추가 + 기본값 → BACKWARD 통과
        AvroSchema v2 = loadSchema("order-created-v2.avsc");
        int v2Id;
        try {
            v2Id = schemaRegistry.register(subject, v2);
        } catch (IOException | RestClientException e) {
            System.out.printf("SR 호출 실패 — Schema Registry 상태를 확인하세요: %s%n", rootMessage(e));
            return 1;
        }
        System.out.printf("① v2 등록 성공  schemaId=%d  (couponCode 는 기본값 null → 옛 데이터를 읽을 수 있어 BACKWARD 호환)%n", v2Id);

        try (Consumer<String, Object> consumer = newConsumerAtEnd(topic)) {
            // ② v2 로 한 건 전송 — GenericRecord 라 생성 클래스 없이도 새 스키마로 보낼 수 있다
            GenericRecord record = new GenericData.Record(v2.rawSchema());
            record.put("orderId", "ORD-2001");
            record.put("customerId", "CUST-9");
            record.put("amount", 55_000L);
            record.put("createdAt", Instant.now().toString());
            record.put("couponCode", "WELCOME10");
            try {
                template.send(topic, "ORD-2001", record).get(30, TimeUnit.SECONDS);
            } catch (SerializationException e) {
                System.out.printf("② 전송 실패 — Schema Registry 상태와 스키마 호환성을 확인하세요: %s%n", rootMessage(e));
                return 1;
            } catch (ExecutionException e) {
                System.out.printf("② 전송 실패 — Schema Registry 상태를 확인하세요: %s%n", rootMessage(e));
                return 1;
            }
            System.out.println("② v2 메시지 전송  key=ORD-2001  couponCode=WELCOME10");

            // ③ v1 생성 클래스(reader 스키마)로 v2 메시지(writer 스키마) 수신
            OrderCreated got = pollOne(consumer, Duration.ofSeconds(30));
            if (got == null) {
                System.out.println("③ 30초 안에 메시지를 받지 못했습니다 — FAIL");
                return 1;
            }
            System.out.printf("③ v1 클래스로 수신 OK  orderId=%s  amount=%d  (writer=v2, reader=v1 — couponCode 는 reader 에 없어 무시됨)%n",
                got.getOrderId(), got.getAmount());
        }

        // ④ 비호환 스키마 등록 — 기본값 없는 필드 추가 → SR 이 거부
        AvroSchema incompatible = loadSchema("order-created-incompatible.avsc");
        try {
            int id = schemaRegistry.register(subject, incompatible);
            System.out.printf("④ 비호환 스키마가 등록되어 버렸습니다 (id=%d) — SR 호환성 설정을 확인하세요. FAIL%n", id);
            return 1;
        } catch (RestClientException e) {
            if (e.getStatus() != 409) {
                System.out.printf("SR 호출 실패 — Schema Registry 상태를 확인하세요: %s%n", rootMessage(e));
                return 1;
            }
            System.out.printf("④ SR 거부  HTTP 409  (channel 에 기본값이 없어 옛 데이터를 읽을 수 없음)%n    %s%n", firstLine(e.getMessage()));
        } catch (IOException e) {
            System.out.printf("SR 호출 실패 — Schema Registry 상태를 확인하세요: %s%n", rootMessage(e));
            return 1;
        }

        List<Integer> versions;
        try {
            versions = schemaRegistry.getAllVersions(subject);
        } catch (IOException | RestClientException e) {
            System.out.printf("SR 호출 실패 — Schema Registry 상태를 확인하세요: %s%n", rootMessage(e));
            return 1;
        }
        System.out.printf("subject=%s 버전 목록: %s  (비호환 스키마는 버전에 남지 않는다)%n", subject, versions);
        return 0;
    }

    /** 전송 직전에 모든 파티션 끝에 위치시켜, 이번에 보내는 1건만 읽는다. */
    private Consumer<String, Object> newConsumerAtEnd(String topic) {
        var consumerProps = SampleKafkaConfig.avroConsumerProps(kafka);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "schema-evolution-" + System.currentTimeMillis());
        var consumer = new KafkaConsumer<String, Object>(consumerProps);
        List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
            .map(p -> new TopicPartition(topic, p.partition())).toList();
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        partitions.forEach(consumer::position); // seekToEnd 는 지연 평가라 position() 으로 확정
        return consumer;
    }

    private static OrderCreated pollOne(Consumer<String, Object> consumer, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            for (var record : consumer.poll(Duration.ofMillis(500))) {
                if (record.value() instanceof OrderCreated event) return event;
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

    private static String firstLine(String s) { return s == null ? "" : s.lines().findFirst().orElse(""); }

    /** 원인 체인을 끝까지 따라가 근본 원인의 메시지를 돌려준다 — 래핑된 예외의 최상위 메시지는 대개 정보가 없다. */
    private static String rootMessage(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage();
    }
}
