package dev.devopsnote.kafkarunner.sample;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/** 오류 시나리오 ②: 토픽에 Avro 가 아닌 메시지(poison pill)가 있을 때 Avro 컨슈머가 어떻게 되는가.
 *  일부러 깨진 바이트를 전용 토픽에 넣고 Avro 컨슈머로 읽으면 역직렬화가 실패한다. 실제 @KafkaListener 라면
 *  같은 메시지에서 무한 재시도(poison pill)에 빠지므로, 여기서는 수동 컨슈머로 1회만 안전하게 재현한다.
 *  실무 해결책은 ErrorHandlingDeserializer + DLQ 다 (usage-guide 06 자주 하는 실수). */
@Component
@Order(7)
public class SampleConsumePoisonCommand implements Command {
    static final String POISON_GROUP = "poison-demo";
    private final KafkaProperties kafka;
    private final RunnerProperties props;

    public SampleConsumePoisonCommand(KafkaProperties kafka, RunnerProperties props) {
        this.kafka = kafka;
        this.props = props;
    }

    @Override public String name() { return "sample-consume-poison"; }
    @Override public String description() { return "깨진 메시지(poison pill)를 Avro 컨슈머로 읽어 역직렬화 실패 재현"; }

    @Override public int run(List<String> args) throws Exception {
        SampleArgs.count(args, 1);
        // 실제 avro 토픽을 오염시키지 않도록 전용 토픽을 쓴다
        String topic = props.sampleAvroTopic() + "-poison";
        new FaultInjector(props.bootstrapServers(), props.containers()).ensureTopic(topic, props.partitions());

        // 1) poison pill 주입 — Avro magic byte 가 없는 일반 문자열 바이트
        System.out.println("poison pill 주입 (Avro 가 아닌 일반 문자열)...");
        try (Producer<String, String> producer = new KafkaProducer<>(stringProducerProps())) {
            producer.send(new ProducerRecord<>(topic, "poison-key", "this-is-not-avro")).get();
        }

        // 2) Avro 컨슈머로 읽기 — magic byte 가 없어 역직렬화가 실패한다
        System.out.println("Avro 컨슈머로 읽기 시도...");
        Map<String, Object> consumerProps = SampleKafkaConfig.avroConsumerProps(kafka);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, POISON_GROUP);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (Consumer<String, Object> consumer = new KafkaConsumer<>(consumerProps)) {
            var partitions = consumer.partitionsFor(topic).stream()
                .map(p -> new TopicPartition(topic, p.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            // poll 이 역직렬화 실패를 던진다 (RecordDeserializationException 은 SerializationException 의 하위)
            consumer.poll(Duration.ofSeconds(15));
            System.out.println("경고: poison pill 이 오류 없이 읽혔습니다 — 역직렬화 검증이 동작하지 않습니다. FAIL");
            return 1;
        } catch (SerializationException e) {
            System.out.printf("역직렬화 실패 — 컨슈머가 이 메시지에서 막힌다: %s%n", rootMessage(e));
            System.out.println("실무 해결책: ErrorHandlingDeserializer + DLQ 로 깨진 메시지를 격리한다 (usage-guide 06).");
            return 0;   // 오류 재현이 목적 — 실패하는 것이 정상(PASS)
        }
    }

    private Properties stringProducerProps() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return p;
    }

    private static String rootMessage(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause.getMessage();
    }
}
