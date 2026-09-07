package dev.devopsnote.kafkarunner.sample;

import dev.devopsnote.kafkarunner.sample.avro.OrderCreated;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

/** sample-* 명령 전용 Kafka 빈. 접속·acks·idempotence·auto-offset-reset 은 application.yml 의 spring.kafka.* 에서 오고,
 *  여기서는 명령마다 다른 serializer/deserializer 만 지정한다. 러너 내부(load/)는 부하 제어를 위해 kafka-clients 를 직접 쓴다. */
@Configuration
public class SampleKafkaConfig {
    static final String JSON_LISTENER_FACTORY = "sampleJsonListenerFactory";
    static final String AVRO_LISTENER_FACTORY = "sampleAvroListenerFactory";
    static final String SCHEMA_REGISTRY_URL_KEY = "schema.registry.url";

    private final KafkaProperties kafka;

    public SampleKafkaConfig(KafkaProperties kafka) { this.kafka = kafka; }

    /** Avro 1.12 는 스키마 이름으로 생성 클래스를 찾을 때(SpecificData) 신뢰 목록을 요구한다 — 임의 클래스 로딩 취약점 대응.
     *  Avro 클래스가 로딩되기 전에 설정돼야 하므로 RunnerApplication.main 이 Spring 기동 전에 호출한다.
     *  운영 앱이라면 JVM 옵션 -Dorg.apache.avro.SERIALIZABLE_PACKAGES=... 로 두는 것이 일반적이다. */
    public static void trustGeneratedAvroClasses() {
        System.setProperty("org.apache.avro.SERIALIZABLE_PACKAGES", OrderCreated.class.getPackageName());
    }

    /** 팩토리를 빈으로 두어야 컨텍스트 종료 시 프로듀서가 close 된다 (KafkaTemplate 은 외부에서 받은 팩토리를 닫지 않는다). */
    @Bean
    public DefaultKafkaProducerFactory<String, OrderCreatedEvent> sampleJsonProducerFactory() {
        Map<String, Object> props = kafka.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    /** usage-guide 05 와 같은 조합: String key + JsonSerializer.
     *  주의: KafkaTemplate 빈을 하나라도 정의하면 Boot 의 기본 kafkaTemplate 은 @ConditionalOnMissingBean 으로 물러난다 —
     *  그래서 샘플마다 템플릿을 직접 선언한다. */
    @Bean
    public KafkaTemplate<String, OrderCreatedEvent> sampleJsonTemplate(
            DefaultKafkaProducerFactory<String, OrderCreatedEvent> sampleJsonProducerFactory) {
        return new KafkaTemplate<>(sampleJsonProducerFactory);
    }

    /** usage-guide 04·05: JsonDeserializer + trusted packages + 수동 ack. */
    @Bean(JSON_LISTENER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> sampleJsonListenerFactory() {
        Map<String, Object> props = kafka.buildConsumerProperties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "dev.devopsnote.kafkarunner.sample"); // 미설정 시 역직렬화 예외
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderCreatedEvent.class.getName());
        var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    /** 팩토리를 빈으로 두어야 컨텍스트 종료 시 프로듀서가 close 된다 (KafkaTemplate 은 외부에서 받은 팩토리를 닫지 않는다). */
    @Bean
    public DefaultKafkaProducerFactory<String, Object> sampleAvroProducerFactory() {
        Map<String, Object> props = kafka.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    /** Avro + Schema Registry. serializer 가 SR 에 스키마를 등록·조회하므로 앱 코드에는 SR 호출이 없다.
     *  schema.registry.url 은 spring.kafka.properties 에서 buildProducerProperties() 로 함께 들어온다.
     *  값 타입을 Object 로 두어 생성 클래스(SpecificRecord)와 GenericRecord 를 한 템플릿으로 보낸다. */
    @Bean
    public KafkaTemplate<String, Object> sampleAvroTemplate(
            DefaultKafkaProducerFactory<String, Object> sampleAvroProducerFactory) {
        return new KafkaTemplate<>(sampleAvroProducerFactory);
    }

    /** Avro 컨슈머 설정. ConsumerFactory 를 빈으로 노출하면 Boot 기본 팩토리와 제네릭이 충돌하므로 static 으로 공유한다. */
    static Map<String, Object> avroConsumerProps(KafkaProperties kafka) {
        Map<String, Object> props = kafka.buildConsumerProperties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        // true: writer 스키마의 이름으로 생성 클래스(OrderCreated)를 찾아 역직렬화. false 면 GenericRecord
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return props;
    }

    @Bean(AVRO_LISTENER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, Object> sampleAvroListenerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, Object>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(avroConsumerProps(kafka)));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    /** 스키마 id·버전 조회와 sample-schema-evolution 의 직접 등록용. 생성 시점에는 접속하지 않는다. */
    @Bean
    public SchemaRegistryClient schemaRegistryClient() {
        String url = Objects.requireNonNull(kafka.getProperties().get(SCHEMA_REGISTRY_URL_KEY),
            "spring.kafka.properties.schema.registry.url 이 필요합니다");
        return new CachedSchemaRegistryClient(url, 100);
    }
}
