package dev.devopsnote.kafkarunner.sample;

import dev.devopsnote.kafkarunner.RunnerProperties;
import dev.devopsnote.kafkarunner.command.Command;
import dev.devopsnote.kafkarunner.fault.FaultInjector;
import dev.devopsnote.kafkarunner.sample.avro.OrderCreated;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** 오류 시나리오 ①: 스키마에 맞지 않는 데이터를 프로듀서가 보낼 때 무슨 일이 일어나는가.
 *  amount(long) 자리에 문자열을 넣은 레코드를 전송하면 KafkaAvroSerializer 가 직렬화 단계에서 거부한다 —
 *  잘못된 데이터는 토픽에 들어가지 못한다. JSON 이었다면 그대로 나갔을 것이다. 오류 재현이 이 명령의 목적이다. */
@Component
@Order(6)
public class SampleProduceInvalidCommand implements Command {
    private final KafkaTemplate<String, Object> template;
    private final RunnerProperties props;

    public SampleProduceInvalidCommand(KafkaTemplate<String, Object> sampleAvroTemplate, RunnerProperties props) {
        this.template = sampleAvroTemplate;
        this.props = props;
    }

    @Override public String name() { return "sample-produce-invalid"; }
    @Override public String description() { return "스키마에 맞지 않는 데이터 전송 → 직렬화 실패 재현(잘못된 데이터는 토픽에 못 들어감)"; }

    @Override public int run(List<String> args) throws Exception {
        SampleArgs.count(args, 1);   // 인자 검증만 (이 명령은 1건만 시도)
        String topic = props.sampleAvroTopic();
        new FaultInjector(props.bootstrapServers(), props.containers()).ensureTopic(topic, props.partitions());

        // amount 는 스키마상 long 인데 문자열을 넣는다 — 스키마 위반
        GenericRecord invalid = new GenericData.Record(OrderCreated.getClassSchema());
        invalid.put("orderId", "ORD-9001");
        invalid.put("customerId", 10001L);
        invalid.put("amount", "not-a-number");
        invalid.put("createdAt", "2026-09-08T00:00:00Z");

        System.out.println("스키마 위반 레코드 전송 시도 (amount=long 자리에 문자열)...");
        try {
            template.send(topic, "ORD-9001", invalid).get(30, TimeUnit.SECONDS);
        } catch (SerializationException e) {
            // KafkaAvroSerializer 가 send() 안에서 즉시 던진다 (콜백이 아니라 예외)
            System.out.printf("직렬화 거부됨 — 잘못된 데이터는 토픽에 들어가지 못한다: %s%n", rootMessage(e));
            return 0;   // 오류 재현이 목적 — 거부되는 것이 정상(PASS)
        } catch (ExecutionException e) {
            if (rootCause(e) instanceof SerializationException) {
                System.out.printf("직렬화 거부됨 — 잘못된 데이터는 토픽에 들어가지 못한다: %s%n", rootMessage(e));
                return 0;
            }
            System.out.printf("전송 실패(직렬화 오류 아님) — 브로커 상태를 확인하세요: %s%n", rootMessage(e));
            return 1;
        } catch (TimeoutException e) {
            System.out.printf("30초 안에 전송이 끝나지 않았습니다: %s%n", rootMessage(e));
            return 1;
        }
        System.out.println("경고: 스키마 위반 레코드가 전송되어 버렸습니다 — 직렬화 검증이 동작하지 않습니다. FAIL");
        return 1;
    }

    private static Throwable rootCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }

    private static String rootMessage(Throwable t) {
        return rootCause(t).getMessage();
    }
}
