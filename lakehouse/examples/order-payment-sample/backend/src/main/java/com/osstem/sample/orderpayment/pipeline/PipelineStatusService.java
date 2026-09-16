package com.osstem.sample.orderpayment.pipeline;

import com.osstem.sample.orderpayment.analytics.AnalyticsRepository;
import com.osstem.sample.orderpayment.config.AppProperties;
import com.osstem.sample.orderpayment.order.OrderRepository;
import com.osstem.sample.orderpayment.payment.PaymentRepository;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

/**
 * 파이프라인 각 단계의 현재 상태를 한 번에 모은다.
 *   앱(H2) → Kafka(토픽 오프셋·컨슈머 lag) → Kafka Connect(커넥터·태스크 상태)
 *   → Iceberg/MinIO(스냅샷·파일·커밋된 Kafka 오프셋) → Trino(행 수)
 * 단계마다 독립적으로 조회하고 실패는 그 단계의 error 필드에만 담는다. 한 단계가 죽어도 나머지는 보인다.
 */
@Service
public class PipelineStatusService {

    private final OrderRepository orders;
    private final PaymentRepository payments;
    private final KafkaAdmin kafkaAdmin;
    private final AnalyticsRepository trino;
    private final AppProperties props;
    private final RestClient http = RestClient.create();

    public PipelineStatusService(OrderRepository orders, PaymentRepository payments, KafkaAdmin kafkaAdmin,
                                 AnalyticsRepository trino, AppProperties props) {
        this.orders = orders;
        this.payments = payments;
        this.kafkaAdmin = kafkaAdmin;
        this.trino = trino;
        this.props = props;
    }

    public Map<String, Object> status() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("app", stage(this::app));
        out.put("kafka", stage(this::kafka));
        out.put("connect", stage(this::connect));
        out.put("iceberg", stage(this::iceberg));
        out.put("trino", stage(this::trinoRows));
        return out;
    }

    private interface Stage { Map<String, Object> call() throws Exception; }

    private Map<String, Object> stage(Stage s) {
        try {
            Map<String, Object> m = s.call();
            m.put("ok", true);
            return m;
        } catch (Exception e) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ok", false);
            m.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
            return m;
        }
    }

    // ---------- ① 앱 (운영 DB 역할)
    private Map<String, Object> app() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("orders", orders.count());
        m.put("payments", payments.count());
        m.put("topics", List.of(props.topics().order(), props.topics().payment()));
        return m;
    }

    // ---------- ② Kafka: 토픽별 파티션 끝 오프셋, 커넥터 컨슈머 그룹의 커밋 오프셋과 lag
    private Map<String, Object> kafka() throws Exception {
        Map<String, Object> cfg = new HashMap<>(kafkaAdmin.getConfigurationProperties());
        cfg.put("request.timeout.ms", "5000");
        cfg.put("default.api.timeout.ms", "5000");
        try (AdminClient admin = AdminClient.create(cfg)) {
            List<String> topics = List.of(props.topics().order(), props.topics().payment());
            Map<String, TopicDescription> desc = admin.describeTopics(topics).allTopicNames().get(5, TimeUnit.SECONDS);
            Map<TopicPartition, OffsetSpec> want = new HashMap<>();
            desc.forEach((t, d) -> d.partitions().forEach(p -> want.put(new TopicPartition(t, p.partition()), OffsetSpec.latest())));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends = admin.listOffsets(want).all().get(5, TimeUnit.SECONDS);

            List<Map<String, Object>> topicRows = new ArrayList<>();
            Map<String, Long> totalByTopic = new LinkedHashMap<>();
            for (String t : topics) {
                List<Map<String, Object>> parts = new ArrayList<>();
                long total = 0;
                for (var p : desc.get(t).partitions()) {
                    long end = ends.get(new TopicPartition(t, p.partition())).offset();
                    total += end;
                    parts.add(Map.of("partition", p.partition(), "endOffset", end));
                }
                totalByTopic.put(t, total);
                topicRows.add(Map.of("topic", t, "partitions", parts, "messages", total));
            }

            List<Map<String, Object>> groups = new ArrayList<>();
            for (String connector : props.pipeline().connectors()) {
                String group = "connect-" + connector;
                Map<TopicPartition, OffsetAndMetadata> committed =
                        admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                long committedTotal = 0, lag = 0;
                String topic = null;
                for (var e : committed.entrySet()) {
                    topic = e.getKey().topic();
                    long c = e.getValue().offset();
                    committedTotal += c;
                    var end = ends.get(e.getKey());
                    if (end != null) lag += Math.max(0, end.offset() - c);
                }
                if (topic == null) {
                    // 아직 커밋한 적 없음 → 전체가 lag
                    topic = connector.startsWith("order") ? props.topics().order() : props.topics().payment();
                    lag = totalByTopic.getOrDefault(topic, 0L);
                }
                Map<String, Object> g = new LinkedHashMap<>();
                g.put("group", group);
                g.put("topic", topic);
                g.put("committed", committedTotal);
                g.put("lag", lag);
                groups.add(g);
            }
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("bootstrap", cfg.get("bootstrap.servers"));
            m.put("topics", topicRows);
            m.put("groups", groups);
            return m;
        }
    }

    // ---------- ③ Kafka Connect: 커넥터·태스크 상태 (REST)
    private Map<String, Object> connect() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (String name : props.pipeline().connectors()) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("name", name);
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> st = http.get().uri(props.pipeline().connectUrl() + "/connectors/" + name + "/status")
                        .retrieve().body(Map.class);
                @SuppressWarnings("unchecked")
                Map<String, Object> c = (Map<String, Object>) st.get("connector");
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> tasks = (List<Map<String, Object>>) st.get("tasks");
                row.put("state", c.get("state"));
                row.put("tasks", tasks.stream().map(t -> t.get("state")).toList());
                row.put("commitIntervalMs", 30000);
            } catch (Exception e) {
                row.put("state", "UNKNOWN");
                row.put("error", e.getMessage());
            }
            rows.add(row);
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("url", props.pipeline().connectUrl());
        m.put("connectors", rows);
        return m;
    }

    // ---------- ④ Iceberg / MinIO: 테이블별 스냅샷·파일·커밋된 Kafka 오프셋 (Trino 메타데이터 테이블로 조회)
    private Map<String, Object> iceberg() {
        List<Map<String, Object>> tables = new ArrayList<>();
        for (String t : List.of("order_events", "payment_events")) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("table", "commerce." + t);
            var snap = trino.query("""
                    SELECT snapshot_id, committed_at,
                           json_format(cast(map_filter(summary, (k, v) -> k LIKE 'kafka.connect.offsets%%') AS json)) AS kafka_offsets,
                           element_at(summary, 'added-records') AS added_records
                    FROM "%s$snapshots" ORDER BY committed_at DESC LIMIT 1
                    """.formatted(t));
            var cnt = trino.query("SELECT count(*) AS snapshots FROM \"%s$snapshots\"".formatted(t));
            var files = trino.query("""
                    SELECT count(*) AS files, coalesce(sum(record_count), 0) AS records, coalesce(sum(file_size_in_bytes), 0) AS bytes,
                           regexp_replace(min(file_path), '/data/.*$', '') AS location
                    FROM "%s$files"
                    """.formatted(t));
            var recent = trino.query("""
                    SELECT regexp_extract(file_path, '[^/]+$') AS file, record_count, file_size_in_bytes
                    FROM "%s$files" ORDER BY file_path DESC LIMIT 5
                    """.formatted(t));
            row.put("snapshots", cnt.isEmpty() ? 0 : cnt.get(0).get("snapshots"));
            if (!snap.isEmpty()) {
                row.put("lastCommittedAt", snap.get(0).get("committed_at"));
                row.put("lastSnapshotId", snap.get(0).get("snapshot_id"));
                row.put("lastAddedRecords", snap.get(0).get("added_records"));
                row.put("kafkaOffsets", snap.get(0).get("kafka_offsets"));
            }
            if (!files.isEmpty()) {
                row.put("files", files.get(0).get("files"));
                row.put("records", files.get(0).get("records"));
                row.put("bytes", files.get(0).get("bytes"));
                row.put("location", files.get(0).get("location"));
            }
            row.put("recentFiles", recent);
            tables.add(row);
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("tables", tables);
        return m;
    }

    // ---------- ⑤ Trino: 테이블 행 수 (조회 화면이 보는 것)
    private Map<String, Object> trinoRows() {
        // UNION ALL 은 순서를 보장하지 않으므로 ORDER BY 로 고정한다 (화면이 인덱스로 이전 값과 비교한다)
        var rows = trino.query("""
                SELECT t, rows FROM (
                  SELECT 'commerce.order_events' AS t, count(*) AS rows FROM order_events
                  UNION ALL SELECT 'commerce.payment_events', count(*) FROM payment_events
                ) ORDER BY t
                """);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("url", props.analytics().trinoUrl());
        m.put("tables", rows);
        return m;
    }
}
