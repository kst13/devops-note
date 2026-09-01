package dev.devopsnote.kafkarunner.fault;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** docker stop/start 로 장애를 주입하고, AdminClient 로 클러스터 상태를 관찰한다. */
public class FaultInjector {
    private static final Logger log = LoggerFactory.getLogger(FaultInjector.class);
    private final String bootstrapServers;
    private final List<String> containers;

    public FaultInjector(String bootstrapServers, List<String> containers) {
        this.bootstrapServers = bootstrapServers;
        this.containers = containers;
    }

    static List<String> command(String action, String container) {
        return List.of("docker", action, container);
    }

    public void stop(String container) { run(command("stop", container)); }
    public void start(String container) { run(command("start", container)); }
    public void startAll() { containers.forEach(this::start); }

    /** 시나리오 시작 전 사전 점검: 3컨테이너가 모두 Up 인지. */
    public void ensureAllRunning() {
        for (String container : containers) {
            String state = output(List.of("docker", "inspect", "-f", "{{.State.Running}}", container)).trim();
            if (!"true".equals(state)) {
                throw new IllegalStateException("컨테이너 " + container + " 가 실행 중이 아닙니다. docker compose up -d 후 재시도하세요.");
            }
        }
    }

    /** 이전 실행 데이터를 지우기 위해 토픽을 삭제 후 재생성. */
    public void resetTopic(String topic, int partitions) {
        try (Admin admin = admin()) {
            try { admin.deleteTopics(List.of(topic)).all().get(); } catch (ExecutionException ignored) { }
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
        awaitCondition(Duration.ofSeconds(30), "토픽 삭제 완료",
            admin -> !admin.listTopics().names().get().contains(topic));
        ensureTopic(topic, partitions);
    }

    /** 테스트 토픽 생성 (이미 있으면 통과). */
    public void ensureTopic(String topic, int partitions) {
        try (Admin admin = admin()) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 3)
                    .configs(Map.of("min.insync.replicas", "2")))).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) throw new IllegalStateException(e);
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
    }

    /** 모든 파티션이 리더를 갖고 ISR 3개로 회복될 때까지 대기. */
    public void awaitFullIsr(String topic, Duration timeout) {
        awaitCondition(timeout, "ISR 완전 회복", admin -> {
            var description = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
            return description.partitions().stream().allMatch(p -> p.leader() != null && p.isr().size() == 3);
        });
    }

    /** 클러스터가 응답하고 모든 파티션에 리더가 있을 때까지 대기 (전체 정지 복구용). */
    public void awaitClusterReady(String topic, Duration timeout) {
        awaitCondition(timeout, "클러스터 응답·리더 존재", admin -> {
            var description = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
            return description.partitions().stream().allMatch(p -> p.leader() != null);
        });
    }

    private interface Check { boolean ok(Admin admin) throws Exception; }

    private void awaitCondition(Duration timeout, String what, Check check) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            try (Admin admin = admin()) {
                if (check.ok(admin)) { log.info("{} 확인 완료", what); return; }
            } catch (Exception e) { log.debug("{} 대기 중: {}", what, e.getMessage()); }
            sleep(2000);
        }
        throw new IllegalStateException(what + " 대기 시간 초과 (" + timeout + ")");
    }

    private Admin admin() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        return Admin.create(props);
    }

    private void run(List<String> cmd) {
        String out = output(cmd);
        log.info("$ {} -> {}", String.join(" ", cmd), out.trim());
    }

    private String output(List<String> cmd) {
        try {
            Process process = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            String out = new String(process.getInputStream().readAllBytes());
            if (!process.waitFor(30, TimeUnit.SECONDS) || process.exitValue() != 0) {
                throw new IllegalStateException("명령 실패: " + String.join(" ", cmd) + "\n" + out);
            }
            return out;
        } catch (java.io.IOException e) { throw new IllegalStateException(e); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
