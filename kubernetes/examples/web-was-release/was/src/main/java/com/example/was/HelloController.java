package com.example.was;

import java.net.InetAddress;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 배포가 어느 버전·어느 Pod 로 들어갔는지 눈으로 확인하기 위한 API.
 * web(nginx) 가 /api/ 를 이 서비스로 프록시한다.
 */
@RestController
@RequestMapping("/api")
public class HelloController {

    @Value("${app.name}")
    private String name;

    @Value("${app.version}")
    private String version;

    @GetMapping("/hello")
    public Map<String, Object> hello() throws Exception {
        return Map.of(
            "service", name,
            "version", version,
            "hostname", InetAddress.getLocalHost().getHostName(),   // Pod 이름. 롤링 업데이트 때 바뀌는 것이 보인다
            "time", OffsetDateTime.now().toString()
        );
    }

    @GetMapping("/orders")
    public List<Map<String, Object>> orders() {
        return List.of(
            Map.of("id", 1001, "item", "임플란트 픽스처", "qty", 10),
            Map.of("id", 1002, "item", "어버트먼트", "qty", 4),
            Map.of("id", 1003, "item", "스캔바디", "qty", 2)
        );
    }
}
