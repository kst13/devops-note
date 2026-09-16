package com.osstem.sample.orderpayment.pipeline;

import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 파이프라인 화면용. 앱 → Kafka → Connect → Iceberg/MinIO → Trino 각 단계의 현재 상태를 한 응답으로. */
@RestController
@RequestMapping("/api/pipeline")
public class PipelineController {

    private final PipelineStatusService service;

    public PipelineController(PipelineStatusService service) {
        this.service = service;
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        return service.status();
    }
}
