package com.osstem.commerce.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SampleApplication {
    public static void main(String[] args) {
        // Avro 1.12 부터 SpecificRecord 클래스를 스키마 이름으로 로딩할 때 신뢰 목록을 요구한다.
        // 없으면 첫 직렬화에서 SecurityException("... is not trusted to be included in Avro schemas").
        // JVM 옵션 -Dorg.apache.avro.SERIALIZABLE_PACKAGES=com.osstem 으로 줘도 같다. 이미 주어졌으면 덮어쓰지 않는다.
        if (System.getProperty("org.apache.avro.SERIALIZABLE_PACKAGES") == null) {
            System.setProperty("org.apache.avro.SERIALIZABLE_PACKAGES", "com.osstem");
        }
        SpringApplication.run(SampleApplication.class, args);
    }
}
