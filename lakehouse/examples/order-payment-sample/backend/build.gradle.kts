// Lakehouse PoC 샘플 백엔드 — 주문·결제 이벤트 발행(Kafka) + Trino 조회.
// Java 21, Spring Boot 3.5. Confluent 의존성 없음(JSON 직렬화)이라 Maven Central 만 쓴다.
plugins {
    java
    id("org.springframework.boot") version "3.5.5"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.osstem"
version = "0.1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.kafka:spring-kafka")
    runtimeOnly("com.h2database:h2")
    // 3단계: Trino JDBC. 드라이버 버전은 Trino 서버(483)와 맞춘다.
    implementation("io.trino:trino-jdbc:483")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
}

tasks.bootJar {
    archiveFileName.set("order-payment-sample.jar")
}

tasks.jar {
    enabled = false
}

tasks.test {
    useJUnitPlatform()
}
