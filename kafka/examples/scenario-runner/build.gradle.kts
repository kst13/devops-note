// Avro 코드 생성 플러그인(davidmc24)은 자체적으로 구버전 avro-compiler 를 쓰므로,
// buildscript classpath 에 런타임과 같은 버전을 올려 생성기·런타임 버전을 일치시킨다
// (pom 시절 avro-maven-plugin 버전을 런타임과 고정했던 것과 같은 의도).
buildscript {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
    dependencies {
        classpath("com.github.davidmc24.gradle.plugin:gradle-avro-plugin:1.9.1")
        classpath("org.apache.avro:avro-compiler:1.12.2")
    }
}

plugins {
    java
    id("org.springframework.boot") version "3.5.5"
    id("io.spring.dependency-management") version "1.1.7"
}

// buildscript 로 올린 플러그인은 plugins {} 블록이 아니라 apply 로 적용한다
apply(plugin = "com.github.davidmc24.gradle.plugin.avro")

group = "dev.devopsnote"
version = "0.1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
    // kafka-avro-serializer 는 Maven Central 에 없다. 사내 미러를 쓰면 이 저장소를 허용 목록에 추가.
    maven("https://packages.confluent.io/maven/")
}

// 7.9 라인을 쓰는 이유: 8.x 는 kafka-clients 4.1 API(Monitorable)를 요구하는데 Spring Boot 3.5 는 3.9.1 을 고정한다
val confluentVersion = "7.9.9"
// Avro 런타임: 코드 생성기(buildscript 의 avro-compiler)와 같은 버전으로 고정
val avroVersion = "1.12.2"

dependencies {
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.apache.avro:avro:$avroVersion")
    // Schema Registry 연동 serializer (kafka-schema-registry-client 포함)
    implementation("io.confluent:kafka-avro-serializer:$confluentVersion")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

configure<com.github.davidmc24.gradle.plugin.avro.AvroExtension> {
    stringType.set("String")   // CharSequence 대신 String getter 가 나오도록
}

tasks.bootJar {
    archiveFileName.set("scenario-runner.jar")   // README 의 java -jar build/libs/scenario-runner.jar 와 짝
}

// 실행 불가능한 -plain.jar 는 만들지 않는다 — build/libs 에 jar 가 하나만 남아 헷갈릴 일이 없다
tasks.jar {
    enabled = false
}

tasks.test {
    useJUnitPlatform()
}
