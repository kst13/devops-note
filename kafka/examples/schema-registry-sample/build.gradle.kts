// usage-guide/09-schema-registry.md 3장 "의존성" 을 그대로 옮긴 빌드 파일.
// Avro 코드 생성 플러그인은 자체 avro-compiler 버전을 쓰므로 런타임과 같은 버전을 buildscript 에 고정한다.
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

apply(plugin = "com.github.davidmc24.gradle.plugin.avro")

group = "com.osstem"
version = "0.1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
    // kafka-avro-serializer 는 Maven Central 에 없다. 사내 Nexus/Artifactory 를 거친다면 이 주소를 프록시에 등록해야 한다.
    maven("https://packages.confluent.io/maven/")
}

// Spring Boot 3.5 = kafka-clients 3.9.x → Confluent 7.9.x 와 짝. 8.x 는 kafka-clients 4.1 을 요구한다.
val confluentVersion = "7.9.9"
val avroVersion = "1.12.2"

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.apache.avro:avro:$avroVersion")
    implementation("io.confluent:kafka-avro-serializer:$confluentVersion")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

configure<com.github.davidmc24.gradle.plugin.avro.AvroExtension> {
    stringType.set("String")   // CharSequence 대신 String getter
}

tasks.bootJar {
    archiveFileName.set("schema-registry-sample.jar")
}

tasks.jar {
    enabled = false
}

tasks.test {
    useJUnitPlatform()
    // Avro 1.12: SpecificRecord 클래스를 스키마에서 로딩할 때 신뢰 목록이 필요하다 (운영 앱은 SampleApplication.main 에서 설정)
    systemProperty("org.apache.avro.SERIALIZABLE_PACKAGES", "com.osstem")
}
