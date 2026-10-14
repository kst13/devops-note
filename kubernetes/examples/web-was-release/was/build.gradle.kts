// DB 없이 JSON 만 돌려주는 최소 WAS. 버전은 빌드 스크립트가 -PappVersion 으로 넘긴다.
plugins {
    java
    id("org.springframework.boot") version "3.5.5"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.example"
version = (project.findProperty("appVersion") as String?) ?: "0.0.0-local"

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
    implementation("org.springframework.boot:spring-boot-starter-actuator")   // /actuator/health → readiness·liveness probe
}

// Dockerfile 이 COPY 할 파일명을 고정한다 (버전이 바뀌어도 app.jar).
tasks.bootJar {
    archiveFileName.set("app.jar")
}
