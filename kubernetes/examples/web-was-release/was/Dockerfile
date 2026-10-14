# jar 는 Mac(또는 Jenkins) 에서 ./gradlew bootJar 로 먼저 만든다. 이미지 빌드는 복사만 해서 가볍다.
FROM eclipse-temurin:21-jre-alpine

ARG APP_VERSION=dev
ENV APP_VERSION=${APP_VERSION}
# 컨테이너 메모리 limit 의 75% 까지만 힙으로 쓰게 해 OOMKilled 를 피한다.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0"

WORKDIR /app
COPY build/libs/app.jar app.jar

EXPOSE 8080
USER 1000
ENTRYPOINT ["java", "-jar", "app.jar"]
