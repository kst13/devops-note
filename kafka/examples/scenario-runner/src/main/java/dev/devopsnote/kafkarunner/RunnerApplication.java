package dev.devopsnote.kafkarunner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RunnerApplication {
    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(RunnerApplication.class, args)));
    }
}
