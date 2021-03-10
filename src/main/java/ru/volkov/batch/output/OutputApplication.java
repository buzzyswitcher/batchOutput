package ru.volkov.batch.output;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class OutputApplication {

    public static void main(String[] args) {
        SpringApplication.run(OutputApplication.class, args);
    }

}
