package com.finstream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entrypoint for FinStream Real-Time Financial Transaction Monitoring Consumer.
 */
@SpringBootApplication
public class FinStreamApplication {

    public static void main(String[] args) {
        SpringApplication.run(FinStreamApplication.class, args);
    }
}
