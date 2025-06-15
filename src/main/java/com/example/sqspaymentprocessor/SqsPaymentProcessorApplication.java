package com.example.sqspaymentprocessor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Aplicação principal do Spring Boot para o processador de pagamentos SQS.
 *
 * A anotação @SpringBootApplication habilita a autoconfiguração, a varredura de componentes
 * e a configuração baseada em Java.
 * A anotação @ConfigurationPropertiesScan habilita a varredura por classes anotadas com
 * @ConfigurationProperties, como a futura classe para nomes de filas (se necessária).
 */
@SpringBootApplication
@ConfigurationPropertiesScan // Habilita a busca por @ConfigurationProperties
public class SqsPaymentProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(SqsPaymentProcessorApplication.class, args);
    }

}

