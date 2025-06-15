package com.example.sqspaymentprocessor.test;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.example.sqspaymentprocessor.domain.Payment;
import com.example.sqspaymentprocessor.producer.PaymentProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
public class PaymentProducerTest {

    private static final String QUEUE_NAME = "test-payment-queue";

    @Container
    public static LocalStackContainer localStack = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:0.14.3"))
            .withServices(SQS);

    @TestConfiguration
    static class TestConfig {
        @Bean
        public String queueName() {
            return QUEUE_NAME;
        }
    }

    @Autowired
    private PaymentProducer paymentProducer;

    @Autowired
    private AmazonSQSAsync amazonSQSAsync;

    @Autowired
    private ObjectMapper objectMapper;

    private String queueUrl;

    @BeforeEach
    void setUp() {
        // Criar a fila de teste
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_NAME);
        CreateQueueResult result = amazonSQSAsync.createQueue(createQueueRequest);
        queueUrl = result.getQueueUrl();
    }

    @Test
    void testSendSinglePayment() throws ExecutionException, InterruptedException, TimeoutException {
        // Criar um pagamento de teste
        Payment payment = Payment.createNew(
                new BigDecimal("100.00"),
                "source123",
                "dest456",
                "Pagamento de teste"
        );

        // Enviar o pagamento
        CompletableFuture<Void> future = paymentProducer.sendPayment(payment);
        
        // Aguardar a conclusão do envio
        future.get(5, TimeUnit.SECONDS);

        // Verificar se a mensagem foi enviada para a fila
        int messageCount = amazonSQSAsync.getQueueAttributes(queueUrl, List.of("ApproximateNumberOfMessages"))
                .getAttributes()
                .get("ApproximateNumberOfMessages")
                .intValue();

        assertThat(messageCount).isGreaterThan(0);
    }

    @Test
    void testSendBatchPayments() throws ExecutionException, InterruptedException, TimeoutException {
        // Criar uma lista de pagamentos de teste
        List<Payment> payments = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            payments.add(Payment.createNew(
                    new BigDecimal("100.00").add(new BigDecimal(i)),
                    "source" + i,
                    "dest" + i,
                    "Pagamento de teste " + i
            ));
        }

        // Enviar os pagamentos em lote
        List<CompletableFuture<Void>> futures = paymentProducer.sendPaymentsBatch(payments);
        
        // Aguardar a conclusão de todos os envios
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);

        // Verificar se as mensagens foram enviadas para a fila
        int messageCount = amazonSQSAsync.getQueueAttributes(queueUrl, List.of("ApproximateNumberOfMessages"))
                .getAttributes()
                .get("ApproximateNumberOfMessages")
                .intValue();

        assertThat(messageCount).isGreaterThanOrEqualTo(payments.size());
    }
}
