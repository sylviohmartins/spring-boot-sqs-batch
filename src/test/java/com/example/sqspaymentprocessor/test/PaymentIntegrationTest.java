package com.example.sqspaymentprocessor.test;

import com.example.sqspaymentprocessor.domain.Payment;
import com.example.sqspaymentprocessor.producer.PaymentProducer;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
public class PaymentIntegrationTest {

    private static final String QUEUE_NAME = "test-payment-queue";

    @Container
    public static LocalStackContainer localStack = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:latest"))
            .withServices(SQS);

    @TestConfiguration
    static class TestConfig {
        @Bean
        public SqsAsyncClient sqsAsyncClient() {
            return SqsAsyncClient.builder()
                    .endpointOverride(URI.create(localStack.getEndpointOverride(SQS).toString()))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("test", "test")))
                    .region(Region.of(localStack.getRegion()))
                    .build();
        }

        @Bean
        public String queueName() {
            return QUEUE_NAME;
        }
    }

    @Autowired
    private SqsAsyncClient sqsAsyncClient;

    @Autowired
    private SqsTemplate sqsTemplate;

    @Autowired
    private PaymentProducer paymentProducer;

    private String queueUrl;

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        // Criar a fila de teste
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(QUEUE_NAME)
                .build();
        queueUrl = sqsAsyncClient.createQueue(createQueueRequest).get().queueUrl();
    }

    @Test
    void testSendAndReceiveBatchMessages() throws ExecutionException, InterruptedException, TimeoutException {
        // Arrange
        List<Payment> payments = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            payments.add(Payment.createNew(
                    new BigDecimal("100.00").add(new BigDecimal(i)),
                    "source" + i,
                    "dest" + i,
                    "Pagamento de teste " + i
            ));
        }

        // Act - Enviar pagamentos em lote
        List<CompletableFuture<?>> futures = paymentProducer.sendPaymentsBatch(payments);
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);

        // Verificar se as mensagens foram enviadas para a fila
        GetQueueAttributesRequest attributesRequest = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                .build();

        // Aguardar um pouco para garantir que as mensagens estejam disponíveis
        TimeUnit.SECONDS.sleep(2);

        Map<QueueAttributeName, String> attributes = sqsAsyncClient.getQueueAttributes(attributesRequest)
                .get(5, TimeUnit.SECONDS)
                .attributes();

        int messageCount = Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));

        // Assert
        assertThat(messageCount).isGreaterThanOrEqualTo(payments.size());
    }

    @Test
    void testHighVolumeMessageProcessing() throws ExecutionException, InterruptedException, TimeoutException {
        // Arrange - Criar um grande volume de mensagens
        List<Payment> payments = new ArrayList<>();
        for (int i = 0; i < 100; i++) { // 100 mensagens para testar volume
            payments.add(Payment.createNew(
                    new BigDecimal("100.00").add(new BigDecimal(i)),
                    "source" + i,
                    "dest" + i,
                    "Pagamento de teste de alto volume " + i
            ));
        }

        // Act - Enviar pagamentos em lote (serão divididos automaticamente em lotes de 10)
        long startTime = System.currentTimeMillis();
        List<CompletableFuture<?>> futures = paymentProducer.sendPaymentsBatch(payments);
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
        long endTime = System.currentTimeMillis();

        // Assert - Verificar que o processamento foi concluído em tempo razoável
        long processingTime = endTime - startTime;
        System.out.println("Tempo de processamento para 100 mensagens: " + processingTime + "ms");
        
        // O tempo exato dependerá do ambiente, mas queremos garantir que seja rápido
        // devido ao uso de virtual threads e processamento concorrente
        assertThat(processingTime).isLessThan(10000); // Menos de 10 segundos para 100 mensagens
    }
}
