package com.example.sqspaymentprocessor;

import com.example.sqspaymentprocessor.config.LocalStackConfig;
import com.example.sqspaymentprocessor.consumer.PaymentConsumer;
import com.example.sqspaymentprocessor.domain.Payment;
import com.example.sqspaymentprocessor.producer.PaymentProducer;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

/**
 * Testes de integração para a aplicação SqsPaymentProcessor.
 *
 * Utiliza @SpringBootTest para carregar o contexto completo da aplicação.
 * Utiliza @ActiveProfiles("test") para carregar as configurações de application-test.yml.
 * Utiliza @Import(LocalStackConfig.class) para iniciar o container LocalStack via Testcontainers.
 * Utiliza @SpyBean para espionar o PaymentConsumer e verificar se o método de recebimento foi chamado.
 */
@SpringBootTest
@ActiveProfiles("test")
@Import(LocalStackConfig.class)
class SqsPaymentProcessorApplicationTests {

    private static final Logger log = LoggerFactory.getLogger(SqsPaymentProcessorApplicationTests.class);

    @Autowired
    private PaymentProducer paymentProducer;

    // Espiona o bean real do consumidor para verificar interações
    @SpyBean
    private PaymentConsumer paymentConsumer;

    @Autowired
    private SqsAsyncClient sqsAsyncClient; // Para interagir com SQS diretamente se necessário

    @Autowired
    private SqsTemplate sqsTemplate; // Para obter URL da fila

    @Value("${app.queues.payment-processing}")
    private String paymentQueueName;

    private String paymentQueueUrl;

    @BeforeEach
    void setUp() throws Exception {
        // Obtém a URL da fila dinamicamente (importante com LocalStack)
        paymentQueueUrl = sqsTemplate.getQueueUrl(paymentQueueName).get(5, TimeUnit.SECONDS);
        log.info("URL da fila de teste obtida: {}", paymentQueueUrl);

        // Garante que a fila esteja vazia antes de cada teste (opcional, mas bom para isolamento)
        // Nota: Purge pode levar até 60 segundos para ter efeito completo.
        // sqsAsyncClient.purgeQueue(req -> req.queueUrl(paymentQueueUrl)).get(10, TimeUnit.SECONDS);
        // log.info("Fila {} purgada antes do teste.", paymentQueueName);
        // Thread.sleep(1000); // Pequena pausa após purgar

        // Reseta o spy antes de cada teste
        reset(paymentConsumer);
    }

    @Test
    void contextLoads() {
        // Teste simples para verificar se o contexto da aplicação carrega corretamente
        assertThat(paymentProducer).isNotNull();
        assertThat(paymentConsumer).isNotNull();
        assertThat(sqsAsyncClient).isNotNull();
        assertThat(paymentQueueUrl).isNotNull();
    }

    @Test
    void shouldSendAndReceivePaymentBatch() throws Exception {
        // Arrange: Cria um lote de pagamentos
        int batchSize = 15; // Testar com um lote maior que 10 para verificar o tratamento do SqsTemplate
        List<Payment> payments = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            payments.add(Payment.createNew(
                    BigDecimal.valueOf(100.0 + i),
                    "source-" + i,
                    "dest-" + i,
                    "Pagamento " + i
            ));
        }

        // Act: Envia o lote de pagamentos
        CompletableFuture<?> sendFuture = paymentProducer.sendPaymentsBatch(payments);

        // Aguarda a conclusão do envio (opcional, mas bom para garantir que foi enviado)
        sendFuture.get(10, TimeUnit.SECONDS);
        log.info("Lote de {} pagamentos enviado para a fila {}.
", batchSize, paymentQueueName);

        // Assert: Verifica se o consumidor processou as mensagens
        // Usamos Awaitility para esperar que o método receivePaymentMessages seja chamado
        // com um lote contendo o número esperado de mensagens.
        // O SqsTemplate divide lotes > 10, então esperamos múltiplas chamadas ao listener.
        // Verificamos se foi chamado pelo menos N vezes, onde N = ceil(batchSize / maxMessagesPerPoll)
        int expectedListenerInvocations = (int) Math.ceil((double) batchSize / 10.0); // 10 é o maxMessagesPerPoll padrão
        log.info("Esperando pelo menos {} invocações do listener...", expectedListenerInvocations);

        await().atMost(Duration.ofSeconds(30)) // Timeout generoso para SQS e processamento simulado
               .pollInterval(Duration.ofSeconds(1))
               .untilAsserted(() -> verify(paymentConsumer, atLeast(expectedListenerInvocations))
                       .receivePaymentMessages(anyList(), any()));

        log.info("Listener invocado pelo menos {} vezes como esperado.", expectedListenerInvocations);

        // Verificação adicional (opcional): Checar se a fila está vazia após o processamento
        // Pode ser instável devido a latências do SQS e long polling
        await().atMost(Duration.ofSeconds(15))
               .pollInterval(Duration.ofSeconds(2))
               .untilAsserted(() -> {
                   Map<QueueAttributeName, String> attributes = sqsAsyncClient.getQueueAttributes(req -> req
                           .queueUrl(paymentQueueUrl)
                           .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                                           QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)
                   ).get(5, TimeUnit.SECONDS).attributes();

                   long visibleMessages = Long.parseLong(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
                   long notVisibleMessages = Long.parseLong(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE));
                   log.debug("Verificando atributos da fila: Visíveis={}, Não Visíveis={}", visibleMessages, notVisibleMessages);
                   assertThat(visibleMessages).as("Mensagens visíveis na fila").isEqualTo(0);
                   assertThat(notVisibleMessages).as("Mensagens não visíveis na fila").isEqualTo(0);
               });
        log.info("Fila {} está vazia após o processamento.", paymentQueueName);
    }

    // TODO: Adicionar testes para cenários de erro (e.g., falha no processamento de uma mensagem no lote)
    // Isso exigiria modificar o consumidor para não lançar exceção imediatamente ou usar DLQ.

    // TODO: Adicionar testes unitários para a lógica de negócio dentro do consumidor (se houver)
    // mockando dependências externas.
}

