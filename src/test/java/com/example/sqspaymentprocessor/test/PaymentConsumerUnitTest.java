package com.example.sqspaymentprocessor.test;

import com.example.sqspaymentprocessor.consumer.PaymentConsumer;
import com.example.sqspaymentprocessor.domain.Payment;
import io.awspring.cloud.sqs.listener.acknowledgement.Acknowledgement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class PaymentConsumerUnitTest {

    @Mock
    private Acknowledgement acknowledgement;

    private PaymentConsumer paymentConsumer;

    @BeforeEach
    void setUp() {
        paymentConsumer = new PaymentConsumer();
        ReflectionTestUtils.setField(paymentConsumer, "maxRetries", 3);
    }

    @Test
    void testProcessSuccessfulMessages() throws ExecutionException, InterruptedException, TimeoutException {
        // Arrange
        List<Message<Payment>> messages = List.of(
                createPaymentMessage("100.00", "source1", "dest1", "Pagamento 1"),
                createPaymentMessage("200.00", "source2", "dest2", "Pagamento 2")
        );

        when(acknowledgement.acknowledge(anyList())).thenReturn(CompletableFuture.completedFuture(null));

        // Modificar o método processPayment para não lançar exceções durante o teste
        ReflectionTestUtils.setField(paymentConsumer, "maxRetries", 0); // Desabilitar falhas simuladas

        // Act
        CompletableFuture<Void> future = paymentConsumer.receivePaymentMessages(messages, acknowledgement);
        future.get(5, TimeUnit.SECONDS); // Esperar a conclusão

        // Assert
        verify(acknowledgement).acknowledge(anyList());
    }

    @Test
    void testProcessMixedSuccessAndFailureMessages() throws ExecutionException, InterruptedException, TimeoutException {
        // Arrange
        List<Message<Payment>> messages = List.of(
                createPaymentMessage("100.00", "source1", "dest1", "Pagamento 1"),
                createPaymentMessage("200.00", "source2", "dest2", "Pagamento 2"),
                createPaymentMessage("300.00", "source3", "dest3", "Pagamento 3")
        );

        when(acknowledgement.acknowledge(anyList())).thenReturn(CompletableFuture.completedFuture(null));

        // Configurar o método processPayment para falhar em mensagens específicas
        // Isso é feito através de um spy que sobrescreve o comportamento do método
        PaymentConsumer spyConsumer = spy(paymentConsumer);
        doAnswer(invocation -> {
            Payment payment = invocation.getArgument(0);
            if (payment.getSourceAccountId().equals("source2")) {
                throw new RuntimeException("Falha simulada para teste");
            }
            return null;
        }).when(spyConsumer).processPayment(any(Payment.class));

        // Act
        CompletableFuture<Void> future = spyConsumer.receivePaymentMessages(messages, acknowledgement);
        future.get(5, TimeUnit.SECONDS); // Esperar a conclusão

        // Assert - Verificar que apenas mensagens bem-sucedidas são reconhecidas
        verify(acknowledgement).acknowledge(argThat(list -> list.size() == 2));
    }

    private Message<Payment> createPaymentMessage(String amount, String sourceId, String destId, String description) {
        Payment payment = Payment.createNew(
                new BigDecimal(amount),
                sourceId,
                destId,
                description
        );
        
        return MessageBuilder
                .withPayload(payment)
                .setHeader("MessageId", UUID.randomUUID().toString())
                .setHeader("ReceiptHandle", UUID.randomUUID().toString())
                .build();
    }
}
