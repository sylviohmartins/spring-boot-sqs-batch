package com.example.sqspaymentprocessor.test;

import com.example.sqspaymentprocessor.domain.Payment;
import com.example.sqspaymentprocessor.producer.PaymentProducer;
import io.awspring.cloud.sqs.operations.SendResult;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.Message;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class PaymentProducerUnitTest {

    @Mock
    private SqsTemplate sqsTemplate;

    private PaymentProducer paymentProducer;

    private final String QUEUE_URL = "http://localhost:4566/000000000000/payment-queue";

    @BeforeEach
    void setUp() {
        paymentProducer = new PaymentProducer(sqsTemplate, QUEUE_URL, new com.fasterxml.jackson.databind.ObjectMapper());
    }

    @Test
    void testSendSinglePayment() {
        // Arrange
        Payment payment = Payment.createNew(
                new BigDecimal("100.00"),
                "source123",
                "dest456",
                "Pagamento de teste"
        );

        SendResult<Payment> mockResult = mock(SendResult.class);
        when(mockResult.messageId()).thenReturn("test-message-id");
        when(sqsTemplate.sendAsync(anyString(), any(Payment.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResult));

        // Act
        CompletableFuture<SendResult<Payment>> future = paymentProducer.sendPayment(payment);
        SendResult<Payment> result = future.join();

        // Assert
        verify(sqsTemplate).sendAsync(eq(QUEUE_URL), eq(payment));
        assertThat(result).isNotNull();
        assertThat(result.messageId()).isEqualTo("test-message-id");
    }

    @Test
    void testSendBatchPayments() {
        // Arrange
        List<Payment> payments = List.of(
                Payment.createNew(new BigDecimal("100.00"), "source1", "dest1", "Pagamento 1"),
                Payment.createNew(new BigDecimal("200.00"), "source2", "dest2", "Pagamento 2"),
                Payment.createNew(new BigDecimal("300.00"), "source3", "dest3", "Pagamento 3")
        );

        SendResult<Object> mockResult = mock(SendResult.class);
        when(mockResult.successful()).thenReturn(List.of());
        when(mockResult.failed()).thenReturn(List.of());
        when(sqsTemplate.sendManyAsync(anyString(), anyList()))
                .thenReturn(CompletableFuture.completedFuture(mockResult));

        // Act
        List<CompletableFuture<SendResult<Object>>> futures = paymentProducer.sendPaymentsBatch(payments);
        SendResult<Object> result = futures.get(0).join();

        // Assert
        ArgumentCaptor<List<Message<Payment>>> messagesCaptor = ArgumentCaptor.forClass(List.class);
        verify(sqsTemplate).sendManyAsync(eq(QUEUE_URL), messagesCaptor.capture());
        
        List<Message<Payment>> capturedMessages = messagesCaptor.getValue();
        assertThat(capturedMessages).hasSize(3);
        assertThat(capturedMessages.get(0).getPayload().getAmount()).isEqualTo(new BigDecimal("100.00"));
        assertThat(capturedMessages.get(1).getPayload().getAmount()).isEqualTo(new BigDecimal("200.00"));
        assertThat(capturedMessages.get(2).getPayload().getAmount()).isEqualTo(new BigDecimal("300.00"));
    }
}
