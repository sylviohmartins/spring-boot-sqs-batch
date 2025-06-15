package com.example.sqspaymentprocessor.producer;

import com.example.sqspaymentprocessor.domain.Payment;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.sqs.operations.SendResult;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Componente responsável por enviar mensagens de pagamento para a fila SQS.
 * Implementa envio individual e em lote com suporte a alta performance.
 */
@Component
public class PaymentProducer {

    private static final Logger log = LoggerFactory.getLogger(PaymentProducer.class);

    private final SqsTemplate sqsTemplate;
    private final String queueUrl;
    private final ObjectMapper objectMapper; // Para logs, SqsTemplate usa seu próprio MessageConverter

    @Autowired
    public PaymentProducer(SqsTemplate sqsTemplate,
                           @Value("${app.config.payment-queue-url}") String queueUrl,
                           ObjectMapper objectMapper) {
        this.sqsTemplate = sqsTemplate;
        this.queueUrl = queueUrl;
        this.objectMapper = objectMapper;
    }

    /**
     * Envia um único pagamento para a fila SQS.
     *
     * @param payment O objeto de pagamento a ser enviado.
     * @return Um CompletableFuture com o resultado do envio.
     */
    public CompletableFuture<SendResult<Payment>> sendPayment(Payment payment) {
        log.info("Enviando pagamento único: {}", payment.getId());
        // SqsTemplate usará o MessageConverter configurado para serializar o Payment
        return sqsTemplate.sendAsync(queueUrl, payment)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Erro ao enviar pagamento {}: {}", payment.getId(), ex.getMessage(), ex);
                    } else {
                        log.info("Pagamento {} enviado com sucesso. MessageId: {}", payment.getId(), result.messageId());
                    }
                });
    }

    /**
     * Envia uma lista de pagamentos em lote para a fila SQS.
     * O SQS suporta envio em lote de até 10 mensagens por chamada.
     * Este método divide lotes maiores em chamadas de 10.
     *
     * @param payments A coleção de pagamentos a ser enviada.
     * @return Uma lista de CompletableFuture, um para cada lote de envio.
     */
    public List<CompletableFuture<SendResult<Object>>> sendPaymentsBatch(Collection<Payment> payments) {
        if (payments == null || payments.isEmpty()) {
            return List.of();
        }

        log.info("Enviando {} pagamentos em lote...", payments.size());

        // Converte pagamentos em mensagens Spring
        List<Message<Payment>> messages = payments.stream()
                .map(payment -> MessageBuilder.withPayload(payment)
                        // Adiciona um ID único para cada mensagem no lote (opcional, mas útil para rastreio)
                        .setHeader("message-group-id", "payment-batch-" + UUID.randomUUID()) // Exemplo para FIFO, pode ser adaptado
                        .setHeader("message-deduplication-id", UUID.randomUUID().toString()) // Exemplo para FIFO
                        .build())
                .collect(Collectors.toList());

        // SqsTemplate lida com o envio em lote (até 10 por vez automaticamente se necessário)
        // O método sendManyAsync retorna um CompletableFuture para o lote inteiro.
        // Se precisar de futuros individuais por lote de 10, a lógica de divisão manual seria necessária aqui.
        // Para simplificar, usamos sendManyAsync que abstrai isso.
        CompletableFuture<SendResult<Object>> batchFuture = sqsTemplate.sendManyAsync(queueUrl, messages);

        batchFuture.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Erro ao enviar lote de pagamentos: {}", ex.getMessage(), ex);
            } else {
                log.info("Lote de {} pagamentos enviado com sucesso. Successful: {}, Failed: {}",
                        payments.size(),
                        result.successful().size(),
                        result.failed().size());
                result.failed().forEach(failed ->
                        log.warn("Falha ao enviar mensagem ID {}: {}", failed.message().getHeaders().getId(), failed.exception().getMessage()));
            }
        });

        // Retorna uma lista contendo o futuro do lote único (ou múltiplos futuros se a divisão manual fosse feita)
        return List.of(batchFuture);
    }

    // Método auxiliar para log (não usado diretamente pelo SqsTemplate)
    private String toJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            log.warn("Erro ao serializar objeto para log: {}", e.getMessage());
            return object.toString();
        }
    }
}
