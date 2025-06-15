package com.example.sqspaymentprocessor.consumer;

import com.example.sqspaymentprocessor.domain.Payment;
import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.listener.acknowledgement.Acknowledgement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumidor SQS responsável por processar mensagens de pagamento em lote.
 * Implementa processamento concorrente com virtual threads e mecanismos de resiliência.
 */
@Component
public class PaymentConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);
    
    // Contador para monitoramento de mensagens processadas
    private final AtomicInteger processedCounter = new AtomicInteger(0);
    private final AtomicInteger failedCounter = new AtomicInteger(0);
    
    // Cache para rastreamento de tentativas de reprocessamento
    private final ConcurrentHashMap<String, Integer> retryTracker = new ConcurrentHashMap<>();
    
    @Value("${app.config.max-retries:3}")
    private int maxRetries;

    /**
     * Listener para a fila de pagamentos, configurado para processamento em lote e acknowledgement manual.
     * Usa virtual threads para processamento concorrente de alto volume.
     *
     * @param messages Lista de mensagens recebidas no lote.
     * @param acknowledgement Objeto para realizar o acknowledgement manual das mensagens.
     * @return Um CompletableFuture que sinaliza a conclusão do processamento do lote.
     */
    @SqsListener(queueNames = "${app.config.payment-queue-name}", factory = "defaultSqsListenerContainerFactory")
    public CompletableFuture<Void> receivePaymentMessages(List<Message<Payment>> messages, Acknowledgement acknowledgement) {
        log.info("Recebido lote de {} mensagens para processamento.", messages.size());

        // Lista para armazenar os futures do processamento de cada mensagem
        List<CompletableFuture<ProcessingResult>> processingFutures = new ArrayList<>();

        // Processa cada mensagem do lote de forma concorrente usando virtual threads
        for (Message<Payment> message : messages) {
            CompletableFuture<ProcessingResult> future = CompletableFuture.supplyAsync(() -> {
                Payment payment = message.getPayload();
                String messageId = message.getHeaders().getId() != null ? message.getHeaders().getId().toString() : "N/A";
                
                // Verifica se a mensagem já foi processada anteriormente e falhou
                int retryCount = retryTracker.getOrDefault(payment.getId(), 0);
                
                log.debug("Processando pagamento ID: {}, Message ID: {}, Tentativa: {}", 
                        payment.getId(), messageId, retryCount + 1);
                
                try {
                    // Simula o processamento do pagamento (ex: salvar no banco, chamar outro serviço)
                    processPayment(payment);
                    
                    // Remove do rastreador de tentativas se processado com sucesso
                    retryTracker.remove(payment.getId());
                    
                    processedCounter.incrementAndGet();
                    log.debug("Pagamento ID: {} processado com sucesso.", payment.getId());
                    
                    return new ProcessingResult(message, true);
                } catch (Exception e) {
                    // Incrementa o contador de tentativas
                    retryTracker.put(payment.getId(), retryCount + 1);
                    
                    failedCounter.incrementAndGet();
                    log.error("Erro ao processar pagamento ID: {}, Message ID: {}, Tentativa: {}. Erro: {}",
                            payment.getId(), messageId, retryCount + 1, e.getMessage(), e);
                    
                    // Verifica se excedeu o número máximo de tentativas
                    if (retryCount >= maxRetries) {
                        log.warn("Pagamento ID: {} excedeu o número máximo de tentativas ({}). Enviando para DLQ.", 
                                payment.getId(), maxRetries);
                        // Em um cenário real, aqui poderia enviar para uma DLQ ou registrar em um sistema de fallback
                    }
                    
                    return new ProcessingResult(message, false);
                }
            });

            processingFutures.add(future);
        }

        // Combina todos os futuros de processamento
        return CompletableFuture.allOf(processingFutures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                // Coleta os resultados
                List<Message<?>> successfulMessages = new ArrayList<>();
                List<Message<?>> failedMessages = new ArrayList<>();

                processingFutures.forEach(future -> {
                    try {
                        ProcessingResult result = future.join();
                        if (result.success()) {
                            successfulMessages.add(result.message());
                        } else {
                            failedMessages.add(result.message());
                        }
                    } catch (Exception e) {
                        log.error("Erro ao obter resultado do processamento: {}", e.getMessage(), e);
                    }
                });

                log.info("Processamento do lote concluído. Sucesso: {}, Falha: {}", 
                        successfulMessages.size(), failedMessages.size());

                // Realiza o acknowledgement APENAS das mensagens processadas com sucesso.
                if (!successfulMessages.isEmpty()) {
                    log.info("Realizando acknowledgement de {} mensagens processadas com sucesso.", successfulMessages.size());
                    acknowledgement.acknowledge(successfulMessages).join();
                    log.info("Acknowledgement concluído.");
                }

                if (!failedMessages.isEmpty()) {
                    log.warn("{} mensagens falharam e retornarão à fila após timeout.", failedMessages.size());
                }

                return null;
            });
    }

    /**
     * Simula a lógica de processamento de um pagamento.
     * Substituir pela implementação real (ex: chamada de serviço, acesso a banco de dados).
     *
     * @param payment O pagamento a ser processado.
     * @throws InterruptedException Se a thread for interrompida durante a simulação.
     */
    private void processPayment(Payment payment) throws InterruptedException {
        // Simulação de trabalho (ex: I/O bound)
        Thread.sleep(50); // Simula 50ms de processamento

        // Simular falha aleatória para teste de resiliência (remover em produção)
        if (Math.random() < 0.1) { // 10% de chance de falha
            throw new RuntimeException("Falha simulada no processamento do pagamento " + payment.getId());
        }
    }
    
    /**
     * Classe interna para representar o resultado do processamento de uma mensagem.
     */
    private record ProcessingResult(Message<?> message, boolean success) {}
}
