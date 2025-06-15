package com.example.sqspaymentprocessor.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Classe que representa um pagamento a ser processado.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Payment {

    /**
     * Identificador único do pagamento.
     */
    private String id;

    /**
     * Valor do pagamento.
     */
    private BigDecimal amount;

    /**
     * Descrição ou motivo do pagamento.
     */
    private String description;

    /**
     * Identificador da conta de origem.
     */
    private String sourceAccountId;

    /**
     * Identificador da conta de destino.
     */
    private String destinationAccountId;

    /**
     * Data e hora de criação do pagamento.
     */
    private LocalDateTime createdAt;

    /**
     * Status atual do pagamento.
     */
    private PaymentStatus status;

    /**
     * Método de fábrica para criar um novo pagamento com valores padrão.
     *
     * @param amount Valor do pagamento
     * @param sourceAccountId ID da conta de origem
     * @param destinationAccountId ID da conta de destino
     * @param description Descrição do pagamento
     * @return Um novo objeto Payment
     */
    public static Payment createNew(BigDecimal amount, String sourceAccountId, 
                                   String destinationAccountId, String description) {
        return Payment.builder()
                .id(UUID.randomUUID().toString())
                .amount(amount)
                .sourceAccountId(sourceAccountId)
                .destinationAccountId(destinationAccountId)
                .description(description)
                .createdAt(LocalDateTime.now())
                .status(PaymentStatus.PENDING)
                .build();
    }

    /**
     * Enum que representa os possíveis status de um pagamento.
     */
    public enum PaymentStatus {
        PENDING,
        PROCESSING,
        COMPLETED,
        FAILED,
        CANCELLED
    }
}
