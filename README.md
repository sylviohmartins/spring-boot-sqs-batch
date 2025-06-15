# Processador de Pagamentos com SQS Batch

Este projeto demonstra uma implementação de alta performance para processamento de milhões de pagamentos utilizando Amazon SQS com processamento em lote (SQS Batch), Java 21 e Spring Boot 3.4.5.

## Características Principais

- **Java 21**: Utiliza recursos modernos como Virtual Threads para alta concorrência com baixo overhead
- **Spring Boot 3.4.5**: Framework mais recente com suporte a Java 21
- **Spring Cloud AWS 3.3.1**: Integração otimizada com serviços AWS
- **SQS Batch Processing**: Processamento em lote para alta performance
- **Acknowledgment Manual**: Controle granular sobre confirmação de mensagens
- **Resiliência**: Mecanismos robustos para reprocessamento de mensagens com falha
- **Alta Concorrência**: Processamento paralelo de mensagens usando Virtual Threads

## Arquitetura

O sistema é composto por dois componentes principais:

1. **Produtor de Pagamentos**: Responsável por enviar pagamentos para a fila SQS, suportando envio individual ou em lote.
2. **Consumidor de Pagamentos**: Processa mensagens da fila SQS em lote, com suporte a reprocessamento e tratamento de falhas.

### Fluxo de Processamento

```
[Aplicação Cliente] → [PaymentProducer] → [Fila SQS] → [PaymentConsumer] → [Processamento de Pagamento]
```

- O `PaymentProducer` envia mensagens para a fila SQS (individualmente ou em lote)
- O `PaymentConsumer` recebe mensagens em lote da fila SQS
- Cada mensagem é processada concorrentemente usando Virtual Threads
- Mensagens processadas com sucesso são confirmadas (acknowledged)
- Mensagens com falha não são confirmadas e voltam para a fila para reprocessamento
- Após um número configurável de tentativas, mensagens com falha persistente podem ser enviadas para uma Dead Letter Queue (DLQ)

## Configuração

### Requisitos

- Java 21 ou superior
- Maven 3.8+ ou Gradle 8.0+
- AWS CLI configurado ou LocalStack para desenvolvimento local

### Propriedades de Configuração

As principais configurações estão no arquivo `application.yml`:

```yaml
spring:
  cloud:
    aws:
      region:
        static: us-east-1
      sqs:
        endpoint: http://localhost:4566  # Para LocalStack (desenvolvimento)
      credentials:
        use-default-aws-credentials-chain: true

app:
  config:
    payment-queue-name: payment-queue
    payment-queue-url: http://localhost:4566/000000000000/payment-queue
    max-concurrent-messages: 20  # Concorrência máxima
    max-retries: 3  # Tentativas de reprocessamento
    visibility-timeout: 30  # Tempo de invisibilidade (segundos)
```

### Configuração para Ambiente de Produção

Para ambientes de produção, ajuste as seguintes configurações:

1. Remova o endpoint SQS (para usar o serviço AWS real)
2. Configure credenciais AWS apropriadas
3. Ajuste `max-concurrent-messages` com base na capacidade do seu sistema
4. Configure uma Dead Letter Queue (DLQ) para mensagens com falha persistente
5. Ajuste `visibility-timeout` com base no tempo médio de processamento

## Uso

### Envio de Pagamentos

Para enviar um pagamento individual:

```java
Payment payment = Payment.createNew(
    new BigDecimal("100.00"),
    "conta-origem-123",
    "conta-destino-456",
    "Pagamento de fatura"
);

paymentProducer.sendPayment(payment)
    .thenAccept(result -> log.info("Pagamento enviado: {}", result.messageId()));
```

Para enviar pagamentos em lote:

```java
List<Payment> payments = List.of(
    Payment.createNew(new BigDecimal("100.00"), "origem1", "destino1", "Pagamento 1"),
    Payment.createNew(new BigDecimal("200.00"), "origem2", "destino2", "Pagamento 2")
);

paymentProducer.sendPaymentsBatch(payments)
    .forEach(future -> future.thenAccept(result -> 
        log.info("Lote enviado: {} mensagens", result.successful().size())));
```

### Processamento de Pagamentos

O processamento é configurado automaticamente através do `@SqsListener` no `PaymentConsumer`. Para personalizar o processamento, modifique o método `processPayment` na classe `PaymentConsumer`.

## Otimizações para Alta Performance

### Virtual Threads (Java 21)

O projeto utiliza Virtual Threads do Java 21 para processamento concorrente de mensagens, permitindo alta escalabilidade com baixo overhead:

```java
@Bean
public ExecutorService virtualThreadExecutor() {
    return Executors.newVirtualThreadPerTaskExecutor();
}
```

### Processamento em Lote

O SQS Listener é configurado para receber e processar mensagens em lote:

```java
@SqsListener(queueNames = "${app.config.payment-queue-name}", factory = "defaultSqsListenerContainerFactory")
public CompletableFuture<Void> receivePaymentMessages(List<Message<Payment>> messages, Acknowledgement acknowledgement) {
    // Processamento em lote
}
```

### Acknowledgment Manual

O sistema utiliza acknowledgment manual para garantir que apenas mensagens processadas com sucesso sejam removidas da fila:

```java
acknowledgement.acknowledge(successfulMessages).join();
```

### Rastreamento de Tentativas

O sistema rastreia tentativas de processamento para implementar políticas de retry e eventual envio para DLQ:

```java
private final ConcurrentHashMap<String, Integer> retryTracker = new ConcurrentHashMap<>();
```

## Testes

O projeto inclui testes unitários e de integração para validar o funcionamento do sistema:

- **Testes Unitários**: Validam o comportamento do produtor e consumidor isoladamente
- **Testes de Integração**: Validam o fluxo completo usando LocalStack para simular o SQS

Para executar os testes:

```bash
mvn test
```

## Execução Local

Para executar o projeto localmente com LocalStack:

1. Inicie o LocalStack:
   ```bash
   docker run -d -p 4566:4566 -e SERVICES=sqs localstack/localstack
   ```

2. Crie a fila SQS:
   ```bash
   aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name payment-queue
   ```

3. Execute a aplicação:
   ```bash
   mvn spring-boot:run
   ```

## Monitoramento e Observabilidade

O projeto está configurado com Spring Boot Actuator para monitoramento. Endpoints disponíveis:

- `/actuator/health`: Status da aplicação
- `/actuator/metrics`: Métricas da aplicação

Para ambientes de produção, recomenda-se integrar com:

- AWS CloudWatch para métricas do SQS
- Prometheus/Grafana para métricas da aplicação
- Distributed tracing com Zipkin ou AWS X-Ray

## Recomendações para Produção

1. **Ajuste de Concorrência**: Configure `max-concurrent-messages` com base na capacidade do seu sistema e requisitos de throughput
2. **Dead Letter Queue**: Configure uma DLQ para mensagens que falham repetidamente
3. **Visibility Timeout**: Ajuste com base no tempo médio de processamento mais uma margem de segurança
4. **Monitoramento**: Implemente alertas para filas com muitas mensagens ou alta taxa de falhas
5. **Escalabilidade**: Para volumes extremamente altos, considere múltiplas instâncias da aplicação

## Referências

- [Documentação do Spring Cloud AWS](https://docs.awspring.io/spring-cloud-aws/docs/3.3.1/reference/html/index.html)
- [Amazon SQS Developer Guide](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html)
- [Java 21 Virtual Threads](https://docs.oracle.com/en/java/javase/21/core/virtual-threads.html)
