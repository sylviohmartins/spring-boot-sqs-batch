package com.example.sqspaymentprocessor.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.sqs.config.SqsMessageListenerContainerFactory;
import io.awspring.cloud.sqs.listener.ListenerMode;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/**
 * Configuração do SQS para a aplicação Spring Boot. Define beans para SqsAsyncClient, SqsTemplate e SqsMessageListenerContainerFactory. Utiliza recursos modernos do Java 21 como Virtual Threads para alta performance.
 */
@Configuration
public class SqsConfig {

  // Injeta a URL do endpoint SQS (útil para LocalStack)
  @Value("${spring.cloud.aws.sqs.endpoint:}") // Usa : para valor default vazio se não definido
  private String sqsEndpointUrl;

  @Value("${spring.cloud.aws.region.static}")
  private String awsRegion;

  @Value("${app.config.max-concurrent-messages:10}")
  private int maxConcurrentMessages;

  /**
   * Cria um pool de virtual threads para processamento de mensagens SQS. Virtual threads são ideais para operações I/O bound como processamento de pagamentos.
   *
   * @return ExecutorService baseado em virtual threads
   */
  @Bean
  public ExecutorService virtualThreadExecutor() {
    // Usando virtual threads do Java 21 para alta concorrência com baixo overhead
    return Executors.newVirtualThreadPerTaskExecutor();
  }

  /**
   * Cria um bean SqsAsyncClient. Configura o endpoint se estiver usando LocalStack.
   *
   * @return SqsAsyncClient configurado.
   */
  @Bean
  @Primary // Marca como primário para injeção automática
  public SqsAsyncClient sqsAsyncClient() {
    SqsAsyncClient builder = SqsAsyncClient.builder()
        .region(Region.of(awsRegion))
        .credentialsProvider(DefaultCredentialsProvider.create())
        .build();

    // Se a URL do endpoint SQS (LocalStack) for fornecida, configura-a
    if (sqsEndpointUrl != null && !sqsEndpointUrl.trim().isEmpty()) {
      builder.endpointOverride(URI.create(sqsEndpointUrl));
    }

    return builder;
  }

  /**
   * Cria um bean SqsTemplate para enviar mensagens SQS. Utiliza o SqsAsyncClient configurado e um MessageConverter Jackson.
   *
   * @param sqsAsyncClient   O cliente SQS assíncrono.
   * @param messageConverter O conversor de mensagens Jackson.
   * @return SqsTemplate configurado.
   */
  @Bean
  public SqsTemplate sqsTemplate(SqsAsyncClient sqsAsyncClient, MessageConverter messageConverter) {
    return SqsTemplate.builder()
        .sqsAsyncClient(sqsAsyncClient)
        .messageConverter(messageConverter) // Configura o conversor para serialização/desserialização
        .build();
  }

  /**
   * Cria um bean SqsMessageListenerContainerFactory para configurar os listeners SQS. Define o modo de acknowledgement como MANUAL para controle explícito. Define o modo de listener como BATCH para processamento em lote. Configura o MessageConverter
   * Jackson. Utiliza virtual threads para processamento concorrente.
   *
   * @param sqsAsyncClient        O cliente SQS assíncrono.
   * @param messageConverter      O conversor de mensagens Jackson.
   * @param virtualThreadExecutor Executor baseado em virtual threads para processamento concorrente.
   * @return SqsMessageListenerContainerFactory configurado.
   */
  @Bean
  public SqsMessageListenerContainerFactory<Object> defaultSqsListenerContainerFactory(
      SqsAsyncClient sqsAsyncClient,
      MessageConverter messageConverter,
      ExecutorService virtualThreadExecutor) {

    return SqsMessageListenerContainerFactory
        .builder()
        .configure(options -> options
            .acknowledgementMode(AcknowledgementMode.MANUAL) // Essencial para reprocessamento e resiliência
            .listenerMode(ListenerMode.BATCH) // Processamento em lote
            .messageConverter(messageConverter) // Garante a desserialização correta
            .maxMessagesPerPoll(10) // Pega até 10 mensagens por poll (máximo SQS)
            .pollTimeout(Duration.ofSeconds(20)) // Tempo máximo de espera por mensagens
            .maxConcurrentMessages(maxConcurrentMessages) // Configurável via propriedades
            .taskExecutor(virtualThreadExecutor) // Usa virtual threads para processamento concorrente
        )
        .sqsAsyncClient(sqsAsyncClient)
        .build();
  }

  /**
   * Configura o MessageConverter padrão para usar Jackson. Isso permite a serialização/desserialização automática de objetos Java para JSON.
   *
   * @param objectMapper O ObjectMapper configurado pelo Spring Boot.
   * @return MessageConverter configurado.
   */
  @Bean
  public MessageConverter messageConverter(ObjectMapper objectMapper) {
    MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
    converter.setObjectMapper(objectMapper);
    // Configurações adicionais do Jackson podem ser feitas aqui se necessário
    converter.setSerializedPayloadClass(String.class); // Importante para SQS que espera String
    converter.setStrictContentTypeMatch(false); // Permite flexibilidade no content-type
    return converter;
  }
}
