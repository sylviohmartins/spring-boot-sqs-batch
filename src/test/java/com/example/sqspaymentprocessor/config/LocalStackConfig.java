package com.example.sqspaymentprocessor.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Configuração de Teste para levantar um container LocalStack usando Testcontainers.
 *
 * A anotação @TestConfiguration indica que esta classe contém beans de configuração
 * específicos para testes.
 * A anotação @ServiceConnection (do Spring Boot Testcontainers) configura automaticamente
 * a conexão com o serviço (neste caso, SQS via LocalStack) para a aplicação Spring Boot
 * durante os testes, definindo propriedades como o endpoint do SQS.
 */
@TestConfiguration(proxyBeanMethods = false)
public class LocalStackConfig {

    private static final Logger log = LoggerFactory.getLogger(LocalStackConfig.class);
    // Define a imagem e versão do LocalStack a ser usada.
    // É uma boa prática fixar a versão para garantir a consistência dos testes.
    private static final DockerImageName LOCALSTACK_IMAGE = DockerImageName.parse("localstack/localstack:3.4.0");

    /**
     * Cria e configura o bean do container LocalStack.
     *
     * @return Uma instância do LocalStackContainer gerenciada pelo Testcontainers.
     */
    @Bean
    @ServiceConnection // Conecta automaticamente o Spring Boot a este container
    public LocalStackContainer localStackContainer() {
        log.info("Criando container LocalStack com a imagem: {}", LOCALSTACK_IMAGE);
        // Habilita apenas o serviço SQS para otimizar o tempo de inicialização.
        // Outros serviços podem ser habilitados conforme necessário.
        return new LocalStackContainer(LOCALSTACK_IMAGE)
                .withServices(LocalStackContainer.Service.SQS)
                // .withReuse(true) // Descomente para reutilizar o container entre execuções de teste (acelera)
                ; 
    }

    // Nota: Não é mais necessário criar manualmente as filas SQS nos testes
    // usando SqsAsyncClient. O Spring Cloud AWS 3.x com @SqsListener
    // tentará criar as filas automaticamente ao iniciar o listener se elas não existirem.
    // Isso funciona bem com LocalStack. Em produção, a aplicação não deve ter permissão
    // para criar filas, e elas devem existir previamente.
}

