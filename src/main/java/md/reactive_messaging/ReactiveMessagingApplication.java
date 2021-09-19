package md.reactive_messaging;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.apps.JmsSyncSender;
import md.reactive_messaging.functional.throwing.ThrowingFunction;
import md.reactive_messaging.jms.JmsSimplifiedApiManager;
import md.reactive_messaging.jms.MessageConverters;
import md.reactive_messaging.jms.MessageExtract;
import md.reactive_messaging.reactive.ReactiveOps;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import reactor.core.publisher.Flux;

import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import java.time.Duration;
import java.util.Properties;

import static java.time.Duration.ofSeconds;
import static md.reactive_messaging.Profiles.JMS_SYNC_RECEIVER;
import static md.reactive_messaging.Profiles.JMS_SYNC_SENDER;
import static md.reactive_messaging.reactive.GenericSubscribers.FluxSubscribers.subscribeAndAwait;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@SpringBootApplication
@Slf4j
public class ReactiveMessagingApplication
{
    public static void main(String[] args)
    {
        SpringApplication.run(ReactiveMessagingApplication.class, args);
    }

    @Bean
    @Qualifier("app-runner")
    TaskExecutor appRunner()
    {
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(4);
        executor.setThreadNamePrefix("app-runner-");
        return executor;
    }

    @Profile(JMS_SYNC_SENDER)
    @Bean
    ApplicationRunner jmsSyncSender
            (
                    @Qualifier("app-runner") TaskExecutor taskExecutor,
                    JmsSimplifiedApiManager manager,
                    ThrowingFunction<String, ConnectionFactory, JMSException> connectionFactory,
                    @Qualifier("url") String url,
                    @Qualifier("user-name") String userName,
                    @Qualifier("password") String password,
                    @Qualifier("queue-name") String queueName
            )
    {
        return args ->
                taskExecutor.execute(
                        JmsSyncSender.builder().
                                manager(manager).
                                connectionFactory(connectionFactory).url(url).
                                userName(userName).password(password).
                                queueName(queueName).
                                count(600).
                                sleep(ofSeconds(1)).
                                text("Message in the bottle").
                                createMessage(JMSContext::createMessage).prepareMessage(MessageConverters::setSequentialId).
                                build()
                );
    }

    @Profile(JMS_SYNC_RECEIVER)
    @Bean
    ApplicationRunner jmsSyncReceiver
            (
                    @Qualifier("app-runner") TaskExecutor taskExecutor,
                    ReactiveOps ops,
                    ThrowingFunction<String, ConnectionFactory, JMSException> connectionFactory,
                    @Qualifier("url") String url,
                    @Qualifier("user-name") String userName,
                    @Qualifier("password") String password,
                    @Qualifier("queue-name") String jmsQueueName,
                    @Qualifier("max-attempts") long maxAttempts,
                    @Qualifier("min-backoff") Duration minBackoff,
                    @Qualifier("max-backoff") Duration maxBackoff,
                    KafkaProducer<Long, String> kafkaProducer,
                    @Qualifier("kafka-topic") String kafkaTopic
            )
    {
        return args ->
                taskExecutor.execute(() -> {
                            Flux<MessageExtract> messages =
                                    ops.messages(
                                            connectionFactory, url,
                                            userName, password,
                                            jmsQueueName, MessageConverters::extract,
                                            maxAttempts, minBackoff, maxBackoff
                                    );
                            try
                            {
                                Flux<MessageExtract> written =
                                        messages.doOnNext(message -> {
                                                    ProducerRecord<Long, String> record = new ProducerRecord<>(kafkaTopic, 0L, "test value");
                                                    kafkaProducer.send(record);
                                                }
                                        );
                                subscribeAndAwait(written);
                            }
                            catch (InterruptedException e)
                            {
                                log.info("Requested to interrupt");
                            }
                        }
                );
    }

    @Profile(JMS_SYNC_RECEIVER)
    @Bean
    KafkaProducer<Long, String> kafkaProducer
            (
                    @Qualifier("bootstrap-servers") String bootstrapServers
            )
    {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        config.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(config);
    }
}
