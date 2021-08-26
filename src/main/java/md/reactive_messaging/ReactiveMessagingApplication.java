package md.reactive_messaging;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.apps.JmsAsyncListener;
import md.reactive_messaging.apps.JmsSyncReceiver;
import md.reactive_messaging.apps.JmsSyncSender;
import md.reactive_messaging.functional.throwing.ThrowingFunction;
import md.reactive_messaging.jms.JmsSimplifiedApiManager;
import md.reactive_messaging.jms.JmsSimplifiedApiOps;
import md.reactive_messaging.reactive.ReactivePublishers;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import java.time.Duration;
import java.util.function.Function;

import static java.lang.String.format;
import static java.time.Duration.between;
import static java.time.Duration.ofSeconds;
import static java.time.Instant.now;
import static java.time.Instant.ofEpochMilli;
import static md.reactive_messaging.Profiles.*;
import static md.reactive_messaging.tasks.RethrowingHandler.RETHROWING_HANDLER;

@SpringBootApplication
@Slf4j
public class ReactiveMessagingApplication
{
    public static void main(String[] args)
    {
        SpringApplication.run(ReactiveMessagingApplication.class, args);
    }

    @Bean
    @Qualifier("app")
    TaskExecutor taskExecutor()
    {
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(4);
        executor.setThreadNamePrefix("app-task-executor-");
        return executor;
    }

    @Profile(JMS_SYNC_SENDER)
    @Bean
    ApplicationRunner jmsSyncSender
            (
                    @Qualifier("app") TaskExecutor taskExecutor,
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
                                queueName(queueName).text("Message in the bottle").
                                sleep(ofSeconds(1)).
                                build()
                );
    }

    @Profile(JMS_ASYNC_LISTENER)
    @Bean
    ApplicationRunner jmsAsyncListener
            (
                    @Qualifier("app") TaskExecutor taskExecutor,
                    ReactivePublishers publishers,
                    JmsSimplifiedApiOps jmsOps,
                    ThrowingFunction<String, ConnectionFactory, JMSException> connectionFactory,
                    @Qualifier("url") String url,
                    @Qualifier("user-name") String userName,
                    @Qualifier("password") String password,
                    @Qualifier("queue-name") String queueName,
                    @Qualifier("max-attempts") long maxAttempts,
                    @Qualifier("min-backoff") Duration minBackoff
            )
    {
        return args ->
                taskExecutor.execute(() ->
                        RETHROWING_HANDLER.handle(
                                JmsAsyncListener.<String>builder().
                                        publishers(publishers).jmsOps(jmsOps).
                                        connectionFactory(connectionFactory).url(url).
                                        userName(userName).password(password).
                                        queueName(queueName).
                                        converter(message ->
                                                format("RECEIVED %s (after %s)",
                                                        message.getBody(String.class),
                                                        between(
                                                                ofEpochMilli(message.getJMSDeliveryTime()),
                                                                now()
                                                        )
                                                )
                                        ).
                                        maxAttempts(maxAttempts).minBackoff(minBackoff).
                                        build(),
                                JmsAsyncListener.class.getName()
                        )
                );
    }

    @Profile(JMS_SYNC_RECEIVER)
    @Bean
    ApplicationRunner jmsSyncReceiver
            (
                    @Qualifier("app") TaskExecutor taskExecutor,
                    ReactivePublishers publishers,
                    ThrowingFunction<String, ConnectionFactory, JMSException> connectionFactory,
                    @Qualifier("url") String url,
                    @Qualifier("user-name") String userName,
                    @Qualifier("password") String password,
                    @Qualifier("queue-name") String queueName,
                    @Qualifier("max-attempts") long maxAttempts,
                    @Qualifier("min-backoff") Duration minBackoff
            )
    {
        return args ->
                taskExecutor.execute(() ->
                        RETHROWING_HANDLER.handle(
                                JmsSyncReceiver.builder().
                                        publishers(publishers).
                                        connectionFactory(connectionFactory).url(url).
                                        userName(userName).password(password).
                                        queueName(queueName).
                                        maxAttempts(maxAttempts).minBackoff(minBackoff).
                                        build(),
                                JmsSyncReceiver.class.getName()
                        )
                );
    }
}
