package md.reactive_messaging;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.apps.JmsAsyncListener;
import md.reactive_messaging.apps.JmsSyncReceiver;
import md.reactive_messaging.apps.JmsSyncSender;
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

import javax.jms.ConnectionFactory;
import java.time.Duration;
import java.util.function.Function;

import static java.time.Duration.ofSeconds;
import static md.reactive_messaging.Profiles.*;

@SpringBootApplication
@Slf4j
public class ReactiveMessagingApplication
{
    public static void main(String[] args)
    {
        SpringApplication.run(ReactiveMessagingApplication.class, args);
    }

    @Profile(JMS_SYNC_SENDER)
    @Bean
    ApplicationRunner jmsSyncSender
            (
                    TaskExecutor taskExecutor,
                    JmsSimplifiedApiManager manager,
                    Function<String, ConnectionFactory> connectionFactory,
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
                    TaskExecutor taskExecutor,
                    ReactivePublishers publishers,
                    JmsSimplifiedApiOps jmsOps,
                    Function<String, ConnectionFactory> connectionFactory,
                    @Qualifier("url") String url,
                    @Qualifier("user-name") String userName,
                    @Qualifier("password") String password,
                    @Qualifier("queue-name") String queueName,
                    @Qualifier("max-attempts") long maxAttempts,
                    @Qualifier("min-backoff") Duration minBackoff
            )
    {
        return args ->
                taskExecutor.execute(() -> {
                            try
                            {
                                log.info("Try task {}", JmsAsyncListener.class.getName());
                                final JmsAsyncListener listener =
                                        JmsAsyncListener.builder().
                                                publishers(publishers).jmsOps(jmsOps).
                                                connectionFactory(connectionFactory).url(url).
                                                userName(userName).password(password).
                                                queueName(queueName).
                                                maxAttempts(maxAttempts).minBackoff(minBackoff).
                                                build();
                                listener.run();
                                log.info("Success in task {}", JmsAsyncListener.class.getName());
                            }
                            catch (Exception e)
                            {
                                log.error("Exception in {} task", JmsAsyncListener.class.getName(), e);
                                throw e;
                            }
                            log.info("Critical error in {}", JmsAsyncListener.class.getName());
                        }
                );
    }

    @Profile(JMS_SYNC_RECEIVER)
    @Bean
    ApplicationRunner jmsSyncReceiver
            (
                    TaskExecutor taskExecutor,
                    ReactivePublishers publishers,
                    Function<String, ConnectionFactory> connectionFactory,
                    @Qualifier("url") String url,
                    @Qualifier("user-name") String userName,
                    @Qualifier("password") String password,
                    @Qualifier("queue-name") String queueName,
                    @Qualifier("max-attempts") long maxAttempts,
                    @Qualifier("min-backoff") Duration minBackoff
            )
    {
        return args ->
                taskExecutor.execute(() -> {
                            try
                            {
                                log.info("Try task {}", JmsSyncReceiver.class.getName());
                                final JmsSyncReceiver receiver =
                                        JmsSyncReceiver.builder().
                                                publishers(publishers).
                                                connectionFactory(connectionFactory).url(url).
                                                userName(userName).password(password).
                                                queueName(queueName).
                                                maxAttempts(maxAttempts).minBackoff(minBackoff).
                                                build();
                                receiver.run();
                                log.info("Success in task {}", JmsSyncReceiver.class.getName());
                            }
                            catch (Exception e)
                            {
                                log.error("Exception in {} task", JmsSyncReceiver.class.getName(), e);
                                throw e;
                            }
                            log.info("Critical error in {}", JmsSyncReceiver.class.getName());
                        }
                );
    }
}
