package md.reactive_messaging;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.jms.JmsSimplifiedApiManager;
import md.reactive_messaging.reactive.ReactivePublishers;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.TaskExecutor;
import reactor.core.publisher.Flux;

import javax.jms.ConnectionFactory;
import javax.jms.Message;
import java.time.Duration;
import java.util.function.Function;

import static java.lang.Thread.sleep;
import static java.time.Duration.ofMillis;
import static md.reactive_messaging.Profiles.JMS_ASYNC_LISTENER;
import static md.reactive_messaging.Profiles.JMS_SYNC_SENDER;

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
                    Function<String, ConnectionFactory> connectonFactory,
                    @Qualifier("url") String url,
                    @Qualifier("user-name") String userName,
                    @Qualifier("password") String password,
                    @Qualifier("queue-name") String queueName
            )
    {
        return args ->
                taskExecutor.execute(() ->
                        {
                            while (true)
                            {
                                try
                                {
                                    log.info("Sending");
                                    manager.sendTextMessage(connectonFactory, url, userName, password, queueName, "text");
                                    sleep(ofMillis(100).toMillis());
                                }
                                catch (InterruptedException e)
                                {
                                    log.info("Requested to stop");
                                    break;
                                }
                            }
                        }
                );
    }

    @Profile(JMS_ASYNC_LISTENER)
    @Bean
    ApplicationRunner jmsSyncReceiver
            (
                    TaskExecutor taskExecutor,
                    ReactivePublishers publishers,
                    @Qualifier("url") String url,
                    @Qualifier("user-name") String userName,
                    @Qualifier("password") String password,
                    @Qualifier("queue-name") String queueName,
                    @Qualifier("max-attempts") long maxAttempts,
                    @Qualifier("min-backoff") Duration minBackoff
            )
    {
        return args -> {
            taskExecutor.execute(() ->
                    {
                        final Flux<Message> messages =
                                publishers.asyncMessages(url, userName, password, queueName, maxAttempts, minBackoff);
                        messages.subscribe(
                                message -> {
                                    log.info("Message {}", message);
                                },
                                error -> {
                                    log.error("Error", error);
                                },
                                () -> {
                                    log.error("Completed");
                                }
                        );
                    }
            );
        };
    }
}
