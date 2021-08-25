package md.reactive_messaging.apps;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.throwing.ThrowingFunction;
import md.reactive_messaging.jms.JmsSimplifiedApiOps;
import md.reactive_messaging.reactive.ReactivePublishers;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import java.time.Duration;
import java.util.function.Function;

@Builder
@Slf4j
public final class WellBehavedReconnector implements Runnable
{
    @NonNull
    private final ReactivePublishers publishers;
    @NonNull
    private final JmsSimplifiedApiOps jmsOps;
    @NonNull
    private final Function<String, ConnectionFactory> connectionFactory;
    @NonNull
    private final String url;
    @NonNull
    private final String userName;
    @NonNull
    private final String password;
    @NonNull
    private final String queueName;
    @NonNull
    private final ThrowingFunction<Message, Object, JMSException> converter;

    private final long maxAttempts;
    @NonNull
    private final Duration minBackoff;

    @Override
    public void run()
    {
        log.info("Start");
        publishers.asyncMessages3(
                connectionFactory, url,
                userName, password,
                queueName, converter,
                maxAttempts, minBackoff
        ).subscribe(
                success ->
                        log.info("Success {}", success),
                error ->
                        log.error("Error", error),
                () ->
                        log.error("Completed")
        );
        log.info("Finish");
    }
}
