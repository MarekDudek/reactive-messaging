package md.reactive_messaging.apps;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.jms.JmsSimplifiedApiOps;
import md.reactive_messaging.reactive.ReactivePublishers;

import javax.jms.ConnectionFactory;
import java.time.Duration;
import java.util.function.Function;

@Builder
@Slf4j
public final class JmsAsyncListener implements Runnable
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

    private final long maxAttempts;
    @NonNull
    private final Duration minBackoff;

    @Override
    public void run()
    {
        log.info("Start");
        publishers.asyncMessages(
                connectionFactory, url,
                userName, password,
                queueName,
                maxAttempts, minBackoff
        ).subscribe(
                message ->
                        jmsOps.applyToMessage(message, msg ->
                                msg.getBody(String.class)
                        ).consume(
                                error ->
                                        log.error("Failed", error),
                                result ->
                                        log.info("Got {}", result)
                        ),
                error ->
                        log.error("Error", error),
                () ->
                        log.error("Completed")
        );
        log.info("Finish");
    }
}
