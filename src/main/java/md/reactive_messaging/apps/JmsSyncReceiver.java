package md.reactive_messaging.apps;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.reactive.ReactivePublishers;
import reactor.core.publisher.Flux;

import javax.jms.ConnectionFactory;
import java.time.Duration;
import java.util.function.Function;

@Builder
@Slf4j
public final class JmsSyncReceiver implements Runnable
{
    @NonNull
    private final ReactivePublishers publishers;
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
        final Flux<String> messageBodies =
                publishers.syncMessages(connectionFactory, url,
                        userName, password,
                        queueName,
                        String.class,
                        maxAttempts, minBackoff
                );
        messageBodies.subscribe(
                body ->
                        log.info("Body {}", body),
                error ->
                        log.error("Error", error),
                () ->
                        log.error("Completed")
        );
        log.info("Finish");
    }
}