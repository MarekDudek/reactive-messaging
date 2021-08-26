package md.reactive_messaging.apps;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.throwing.ThrowingFunction;
import md.reactive_messaging.jms.JmsSimplifiedApiOps;
import md.reactive_messaging.reactive.ReactivePublishers;
import reactor.core.publisher.Flux;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import java.time.Duration;
import java.util.function.Function;

import static md.reactive_messaging.reactive.GenericSubscribers.defaultSubscriber;

@Builder
@Slf4j
public final class JmsAsyncListener<T> implements Runnable
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
    private final ThrowingFunction<Message, T, JMSException> converter;

    private final long maxAttempts;
    @NonNull
    private final Duration minBackoff;

    @Override
    public void run()
    {
        log.info("Start");
        final Flux<T> publisher =
                publishers.asyncMessagesFunctionally(
                        connectionFactory, url,
                        userName, password,
                        queueName, converter,
                        maxAttempts, minBackoff
                );
        defaultSubscriber(publisher);
        log.info("Finish");
    }
}
