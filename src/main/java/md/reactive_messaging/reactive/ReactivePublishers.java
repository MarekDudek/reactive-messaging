package md.reactive_messaging.reactive;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.throwing.ThrowingFunction;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

import javax.jms.*;
import java.time.Duration;
import java.util.function.Function;

import static md.reactive_messaging.functional.Functional.consume;
import static md.reactive_messaging.reactive.ReactiveOps.*;
import static reactor.util.retry.Retry.backoff;

@RequiredArgsConstructor
@Slf4j
public class ReactivePublishers
{
    @NonNull
    public final ReactiveOps ops;

    public <T> Flux<T> syncMessages
            (
                    ThrowingFunction<String, ConnectionFactory, JMSException> connectionFactory,
                    String url,
                    String userName,
                    String password,
                    String queueName,
                    Class<T> klass,
                    long maxAttempts,
                    Duration minBackoff
            )
    {
        final Many<Reconnect> reconnect = Sinks.many().unicast().onBackpressureBuffer();

        final Mono<JMSConsumer> monitoredConsumer =
                ops.factory(connectionFactory, url).flatMap(factory ->
                        ops.context(factory, userName, password).map(context ->
                                ops.setExceptionListener(context, reconnect)
                        )
                ).flatMap(context ->
                        ops.createQueueConsumer(context, queueName, reconnect)
                );

        final Flux<T> single =
                monitoredConsumer.flatMapMany(consumer ->
                        ops.receiveMessageBodies(consumer, klass, reconnect)
                );

        return single.
                retryWhen(backoff(maxAttempts, minBackoff)).
                repeatWhen(repeat -> reconnect.asFlux());
    }

    public <T> Flux<T> asyncMessages(
            ThrowingFunction<String, ConnectionFactory, JMSException> connectionFactory,
            String url,
            String userName,
            String password,
            String queueName,
            ThrowingFunction<Message, T, JMSException> converter,
            long maxAttempts,
            Duration minBackoff
    )
    {
        final Mono<JMSContext> contextM = factoryContext(connectionFactory, url, userName, password);
        final Flux<JMSContext> reliable = retriedAndRepeated(contextM, maxAttempts, minBackoff);
        return listenOn(reliable, queueName, converter);
    }

    public <T> Flux<T> asyncMessagesAlt
            (
                    Function<String, ConnectionFactory> connectionFactory,
                    String url,
                    String userName,
                    String password,
                    String queueName,
                    ThrowingFunction<Message, T, JMSException> converter,
                    long maxAttempts,
                    Duration minBackoff
            )
    {
        final Mono<JMSContext> contextM = factoryContextAlt(connectionFactory, url, userName, password);
        final Flux<JMSContext> reliable = retriedAndRepeated(contextM, maxAttempts, minBackoff);
        return listenOn(reliable, queueName, converter);
    }

    private Mono<JMSContext> factoryContext(ThrowingFunction<String, ConnectionFactory, JMSException> connectionFactory, String url, String userName, String password)
    {
        final Mono<ConnectionFactory> factoryM =
                ops.ops.instantiateConnectionFactory2(connectionFactory, url).<Mono<ConnectionFactory>>apply(
                                Mono::error,
                                Mono::just
                        ).doOnEach(onEach("factory")).name("factory").
                        cache();

        return
                factoryM.flatMap(factory ->
                        ops.ops.createContext(factory, userName, password).apply(
                                Mono::error,
                                Mono::just
                        )
                ).doOnEach(onEach("context")).name("context");
    }

    private static Mono<JMSContext> factoryContextAlt(Function<String, ConnectionFactory> connectionFactory, String url, String userName, String password)
    {
        return
                Mono.fromCallable(() -> {
                                    log.info("Creating factory");
                                    return connectionFactory.apply(url);
                                }
                        ).doOnEach(onEach("factory")).name("factory").
                        cache(
                        ).flatMap(factory ->
                                Mono.fromCallable(() -> {
                                            log.info("Creating context");
                                            return factory.createContext(userName, password);
                                        }
                                )
                        ).doOnEach(onEach("context")).name("context");
    }

    private Flux<JMSContext> retriedAndRepeated(Mono<JMSContext> contextM, long maxAttempts, Duration minBackoff)
    {
        final Many<Reconnect> reconnects = Sinks.many().multicast().onBackpressureBuffer();
        final Mono<JMSContext> retriedM =
                contextM.
                        retryWhen(backoff(maxAttempts, minBackoff)).flatMap(context ->
                                ops.ops.setExceptionListener(context,
                                        exception ->
                                                nextReconnect(reconnects, ReactiveOps::reportFailure)
                                ).<Mono<JMSContext>>map(
                                        Mono::error
                                ).orElse(
                                        Mono.just(context)
                                )
                        ).doOnEach(onEach("retried")).name("retried");

        return
                retriedM.repeatWhen(
                        repeat ->
                                reconnects.asFlux().
                                        doOnEach(onEach("reconnect")).name("reconnect")
                ).doOnEach(onEach("repeated")).name("repeated");
    }

    private <T> Flux<T> listenOn(Flux<JMSContext> contextM, String queueName, ThrowingFunction<Message, T, JMSException> converter)
    {
        final Flux<JMSConsumer> consumers =
                contextM.flatMap(context ->
                        ops.ops.createQueue(context, queueName).flatMap(queue ->
                                ops.ops.createConsumer(context, queue)
                        ).apply(
                                errorCreatingQueueOrConsumer -> {
                                    consume(
                                            ops.ops.closeContext(context),
                                            errorClosing ->
                                                    log.warn("Closing context failed: {}", errorClosing.getMessage()),
                                            () ->
                                                    log.info("Closing context succeeded")
                                    );
                                    return Mono.error(errorCreatingQueueOrConsumer);
                                },
                                Mono::just
                        )
                ).doOnEach(onEach("consumers")).name("consumers");

        return
                consumers.flatMap(consumer -> {
                            Many<T> convertedS = Sinks.many().unicast().onBackpressureBuffer();
                            consumer.setMessageListener(message ->
                                    ops.ops.applyToMessage(message, converter).consume(
                                            exception ->
                                                    log.error("Error converting message {}", message, exception),
                                            converted ->
                                                    tryNextEmission(convertedS, converted, ReactiveOps::reportFailure)
                                    )
                            );
                            return convertedS.asFlux().
                                    doOnEach(onEach("converted")).name("converted");
                        }
                ).doOnEach(onEach("published")).name("published");
    }
}
