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
import static md.reactive_messaging.reactive.ReactiveOps.onEach;
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

    public <T> Flux<T> asyncMessagesFromCallable
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
        final String factoryName = "factory";
        final Mono<ConnectionFactory> factoryM =
                Mono.fromCallable(() -> {
                                    log.info("Creating factory from callable");
                                    return connectionFactory.apply(url);
                                }
                        ).
                        doOnNext(factory -> log.info("Next {} {}", factoryName, factory)).
                        doOnSuccess(factory -> log.info("Success {} {}", factoryName, factory)).
                        doOnError(error -> log.error("Error {} {}", factoryName, error.getMessage())).
                        name(factoryName).
                        cache();

        final String contextName = "context";
        final Mono<JMSContext> contextM =
                factoryM.flatMap(factory ->
                                Mono.fromCallable(() -> {
                                            log.info("Creating context from callable");
                                            return factory.createContext(userName, password);
                                        }
                                )
                        ).
                        doOnNext(context -> log.info("Next {} {}", contextName, context)).
                        doOnSuccess(context -> log.info("Success {} {}", contextName, context)).
                        doOnError(error -> log.error("Error {}: {}", contextName, error.getMessage())).
                        name(contextName);

        final Many<Reconnect> reconnects = Sinks.many().multicast().onBackpressureBuffer();

        final String retriedName = "retried";
        final Mono<JMSContext> retriedM =
                contextM.
                        retryWhen(backoff(maxAttempts, minBackoff)).flatMap(context ->
                                ops.ops.setExceptionListener(context,
                                        exception ->
                                                ReactiveOps.notifyOnFailure(
                                                        ReactiveOps.nextReconnect(reconnects)
                                                )
                                ).<Mono<JMSContext>>map(
                                        Mono::error
                                ).orElse(
                                        Mono.just(context)
                                )
                        ).
                        doOnNext(context -> log.info("Next {} {}", retriedName, context)).
                        doOnSuccess(context -> log.info("Success {} {}", retriedName, context)).
                        doOnError(error -> log.error("Error {}: {}", retriedName, error.getMessage())).
                        name(retriedName);

        final String repeatedName = "repeated";
        final Flux<JMSContext> repeatedF =
                retriedM.repeatWhen(
                                repeat -> {
                                    final String reconnectName = "reconnect";
                                    return reconnects.asFlux().
                                            doOnNext(reconnect -> log.info("Next {}", reconnectName)).
                                            doOnComplete(() -> log.info("Completed {}", reconnectName)).
                                            doOnError(error -> log.error("Error {}: {}", reconnectName, error.getMessage())).
                                            name(reconnectName);
                                }
                        ).
                        doOnNext(context -> log.info("Next {} {}", repeatedName, context)).
                        doOnComplete(() -> log.info("Completed {}", repeatedName)).
                        doOnError(error -> log.error("Error {}: {}", repeatedName, error.getMessage())).
                        name(repeatedName);

        final String consumersName = "consumers";
        final Flux<JMSConsumer> consumers =
                repeatedF.flatMap(context ->
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
                        ).
                        doOnNext(consumer -> log.info("Next {} {}", consumersName, consumer)).
                        doOnComplete(() -> log.info("Completed {}", consumersName)).
                        doOnError(error -> log.error("Error {}: {}", consumersName, error.getMessage())).
                        name(consumersName);

        final String publishedName = "published";
        return
                consumers.flatMap(consumer -> {
                            Many<T> convertedS = Sinks.many().unicast().onBackpressureBuffer();
                            String convertedName = "converted";
                            consumer.setMessageListener(message -> {
                                        try
                                        {
                                            final T converted = converter.apply(message);
                                            ReactiveOps.notifyOnFailure(
                                                    ReactiveOps.tryNextEmission(convertedS, converted)
                                            );
                                        }
                                        catch (JMSException e)
                                        {
                                            log.error("Error converting message {}", message, e);
                                        }
                                    }
                            );
                            return convertedS.asFlux().
                                    doOnNext(item -> log.info("Next {} {}", convertedName, item)).
                                    doOnComplete(() -> log.info("Completed {}", convertedName)).
                                    doOnError(error -> log.error("Error {}: {}", convertedName, error.getMessage())).
                                    name(convertedName);
                        }).
                        doOnNext(convertedItem -> log.info("Next {} {}", publishedName, convertedItem)).
                        doOnComplete(() -> log.info("Completed {}", publishedName)).
                        doOnError(error -> log.error("Error {}: {}", publishedName, error.getMessage())).
                        name(publishedName);
    }

    public <T> Flux<T> asyncMessagesFunctionally(
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
        final Mono<ConnectionFactory> factoryM =
                ops.ops.instantiateConnectionFactory2(connectionFactory, url).<Mono<ConnectionFactory>>apply(
                                Mono::error,
                                Mono::just
                        ).doOnEach(onEach("factory")).name("factory").
                        cache();

        final Mono<JMSContext> contextM =
                factoryM.flatMap(factory ->
                        ops.ops.createContext(factory, userName, password).apply(
                                Mono::error,
                                Mono::just
                        )
                ).doOnEach(onEach("context")).name("context");

        final Many<Reconnect> reconnects = Sinks.many().multicast().onBackpressureBuffer();

        final Mono<JMSContext> retriedM =
                contextM.
                        retryWhen(backoff(maxAttempts, minBackoff)).flatMap(context ->
                                ops.ops.setExceptionListener(context,
                                        exception ->
                                                ReactiveOps.notifyOnFailure(
                                                        ReactiveOps.nextReconnect(reconnects)
                                                )
                                ).<Mono<JMSContext>>map(
                                        Mono::error
                                ).orElse(
                                        Mono.just(context)
                                )
                        ).
                        doOnEach(onEach("retried")).
                        name("retried");

        final Flux<JMSContext> repeatedF =
                retriedM.repeatWhen(
                        repeat ->
                                reconnects.asFlux().
                                        doOnEach(onEach("reconnect")).name("reconnect")
                ).doOnEach(onEach("repeated")).name("repeated");

        final Flux<JMSConsumer> consumersF =
                repeatedF.flatMap(context ->
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
                consumersF.flatMap(consumer -> {
                                    Many<T> convertedS = Sinks.many().unicast().onBackpressureBuffer();
                                    consumer.setMessageListener(message -> {
                                                try
                                                {
                                                    final T converted = converter.apply(message);
                                                    ReactiveOps.notifyOnFailure(
                                                            ReactiveOps.tryNextEmission(convertedS, converted)
                                                    );
                                                }
                                                catch (JMSException e)
                                                {
                                                    log.error("Error converting message {}", message, e);
                                                }
                                            }
                                    );
                                    return convertedS.asFlux().
                                            doOnEach(onEach("converted")).name("converted");
                                }
                        ).
                        doOnEach(onEach("published")).name("published");
    }
}
