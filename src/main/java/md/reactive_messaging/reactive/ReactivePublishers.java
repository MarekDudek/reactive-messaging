package md.reactive_messaging.reactive;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.Either;
import md.reactive_messaging.functional.throwing.ThrowingFunction;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Many;
import reactor.util.retry.Retry;

import javax.jms.*;
import java.lang.IllegalStateException;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Optional.ofNullable;
import static md.reactive_messaging.functional.Either.right;
import static md.reactive_messaging.functional.Functional.consume;
import static md.reactive_messaging.functional.Functional.error;
import static md.reactive_messaging.reactive.Reconnect.RECONNECT;
import static reactor.core.publisher.Sinks.EmitResult.OK;
import static reactor.util.retry.Retry.backoff;

@RequiredArgsConstructor
@Slf4j
public class ReactivePublishers
{
    @NonNull
    public final ReactiveOps ops;

    public <T> Flux<T> syncMessages
            (
                    Function<String, ConnectionFactory> connectionFactory,
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

    public <T> Flux<T> asyncMessages
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
        final Many<Reconnect> reconnect = Sinks.many().unicast().onBackpressureBuffer();

        final Mono<JMSConsumer> monitoredConsumer =
                ops.factory(connectionFactory, url).flatMap(factory ->
                        ops.context(factory, userName, password).map(context ->
                                ops.setExceptionListener(context, reconnect)
                        )
                ).flatMap(monitoredContext ->
                        ops.createQueueConsumer(monitoredContext, queueName, reconnect)
                );

        final Flux<JMSConsumer> reconnections =
                monitoredConsumer.
                        retryWhen(backoff(maxAttempts, minBackoff)).
                        repeatWhen(repeat -> reconnect.asFlux());

        return
                reconnections.flatMap(consumer -> {
                            Many<T> messages = Sinks.many().unicast().onBackpressureBuffer();
                            ops.ops.setMessageListener(consumer, message ->
                                    ops.ops.applyToMessage(message, converter).consume(
                                            conversionError ->
                                                    log.error("Failed converting {}", message, conversionError),
                                            converted -> {
                                                final EmitResult result = messages.tryEmitNext(converted);
                                                log.info("Converted message to {}, emitted {}", converted, result);
                                            }
                                    )
                            );
                            return messages.asFlux();
                        }
                );
    }

    public <T> Flux<T> asyncMessages2
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
        final Many<Reconnect> reconnect = Sinks.many().multicast().onBackpressureBuffer();

        final Mono<ConnectionFactory> factory =
                ops.ops.instantiateConnectionFactory(connectionFactory, url).<Mono<ConnectionFactory>>apply(
                        error -> {
                            final EmitResult result = reconnect.tryEmitNext(RECONNECT);
                            log.info("Error instantiating factory, emitted {}", result);
                            return Mono.error(error);
                        },
                        Mono::just
                ).log();
        final Mono<ConnectionFactory> factory2 =
                Mono.fromCallable(() -> {
                            final ConnectionFactory f = connectionFactory.apply(url);
                            log.info("factory from callable {}", f);
                            return f;
                        }
                ).log();


        final Mono<ConnectionFactory> currentFactory = factory;

        final Mono<ConnectionFactory> retriedFactory =
                currentFactory.retryWhen(backoff(maxAttempts, minBackoff)).log();
        final Flux<T> retriedMessages =
                retriedFactory.flatMapMany(f -> {
                            log.info("retrying");
                            try
                            {
                                final T converted = converter.apply(null);
                                return Flux.just(converted);
                            }
                            catch (JMSException e)
                            {
                                return Flux.error(e);
                            }
                        }
                ).log();

        final Flux<ConnectionFactory> repeatedFactory =
                currentFactory.repeatWhen(repeat -> reconnect.asFlux());
        final Flux<ConnectionFactory> retriedRepeatedFactory =
                repeatedFactory.retryWhen(backoff(maxAttempts, minBackoff));
        final Flux<T> repeatedMessages =
                retriedRepeatedFactory.flatMap(f -> {
                            log.info("retrying");
                            try
                            {
                                final T converted = converter.apply(null);
                                return Flux.just(converted);
                            }
                            catch (JMSException e)
                            {
                                return Flux.error(e);
                            }
                        }
                );


        final Mono<JMSContext> contextsM =
                currentFactory.flatMap(f ->
                        ops.ops.createContext(f, userName, password).apply(
                                error -> {
                                    final EmitResult result = reconnect.tryEmitNext(RECONNECT);
                                    log.error("Error creating context, emitted {}", result);
                                    return Mono.error(error);
                                },
                                Mono::just
                        )
                ).log();

        final Mono<JMSContext> monitoredContext =
                contextsM.flatMap(context ->
                        ops.ops.setExceptionListener(context,
                                exceptionInContext -> {
                                    final EmitResult result = reconnect.tryEmitNext(RECONNECT);
                                    log.info("Heard in context exception, emitted {}", result);
                                }
                        ).map(
                                Mono::<JMSContext>error
                        ).orElse(
                                Mono.just(context)
                        )
                ).log();

        final Mono<JMSConsumer> safeConsumer =
                monitoredContext.flatMap(context ->
                        ops.ops.createQueue(context, queueName).flatMap(queue ->
                                ops.ops.createConsumer(context, queue)
                        ).apply(
                                error -> {
                                    final EmitResult result = reconnect.tryEmitNext(RECONNECT);
                                    log.error("Error creating queue or consumer, emitted {}", result);
                                    return Mono.error(error);
                                },
                                Mono::just
                        )
                ).log();

        final Flux<T> decoded =
                safeConsumer.flatMapMany(consumer -> {
                            Many<T> sink = Sinks.many().unicast().onBackpressureBuffer();
                            final Optional<JMSRuntimeException> errorSetting =
                                    ops.ops.setMessageListener(consumer, message ->
                                            ops.ops.applyToMessage(message, converter).consume(
                                                    error ->
                                                            log.error("Error converting message {}", message),
                                                    converted -> {
                                                        final EmitResult result = sink.tryEmitNext(converted);
                                                        log.info("Emitting converted from heard message {}", result);
                                                    }
                                            )
                                    );
                            return errorSetting.<Flux<T>>map(
                                    Flux::error
                            ).orElseGet(
                                    sink::asFlux
                            );
                        }
                ).log();

        return
                decoded.retryWhen(
                        backoff(maxAttempts, minBackoff)
                ).log();


/*
        final Flux<ConnectionFactory> successfulReconnectedFactories =
                retriedFactory.repeatWhen(repeat -> {
                            log.warn("Ordered to repeat retrying");
                            return reconnect.asFlux();
                        }
                ).log();

        final Flux<ConnectionFactory> repeatedFactories =
                factory2.repeatWhen(repeat -> {
                            log.warn("Ordered to repeat retrying, v2");
                            return reconnect.asFlux();
                        }
                ).log();
        final Flux<ConnectionFactory> successfulReconnectedFactories2 =
                repeatedFactories.retryWhen(Retry.backoff(maxAttempts, minBackoff)).log();

        final Flux<JMSContext> contexts =
                successfulReconnectedFactories2.flatMap(f ->
                        ops.ops.createContext(f, userName, password).apply(
                                error -> {
                                    final EmitResult result = reconnect.tryEmitNext(RECONNECT);
                                    log.error("Error creating context, emitted {}", result);
                                    return Flux.error(error);
                                },
                                Mono::just
                        )
                ).log();
        final Flux<JMSContext> monitoredContexts =
                contexts.flatMap(context ->
                        ops.ops.setExceptionListener(context,
                                exceptionInContext -> {
                                    final EmitResult result = reconnect.tryEmitNext(RECONNECT);
                                    log.info("Heard in context exception, emitted {}", result);
                                }
                        ).map(
                                Mono::<JMSContext>error
                        ).orElse(
                                Mono.just(context)
                        )
                ).log();
        final Flux<JMSConsumer> safeConsumers =
                monitoredContexts.flatMap(context ->
                        ops.ops.createQueue(context, queueName).flatMap(queue ->
                                ops.ops.createConsumer(context, queue)
                        ).apply(
                                error -> {
                                    final EmitResult result = reconnect.tryEmitNext(RECONNECT);
                                    log.error("Error creating queue or consumer, emitted {}", result);
                                    return Flux.error(error);
                                },
                                Mono::just
                        )
                ).log();

        return
                safeConsumers.flatMap(consumer -> {
                            Many<T> sink = Sinks.many().unicast().onBackpressureBuffer();
                            final Optional<JMSRuntimeException> errorSetting =
                                    ops.ops.setMessageListener(consumer, message ->
                                            ops.ops.applyToMessage(message, converter).consume(
                                                    error -> {
                                                        log.error("Error converting message {}", message);
                                                    },
                                                    converted -> {
                                                        final EmitResult result = sink.tryEmitNext(converted);
                                                        log.info("Emitting converted from heard message {}", result);
                                                    }
                                            )
                                    );
                            return errorSetting.<Flux<T>>map(
                                    Flux::error
                            ).orElseGet(
                                    sink::asFlux
                            );
                        }
                );*/
    }

    public <T> Flux<T> asyncMessages3(
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
        final Many<Reconnect> reconnectS =
                Sinks.many().multicast().onBackpressureBuffer();

        final Mono<ConnectionFactory> factoryM =
                Mono.fromCallable(() ->
                        connectionFactory.apply(url)
                ).doOnError(throwable -> {
                            reconnect(reconnectS, "Error creating connection factory, reconnect emitted {}", throwable);
                        }
                ).name("factory-creator").log();

        final Mono<JMSContext> contextM2 =
                factoryM.flatMap(factory ->
                        Mono.fromCallable(() -> {
                                    log.info("Creating context");
                                    final JMSContext context = factory.createContext(userName, password);
                                    log.info("Created context");
                                    context.setExceptionListener(exception -> {
                                                reconnect(reconnectS, "Heard in context, reconnect emitted {}", exception);
                                                context.close();
                                            }
                                    );
                                    return context;
                                }
                        ).doOnError(throwable ->
                                reconnect(reconnectS, "Error creating context, reconnect emitted {}", throwable)
                        ).name("context-creator-exception-listener-setter").log()
                ).name("listened-to-context").log();


        final Mono<JMSContext> retriedM =
                contextM2.
                        retryWhen(Retry.backoff(maxAttempts, minBackoff)).
                        doOnSubscribe(subscription ->
                                log.info("Subscribing to retried {}", subscription)
                        ).
                        name("retryer").log();

        final Flux<JMSContext> repeatedM =
                retriedM.
                        repeatWhen(repeat ->
                                reconnectS.asFlux().name("context-reconnects").log()
                        ).
                        doOnSubscribe(subscription ->
                                log.info("Subscribing do retried {}", subscription)
                        ).
                        name("repeater").log();

        final Flux<JMSConsumer> consumerF =
                repeatedM.flatMap(context ->
                        Mono.fromCallable(() ->
                                context.createQueue(queueName)
                        ).doOnError(throwable -> {
                                    //reconnect(reconnectS, "Error creating queue, reconnect emitted {}", throwable);
                                    //context.close();
                                }
                        ).name("queue-creator").log().flatMap(queue ->
                                Mono.fromCallable(() ->
                                        context.createConsumer(queue)
                                ).doOnError(throwable -> {
                                            //reconnect(reconnectS, "Error creating consumer, reconnect emitted {}", throwable);
                                            //context.close();
                                        }
                                ).name("consumer-creator").log()
                        ).name("queue-consumer-creator").log()
                ).name("queue-consumers").log();

        return
                consumerF.flatMap(consumer -> {
                            Many<T> messageS = Sinks.many().unicast().onBackpressureBuffer();
                            consumer.setMessageListener(message -> {
                                        try
                                        {
                                            final T converted = converter.apply(message);
                                            final EmitResult result = messageS.tryEmitNext(converted);
                                            log.info("Heard message in consumer {}, emitted {}", consumer, result);
                                        }
                                        catch (JMSException e)
                                        {
                                            log.error("Error converting message {}", message, e);
                                        }
                                    }
                            );
                            return messageS.asFlux().name("message-listener-setter").log();
                        }
                ).doOnError(throwable -> {
                            // reconnect(reconnectS, "Error setting message listener, reconnect emitted {}", throwable);
                        }
                ).name("messages").log();
    }

    private static <T> void reconnect(Many<Reconnect> reconnectS, String format, Throwable throwable)
    {
        final EmitResult result = reconnectS.tryEmitNext(RECONNECT);
        log.warn(format, result, throwable);
    }

    public <T> Flux<T> asyncMessages4(
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
        final Many<Reconnect> reconnectS =
                Sinks.many().multicast().onBackpressureBuffer();

        final ConnectionFactory factory = connectionFactory.apply(url);

        final Mono<JMSContext> contextCreationM =
                ops.ops.createContext(factory, userName, password).<Mono<JMSContext>>apply(
                        Mono::error,
                        Mono::just
                ).name("context-creation");

        @Deprecated final Mono<JMSContext> reconnectingContextM =
                contextCreationM.
                        doOnError(throwable ->
                                failOnFailure(nextReconnect(reconnectS)
                                )
                        ).name("reconnecting-context-creation").log();

        final Mono<JMSContext> retriedContextCreationM =
                contextCreationM.flatMap(
                        contextCreated ->
                                contextCreationM
                                        .retryWhen(backoff(maxAttempts, minBackoff)).name("resubscribed-creations")
                                        .onErrorResume(error -> {
                                                    log.error("Error retrying context creation");
                                                    ops.ops.closeContext(contextCreated).
                                                            ifPresent(exception -> {
                                                                        log.error("Error closing retried context {}", contextCreated);
                                                                    }
                                                            );
                                                    return contextCreationM;
                                                }
                                        ).name("resumed")
                );

        final Flux<JMSContext> repeatedContextCreationF =
                retriedContextCreationM.flatMapMany(
                        retriedContext ->
                                retriedContextCreationM
                                        .repeatWhen(repeat -> reconnectS.asFlux())
                                        .onErrorResume(error -> {
                                                    log.error("Error repeating retried context creation");
                                                    notifyOnFailure(
                                                            nextReconnect(reconnectS)
                                                    );
                                                    ops.ops.closeContext(retriedContext).
                                                            ifPresent(exception -> {
                                                                        log.error("Error closing repeated context {}", retriedContext);
                                                                    }
                                                            );
                                                    return retriedContextCreationM;
                                                }
                                        )
                );


        @Deprecated final Mono<JMSContext> retriedContextM =
                reconnectingContextM.retryWhen(backoff(maxAttempts, minBackoff)).name("retried-context-creation").log().
                        doOnError(throwable ->
                                failOnFailure(nextReconnect(reconnectS)
                                )
                        );
        @Deprecated final Flux<JMSContext> repeatedContextsF =
                retriedContextM.repeatWhen(
                        repeat ->
                                reconnectS.asFlux().name("reconnect-events").log()
                ).name("repeated-context-creation").log().map(context -> {

                    return context;
                });

        final Flux<T> out =
                repeatedContextCreationF.flatMap(context -> {
                            try
                            {
                                final T t = converter.apply(null);
                                return Mono.just(t);
                            }
                            catch (JMSException e)
                            {
                                return Mono.error(e);
                            }
                        }
                );

        return out;
    }

    public <T> Flux<T> asyncMessagesFromCallable(
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

        ;
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
                                                notifyOnFailure(
                                                        nextReconnect(reconnects)
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
                                            notifyOnFailure(
                                                    tryNextEmission(convertedS, converted)
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
        final Mono<ConnectionFactory> factoryM =
                ops.ops.instantiateConnectionFactory(connectionFactory, url).<Mono<ConnectionFactory>>apply(
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
                                                notifyOnFailure(
                                                        nextReconnect(reconnects)
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
                                                    notifyOnFailure(
                                                            tryNextEmission(convertedS, converted)
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

    private static <T> Consumer<Signal<T>> onEach(String name)
    {
        return signal -> {
            switch (signal.getType())
            {
                case ON_NEXT:
                    log.info("Next in {} - {}", name, signal.get());
                    break;
                case ON_COMPLETE:
                    log.info("Completed {} with {}", name, signal.get());
                    break;
                case ON_ERROR:
                    log.info("Error in {} - {}", name, ofNullable(signal.getThrowable()).map(
                            Throwable::getMessage
                    ).orElse(
                            "!NO MESSAGE!"
                    ));
                    break;
                default:
                    log.warn("Unknown signal - {}", signal);
                    break;
            }
        };
    }

    private static Either<EmitResult, EmitResult> nextReconnect(Many<Reconnect> reconnect)
    {
        return tryNextEmission(reconnect, RECONNECT);
    }

    private static <T> Either<EmitResult, EmitResult> tryNextEmission(Many<T> sink, T decoded)
    {
        final EmitResult emitted = sink.tryEmitNext(decoded);
        log.info("Emitting {}", decoded);
        return right(emitted).filter(result -> result == OK);
    }

    private static void notifyOnFailure(Either<EmitResult, EmitResult> result)
    {
        result.consume(
                failure ->
                        log.error("Failed to emit {}", failure),
                success ->
                        log.info("Emitted OK")
        );
    }

    private static void failOnFailure(Either<EmitResult, EmitResult> result)
    {
        result.consume(
                failure ->
                        error(new IllegalStateException(failure.toString())),
                success ->
                        log.trace("Emitted")
        );
    }
}
