package md.reactive_messaging.reactive;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.throwing.ThrowingFunction;
import md.reactive_messaging.jms.JmsSimplifiedApiOps;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

import javax.jms.*;
import java.time.Duration;

import static md.reactive_messaging.functional.Functional.consume;
import static md.reactive_messaging.reactive.ReactiveUtils.*;
import static reactor.util.retry.Retry.backoff;

@RequiredArgsConstructor
@Slf4j
public class ReactivePublishers
{
    @NonNull
    public final JmsSimplifiedApiOps ops;

    public <T> Flux<T> receiveMessagesSynchronously
            (
                    ThrowingFunction<String, ConnectionFactory, JMSException> connectionFactory,
                    String url,
                    String userName, String password,
                    String queueName, Class<T> klass,
                    long maxAttempts, Duration minBackoff
            )
    {
        final Mono<JMSContext> contextM = factoryAndContext(connectionFactory, url, userName, password);
        final Flux<JMSContext> reliableF = retriedAndRepeated(contextM, maxAttempts, minBackoff);
        final Flux<JMSConsumer> consumerF = queueAndConsumer(reliableF, queueName);
        return bodiesReceived(consumerF, klass);
    }

    public <T> Flux<T> listenToMessagesAsynchronously
            (
                    ThrowingFunction<String, ConnectionFactory, JMSException> connectionFactory,
                    String url,
                    String userName, String password,
                    String queueName,
                    ThrowingFunction<Message, T, JMSException> converter,
                    long maxAttempts, Duration minBackoff
            )
    {
        final Mono<JMSContext> contextM = factoryAndContext(connectionFactory, url, userName, password);
        final Flux<JMSContext> reliableF = retriedAndRepeated(contextM, maxAttempts, minBackoff);
        final Flux<JMSConsumer> consumerF = queueAndConsumer(reliableF, queueName);
        return heardInConsumer(consumerF, converter);
    }

    private Mono<JMSContext> factoryAndContext
            (
                    ThrowingFunction<String, ConnectionFactory, JMSException> connectionFactory,
                    String url,
                    String userName, String password
            )
    {
        return
                ops.connectionFactoryForUrlChecked(connectionFactory, url).<Mono<ConnectionFactory>>apply(
                        Mono::error,
                        Mono::just
                ).doOnEach(onEach("factory")).name("factory"
                ).cache().flatMap(factory ->
                        ops.createContext(factory, userName, password).apply(
                                Mono::error,
                                Mono::just
                        )
                ).doOnEach(onEach("context")).name("context");
    }

    private Flux<JMSContext> retriedAndRepeated
            (
                    Mono<JMSContext> contextM,
                    long maxAttempts, Duration minBackoff
            )
    {
        final Many<Reconnect> reconnects = Sinks.many().multicast().onBackpressureBuffer();
        return
                contextM.
                        retryWhen(backoff(maxAttempts, minBackoff)).flatMap(context ->
                                ops.setExceptionListener(context,
                                        exception ->
                                                nextReconnect(reconnects, ReactiveUtils::reportFailure)
                                ).<Mono<JMSContext>>map(
                                        Mono::error
                                ).orElse(
                                        Mono.just(context)
                                )
                        ).doOnEach(onEach("retried")).name("retried"
                        ).repeatWhen(
                                repeat ->
                                        reconnects.asFlux().
                                                doOnEach(onEach("reconnect")).name("reconnect")
                        ).doOnEach(onEach("repeated")).name("repeated");
    }

    private Flux<JMSConsumer> queueAndConsumer
            (
                    Flux<JMSContext> contextM,
                    String queueName
            )
    {
        return
                contextM.flatMap(context ->
                        ops.createQueue(context, queueName).flatMap(queue ->
                                ops.createConsumer(context, queue)
                        ).apply(
                                errorCreatingQueueOrConsumer -> {
                                    consume(
                                            ops.closeContext(context),
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
    }

    private <T> Flux<T> bodiesReceived
            (
                    Flux<JMSConsumer> consumerF,
                    Class<T> klass
            )
    {
        return
                consumerF.flatMap(consumer ->
                        Flux.<T>generate(sink ->
                                ops.receiveBody(consumer, klass).consume(
                                        sink::error,
                                        sink::next
                                )
                        )
                ).doOnEach(onEach("bodies")).name("bodies");
    }

    private <T> Flux<T> heardInConsumer
            (
                    Flux<JMSConsumer> consumerF,
                    ThrowingFunction<Message, T, JMSException> converter
            )
    {
        return
                consumerF.flatMap(consumer -> {
                            Many<T> convertedS = Sinks.many().unicast().onBackpressureBuffer();
                            consumer.setMessageListener(message ->
                                    ops.applyToMessage(message, converter).consume(
                                            exception ->
                                                    log.error("Error converting message {}", message, exception),
                                            converted ->
                                                    tryNextEmission(convertedS, converted, ReactiveUtils::reportFailure)
                                    )
                            );
                            return convertedS.asFlux().
                                    doOnEach(onEach("converted")).name("converted");
                        }
                ).doOnEach(onEach("published")).name("published");
    }
}
