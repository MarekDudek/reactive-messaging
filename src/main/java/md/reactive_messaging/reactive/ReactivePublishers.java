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
        final Many<Reconnect> reconnects = Sinks.many().unicast().onBackpressureBuffer();
        final Flux<JMSContext> reliableF = retriedAndRepeated(contextM, reconnects, maxAttempts, minBackoff);
        final Flux<JMSConsumer> consumerF = queueAndConsumer(reliableF, reconnects, queueName);
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
        final Many<Reconnect> reconnects = Sinks.many().multicast().onBackpressureBuffer();
        final Flux<JMSContext> reliableF = retriedAndRepeated(contextM, reconnects, maxAttempts, minBackoff);
        final Flux<JMSConsumer> consumerF = queueAndConsumer(reliableF, reconnects, queueName);
        return heardInConsumer(consumerF, reconnects, converter);
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
                ).doOnEach(genericOnEach("factory")).name("factory"
                ).cache().flatMap(factory ->
                        ops.createContext(factory, userName, password).apply(
                                Mono::error,
                                Mono::just
                        )
                ).doOnEach(genericOnEach("context")).name("context");
    }

    private Flux<JMSContext> retriedAndRepeated
            (
                    Mono<JMSContext> contextM,
                    Many<Reconnect> reconnects,
                    long maxAttempts, Duration minBackoff
            )
    {
        return
                contextM.
                        retryWhen(
                                backoff(maxAttempts, minBackoff).
                                        doBeforeRetry(retry -> log.warn("will retry {}", retry)).
                                        doAfterRetry(retry -> log.warn("retried {}", retry))).flatMap(context ->
                                ops.setExceptionListener(context,
                                        exceptionHeard -> {
                                            log.error("exception heard in context '{}'", exceptionHeard.getMessage());
                                            reconnect(reconnects, ReactiveUtils::reportFailure);
                                        }
                                ).<Mono<JMSContext>>map(
                                        errorSettingListener -> {
                                            log.error("error setting listener: '{}'", errorSettingListener.getMessage());
                                            consume(
                                                    ops.closeContext(context),
                                                    errorClosing ->
                                                            log.warn("closing context failed: '{}'", errorClosing.getMessage()),
                                                    () ->
                                                            log.info("closing context succeeded")
                                            );
                                            reconnect(reconnects, ReactiveUtils::reportFailure);
                                            return Mono.error(errorSettingListener);
                                        }
                                ).orElse(
                                        Mono.just(context)
                                )
                        ).doOnEach(genericOnEach("retried")).name("retried"
                        ).repeatWhen(
                                repeat ->
                                        reconnects.asFlux().
                                                doOnEach(genericOnEach("reconnect")).name("reconnect")
                        ).doOnEach(genericOnEach("repeated")).name("repeated");
    }

    private Flux<JMSConsumer> queueAndConsumer
            (
                    Flux<JMSContext> contextM,
                    Many<Reconnect> reconnects,
                    String queueName
            )
    {
        return
                contextM.flatMap(context ->
                        ops.createQueue(context, queueName).flatMap(queue ->
                                ops.createConsumer(context, queue)
                        ).apply(
                                errorCreating -> {
                                    log.error("error creating queue or consumer: '{}'", errorCreating.getMessage());
                                    consume(
                                            ops.closeContext(context),
                                            errorClosing ->
                                                    log.warn("closing context failed: {}", errorClosing.getMessage()),
                                            () ->
                                                    log.info("closing context succeeded")
                                    );
                                    reconnect(reconnects, ReactiveUtils::reportFailure);
                                    return Flux.error(errorCreating);
                                },
                                Flux::just
                        )
                ).doOnEach(genericOnEach("consumers")).name("consumers");
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
                ).doOnEach(genericOnEach("bodies")).name("bodies");
    }

    private <T> Flux<T> heardInConsumer
            (
                    Flux<JMSConsumer> consumerF,
                    Many<Reconnect> reconnects,
                    ThrowingFunction<Message, T, JMSException> converter
            )
    {
        return
                consumerF.flatMap(consumer -> {
                            Many<T> sink = Sinks.many().multicast().onBackpressureBuffer();
                            return
                                    ops.setMessageListener(consumer, messageHeard ->
                                            ops.applyToMessage(messageHeard, converter).consume(
                                                    exception ->
                                                            log.error("error converting message {}", messageHeard, exception),
                                                    converted ->
                                                            emit(sink, converted, ReactiveUtils::reportFailure)
                                            )
                                    ).<Flux<T>>map(errorSettingListener -> {
                                                log.error("error setting message listener: '{}'", errorSettingListener.getMessage());
                                                reconnect(reconnects, ReactiveUtils::reportFailure);
                                                return Flux.error(errorSettingListener);
                                            }
                                    ).orElse(
                                            sink.asFlux().
                                                    doOnEach(genericOnEach("converted")).name("converted")
                                    );
                        }
                ).doOnEach(genericOnEach("published")).name("published");
    }
}
