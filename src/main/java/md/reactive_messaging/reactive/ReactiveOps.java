package md.reactive_messaging.reactive;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.throwing.ThrowingFunction;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Scheduler;

import javax.jms.*;
import java.time.Duration;

import static md.reactive_messaging.reactive.ReactiveUtils.monitored;
import static md.reactive_messaging.reactive.Reconnect.RECONNECT;
import static reactor.core.scheduler.Schedulers.boundedElastic;
import static reactor.core.scheduler.Schedulers.newSingle;
import static reactor.util.retry.Retry.backoff;

@RequiredArgsConstructor
@Slf4j
public class ReactiveOps
{
    private static final EmitFailureHandler ALWAYS_RETRY = (signal, result) -> true;

    public <M> Flux<M> messages
            (
                    ThrowingFunction<String, ConnectionFactory, JMSException> connectionFactoryForUrl,
                    String url,
                    String userName, String password,
                    String queueName,
                    ThrowingFunction<Message, M, JMSException> converter,
                    long maxAttempts, Duration minBackoff, Duration maxBackoff
            )
    {
        Scheduler connection = newSingle("connection-publisher");
        Mono<JMSContext> contextM =
                connectionFactoryForUrl(connectionFactoryForUrl, url, connection).flatMap(factory ->
                        contextForCredentials(factory, userName, password, connection)
                );
        Many<Reconnect> reconnectS = Sinks.many().multicast().onBackpressureBuffer();
        Mono<JMSContext> exceptionListenedM =
                contextM.
                        doOnNext(context ->
                                setExceptionListener(context, reconnectS)
                        );
        Mono<JMSContext> monitoredExceptionListenedM = monitored(exceptionListenedM, "exception-listened-context");
        Mono<JMSContext> retriedM =
                monitoredExceptionListenedM.
                        retryWhen(
                                backoff(maxAttempts, minBackoff).
                                        maxBackoff(maxBackoff).
                                        doBeforeRetry(retry -> log.info("retrying {}", retry)).
                                        doAfterRetry(retry -> log.info("retried {}", retry))
                        );
        Mono<JMSContext> monitoredRetriedM = monitored(retriedM, "retried-context");
        Flux<JMSContext> repeatedF =
                monitoredRetriedM.
                        repeatWhen(repeat ->
                                {
                                    Flux<Reconnect> reconnectF =
                                            reconnectS.
                                                    asFlux().
                                                    subscribeOn(newSingle("reconnects-subscriber")).
                                                    publishOn(newSingle("reconnects-publisher"));
                                    return monitored(reconnectF, "reconnects");
                                }
                        );
        Flux<JMSContext> monitoredRepeatedF = monitored(repeatedF, "repeated-context");
        Flux<JMSConsumer> consumerF =
                monitoredRepeatedF.flatMap(context ->
                        Mono.just(
                                context.createQueue(queueName)
                        ).flatMap(queue ->
                                Mono.just(context.createConsumer(queue))
                        )
                );
        Flux<JMSConsumer> monitoredQueueConsumerF = monitored(consumerF, "queue-consumer");
        final Flux<M> messageF =
                monitoredQueueConsumerF.flatMap(consumer -> {
                            Many<M> messageS = Sinks.many().multicast().onBackpressureBuffer();
                            consumer.setMessageListener(message -> {
                                        try
                                        {
                                            final M converted = converter.apply(message);
                                            messageS.emitNext(converted, ALWAYS_RETRY);
                                        }
                                        catch (JMSException e)
                                        {
                                            log.error("Error extracting from message");
                                        }
                                    }
                            );
                            final Flux<M> messageHeardF = messageS.
                                    asFlux().
                                    subscribeOn(newSingle("messages-subscriber")).
                                    publishOn(newSingle("messages-publisher"));
                            return monitored(messageHeardF, "messages-heard");
                        }
                );
        return monitored(messageF, "messages");
    }

    Mono<ConnectionFactory> connectionFactoryForUrl
            (
                    ThrowingFunction<String, ConnectionFactory, JMSException> connectionFactoryForUrl,
                    String url,
                    Scheduler publisher
            )
    {
        final Mono<ConnectionFactory> wrapper =
                Mono.fromCallable(() ->
                        connectionFactoryForUrl.apply(url)
                );
        final Mono<ConnectionFactory> properlySubscribed =
                monitored(wrapper, "connection-factory-for-url-wrapper").
                        subscribeOn(boundedElastic()).
                        publishOn(publisher);
        return monitored(properlySubscribed, "connection-factory-for-url").
                cache();
    }

    Mono<JMSContext> contextForCredentials
            (
                    ConnectionFactory factory,
                    String userName, String password,
                    Scheduler publisher
            )
    {
        final Mono<JMSContext> wrapper =
                Mono.fromCallable(() ->
                        factory.createContext(userName, password)
                );
        final Mono<JMSContext> properlySubscribed =
                monitored(wrapper, "context-for-credentials-wrapper").
                        cache().
                        subscribeOn(boundedElastic()).
                        publishOn(publisher);
        return monitored(properlySubscribed, "context-for-credentials").
                cache();
    }

    void setExceptionListener(JMSContext context, Many<Reconnect> reconnectS)
    {
        String name = "setting-exception-listener";
        log.info("before {}", name);
        context.setExceptionListener(jmsException -> {
                    log.error("exception in context, closing - {}", jmsException.getMessage());
                    // context.close();
                    reconnectS.emitNext(RECONNECT, ALWAYS_RETRY);
                }
        );
        log.info("after {}", name);
    }
}
