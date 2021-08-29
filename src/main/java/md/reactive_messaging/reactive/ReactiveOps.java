package md.reactive_messaging.reactive;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.throwing.ThrowingFunction;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Scheduler;

import javax.jms.*;
import java.time.Duration;

import static md.reactive_messaging.reactive.ReactiveUtils.ALWAYS_RETRY;
import static md.reactive_messaging.reactive.ReactiveUtils.monitored;
import static md.reactive_messaging.reactive.Reconnect.RECONNECT;
import static reactor.core.scheduler.Schedulers.boundedElastic;
import static reactor.core.scheduler.Schedulers.newSingle;
import static reactor.util.retry.Retry.backoff;

@RequiredArgsConstructor
@Slf4j
public class ReactiveOps
{

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
        Scheduler connectionPublisher = newSingle("connection-publisher");
        Mono<JMSContext> contextM =
                connectionFactoryForUrl(connectionFactoryForUrl, url, connectionPublisher).flatMap(factory ->
                        contextForCredentials(factory, userName, password, connectionPublisher)
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

        Scheduler reconnectsSubscriber = newSingle("reconnects-subscriber");
        Scheduler reconnectsPublisher = newSingle("reconnects-publisher");
        Flux<JMSContext> repeatedF =
                monitoredRetriedM.
                        repeatWhen(repeat ->
                                {
                                    Flux<Reconnect> reconnectF =
                                            reconnectS.
                                                    asFlux().
                                                    subscribeOn(reconnectsSubscriber).
                                                    publishOn(reconnectsPublisher);
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

        Scheduler messagesSubscriber = newSingle("messages-subscriber");
        Scheduler messagesPublisher = newSingle("messages-publisher");
        Flux<M> messageF =
                monitoredQueueConsumerF.flatMap(consumer -> {
                            Many<M> messageS = Sinks.many().multicast().onBackpressureBuffer();
                            try
                            {
                                log.info("Attempt setting message listener on consumer");
                                consumer.setMessageListener(message -> {
                                            try
                                            {
                                                log.trace("Attempt converting message {}", message);
                                                M converted = converter.apply(message);
                                                log.trace("Success converting message");

                                                log.trace("Attempt emitting converted {}", converted);
                                                messageS.emitNext(converted, ALWAYS_RETRY);
                                                log.trace("Success emitting converted");
                                            }
                                            catch (JMSException e)
                                            {
                                                log.error("Error converting message: '{}'", e.getMessage());
                                            }
                                        }
                                );
                                log.info("Success setting message listener on consumer");
                                final Flux<M> messageHeardF = messageS.
                                        asFlux().
                                        subscribeOn(messagesSubscriber).
                                        publishOn(messagesPublisher);
                                return monitored(messageHeardF, "messages-heard");
                            }
                            catch (JMSRuntimeException errorSetting)
                            {
                                log.error("Failure setting message listener on consumer: '{}'", errorSetting.getMessage());
                                log.info("Requesting reconnect after error setting message listener on consumer");
                                reconnectS.emitNext(RECONNECT, ALWAYS_RETRY);
                                return Flux.error(errorSetting);
                            }
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
        try
        {
            log.info("Attempt setting exception listener on context");
            context.setExceptionListener(
                    exceptionHeard -> {
                        log.error("Exception heard in context: '{}'", exceptionHeard.getMessage());
                        try
                        {
                            log.info("Attempt closing context after exception heard");
                            context.close();
                            log.info("Success closing context after exception heard");
                        }
                        catch (JMSRuntimeException errorClosing)
                        {
                            log.warn("Failure closing context after exception heard: '{}'", errorClosing.getMessage());
                        }
                        finally
                        {
                            log.info("Requesting reconnect after exception heard");
                            reconnectS.emitNext(RECONNECT, ALWAYS_RETRY);
                        }
                    }
            );
            log.info("Success setting exception listener on context");
        }
        catch (JMSRuntimeException errorSetting)
        {
            log.error("Failure setting exception listener on context: '{}'", errorSetting.getMessage());
            try
            {
                log.info("Attempt closing context after error setting exception listener");
                context.close();
                log.info("Success closing context after error setting exception listener");
            }
            catch (JMSRuntimeException errorClosing)
            {
                log.warn("Failure closing context after error setting exception listener: '{}'", errorClosing.getMessage());
            }
            finally
            {
                log.info("Requesting reconnect after error setting exception listener");
                reconnectS.emitNext(RECONNECT, ALWAYS_RETRY);
            }
            throw errorSetting;
        }
    }
}
