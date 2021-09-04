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

import static md.reactive_messaging.functional.LoggingFunctional.logRunnable;
import static md.reactive_messaging.reactive.ReactiveUtils.*;
import static md.reactive_messaging.reactive.Reconnect.*;
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
        Scheduler connectionScheduler = boundedElastic();

        Mono<ConnectionFactory> connectionFactoryM =
                fromCallable(
                        () ->
                                connectionFactoryForUrl.apply(url),
                        "Creating connection factory for URL"
                );
        Mono<ConnectionFactory> scheduledConnectionFactoryM =
                monitored(
                        connectionFactoryM.
                                subscribeOn(connectionScheduler).publishOn(connectionScheduler),
                        "Connection factory"
                );

        Many<Reconnect> reconnectS = Sinks.many().multicast().onBackpressureBuffer();

        Mono<JMSContext> contextM =
                scheduledConnectionFactoryM.flatMap(factory ->
                        contextForCredentials(factory, userName, password, reconnectS)
                );
        Mono<JMSContext> scheduledContextM =
                monitored(
                        contextM.
                                cache().
                                subscribeOn(connectionScheduler).publishOn(connectionScheduler),
                        "Context"
                );

        Mono<JMSContext> retriedM =
                monitored(
                        scheduledContextM.
                                retryWhen(backoff(maxAttempts, minBackoff).maxBackoff(maxBackoff).
                                        doBeforeRetry(retry -> log.info("Retrying {}", retry)).
                                        doAfterRetry(retry -> log.info("Retried {}", retry))
                                ),
                        "Possibly retried context"
                );

        Flux<Reconnect> reconnectF =
                monitored(
                        reconnectS.
                                asFlux().
                                subscribeOn(connectionScheduler).
                                publishOn(connectionScheduler),
                        "Reconnect request"
                );

        Flux<JMSContext> repeatedF =
                monitored(
                        retriedM.
                                repeatWhen(repeat -> reconnectF),
                        "Possibly repeated context"
                );

        Flux<JMSConsumer> consumerF =
                repeatedF.flatMap(context ->
                        fromCallable(
                                () -> {
                                    Queue queue = context.createQueue(queueName);
                                    return context.createConsumer(queue);
                                },
                                "Creating queue and consumer"
                        )
                );
        Flux<JMSConsumer> scheduledConsumer =
                monitored(
                        consumerF.
                                subscribeOn(connectionScheduler).
                                publishOn(connectionScheduler),
                        "Consumer of queue"
                );

        Scheduler messageScheduler = newSingle("Messages");

        Flux<M> messageF =
                monitored(scheduledConsumer, "queue-consumer").flatMap(consumer -> {
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
                                Flux<M> messageHeardF = messageS.
                                        asFlux().
                                        subscribeOn(messageScheduler).
                                        publishOn(messageScheduler);
                                return monitored(messageHeardF, "messages-heard");
                            }
                            catch (JMSRuntimeException errorSetting)
                            {
                                log.error("Failure setting message listener on consumer: '{}'", errorSetting.getMessage());
                                reconnectS.emitNext(AFTER_SETTING_MESSAGE_LISTENER_FAILED, ALWAYS_RETRY);
                                return Flux.error(errorSetting);
                            }
                        }
                );
        return monitored(messageF, "messages");
    }

    Mono<JMSContext> contextForCredentials
            (
                    ConnectionFactory factory,
                    String userName, String password,
                    Many<Reconnect> reconnectS
            )
    {
        return fromCallable(
                () -> {
                    JMSContext context;
                    try
                    {
                        context = factory.createContext(userName, password);
                    }
                    catch (Throwable t)
                    {
                        reconnectS.emitNext(AFTER_CREATING_CONTEXT_FAILED, ALWAYS_RETRY);
                        throw t;
                    }
                    try
                    {
                        context.setExceptionListener(exceptionHeard -> {
                                    log.error("Exception heard in context: '{}'", exceptionHeard.getMessage());
                                    logRunnable(
                                            context::close,
                                            throwable ->
                                                    reconnectS.emitNext(AFTER_EXCEPTION_IN_CONTEXT_HEARD, ALWAYS_RETRY),
                                            "Closing context after error heard"
                                    );
                                }
                        );
                    }
                    catch (Throwable t)
                    {
                        logRunnable(
                                context::close,
                                throwable ->
                                        reconnectS.emitNext(AFTER_SETTING_EXCEPTION_LISTENER_FAILED, ALWAYS_RETRY),
                                "Closing context after setting exception listener failed"
                        );
                        throw t;
                    }
                    return context;
                },
                throwable ->
                        log.error("Error creating context or setting exception listener", throwable),
                "Creating context for credentials and setting exception listener"
        );
    }
}
