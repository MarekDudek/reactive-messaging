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
import static md.reactive_messaging.reactive.ReactiveUtils.ALWAYS_RETRY;
import static md.reactive_messaging.reactive.ReactiveUtils.monitored;
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
        Scheduler connectionPublisher = newSingle("connection-publisher");

        Mono<ConnectionFactory> connectionFactoryM = monitored(
                Mono.fromCallable(() ->
                                connectionFactoryForUrl.apply(url)
                        ).
                        subscribeOn(boundedElastic()).
                        publishOn(connectionPublisher).
                        cache(),
                "Connection factory"
        );

        Many<Reconnect> reconnectS = Sinks.many().multicast().onBackpressureBuffer();
        Many<M> messageS = Sinks.many().multicast().onBackpressureBuffer();

        Mono<JMSConsumer> consumerM = monitored(
                connectionFactoryM.flatMap(factory ->
                                contextForCredentialsQueueAndConsumer(factory, userName, password, queueName, converter, reconnectS, messageS)
                        ).
                        subscribeOn(boundedElastic()).
                        publishOn(connectionPublisher),
                "Context"
        );

        Mono<JMSConsumer> retriedM = monitored(
                consumerM.
                        retryWhen(backoff(maxAttempts, minBackoff).maxBackoff(maxBackoff).
                                doBeforeRetry(retry -> log.info("Retrying {}", retry)).
                                doAfterRetry(retry -> log.info("Retried {}", retry))
                        ),
                "Possibly retried context"
        );

        Scheduler reconnectsSubscriber = newSingle("reconnects-subscriber");
        Scheduler reconnectsPublisher = newSingle("reconnects-publisher");

        Flux<JMSConsumer> repeatedF = monitored(
                retriedM.
                        repeatWhen(repeat -> monitored(
                                reconnectS.
                                        asFlux().
                                        subscribeOn(reconnectsSubscriber).
                                        publishOn(reconnectsPublisher),
                                "Reconnect request"
                        )),
                "Possibly repeated context"
        );


        Scheduler messageSubscriber = newSingle("Message Subscriber");
        Scheduler messagePublisher = newSingle("Message Publisher");


        Flux<M> messageHeardF = monitored(
                messageS.
                        asFlux().
                        subscribeOn(messageSubscriber).
                        publishOn(messagePublisher),
                "Messages heard"
        );

        return monitored(
                repeatedF.flatMap(consumer -> messageHeardF),
                "Messages emitted"
        );
    }

    <M> Mono<JMSConsumer> contextForCredentialsQueueAndConsumer
            (
                    ConnectionFactory factory,
                    String userName, String password,
                    String queueName,
                    ThrowingFunction<Message, M, JMSException> converter,
                    Many<Reconnect> reconnectS, Many<M> messageS
            )
    {
        return Mono.fromCallable(
                () -> {

                    JMSContext context;
                    try
                    {
                        context = factory.createContext(userName, password);
                        context.setAutoStart(false);
                    }
                    catch (Throwable t)
                    {
                        reconnectS.emitNext(CREATING_CONTEXT_FAILED, ALWAYS_RETRY);
                        throw t;
                    }

                    try
                    {
                        context.setExceptionListener(exceptionHeard -> {
                                    log.error("Exception heard in context: '{}'", exceptionHeard.getMessage());
                                    logRunnable(
                                            context::close,
                                            "Closing context after error heard"
                                    );
                                    reconnectS.emitNext(EXCEPTION_IN_CONTEXT_HEARD, ALWAYS_RETRY);
                                }
                        );
                    }
                    catch (Throwable t)
                    {
                        logRunnable(
                                context::close,
                                "Closing context after setting exception listener failed"
                        );
                        reconnectS.emitNext(SETTING_EXCEPTION_LISTENER_FAILED, ALWAYS_RETRY);
                        throw t;
                    }

                    Queue queue;
                    try
                    {
                        queue = context.createQueue(queueName);
                    }
                    catch (Throwable t)
                    {
                        logRunnable(
                                context::close,
                                "Closing context after creating queue failed"
                        );
                        reconnectS.emitNext(CREATING_QUEUE_FAILED, ALWAYS_RETRY);
                        throw t;
                    }

                    JMSConsumer consumer;
                    try
                    {
                        consumer = context.createConsumer(queue);
                    }
                    catch (Throwable t)
                    {
                        logRunnable(
                                context::close,
                                "Closing context after creating consumer failed"
                        );
                        reconnectS.emitNext(CREATING_CONSUMER_FAILED, ALWAYS_RETRY);
                        throw t;
                    }

                    try
                    {
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
                    }
                    catch (Throwable t)
                    {
                        logRunnable(
                                context::close,
                                "Closing context after setting message listener failed"
                        );
                        reconnectS.emitNext(SETTING_MESSAGE_LISTENER, ALWAYS_RETRY);
                        throw t;
                    }

                    try
                    {
                        context.start();
                    }
                    catch (Throwable t)
                    {
                        logRunnable(
                                context::close,
                                "Closing context after starting context failed"
                        );
                        reconnectS.emitNext(STARTING_CONTEXT, ALWAYS_RETRY);
                        throw t;
                    }

                    return consumer;
                }
        );
    }
}
