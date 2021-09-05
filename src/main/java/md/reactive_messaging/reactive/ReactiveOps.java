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
import static md.reactive_messaging.functional.LoggingFunctional.logThrowingFunction;
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

        Many<Reconnect> reconnectS = Sinks.many().unicast().onBackpressureBuffer();
        Many<M> messageS = Sinks.many().multicast().onBackpressureBuffer();

        Mono<JMSConsumer> consumerM = monitored(
                connectionFactoryM.flatMap(factory ->
                                contextAndAsyncListener(factory, userName, password, queueName, converter, reconnectS, messageS)
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


        Flux<M> messageF = monitored(
                messageS.
                        asFlux().
                        subscribeOn(messageSubscriber).
                        publishOn(messagePublisher),
                "Messages heard"
        );

        return monitored(
                repeatedF.flatMap(consumer -> messageF),
                "Messages emitted"
        );
    }

    <M> Mono<JMSConsumer> contextAndAsyncListener
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
                    try
                    {
                        JMSContext context = factory.createContext(userName, password);
                        context.setAutoStart(false);
                        try
                        {
                            context.setExceptionListener(exceptionHeard -> {
                                        log.error("Exception heard in context: '{}'", exceptionHeard.getMessage());
                                        logRunnable(context::close, "Closing context after error heard");
                                        reconnectS.emitNext(EXCEPTION_IN_CONTEXT_HEARD, ALWAYS_RETRY);
                                    }
                            );
                            Queue queue = context.createQueue(queueName);
                            JMSConsumer consumer = context.createConsumer(queue);
                            consumer.setMessageListener(message -> {
                                        try
                                        {
                                            M converted = logThrowingFunction(converter::apply, message, "Converting message");
                                            logRunnable(() -> messageS.emitNext(converted, ALWAYS_RETRY), "Emitting message");
                                        }
                                        catch (Exception e)
                                        {
                                            log.error("Error converting message: '{}'", e.getMessage());
                                        }
                                    }
                            );
                            context.start();
                            return consumer;
                        }
                        catch (Throwable t)
                        {
                            context.close();
                            reconnectS.emitNext(CREATING_ASYNC_LISTENER_FAILED, ALWAYS_RETRY);
                            throw t;
                        }
                    }
                    catch (Throwable t)
                    {
                        reconnectS.emitNext(CREATING_CONTEXT_FAILED, ALWAYS_RETRY);
                        throw t;
                    }
                }
        );
    }
}
