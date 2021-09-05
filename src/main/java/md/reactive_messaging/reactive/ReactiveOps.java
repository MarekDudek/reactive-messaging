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
import static md.reactive_messaging.reactive.ReactiveUtils.alwaysRetrySending;
import static md.reactive_messaging.reactive.ReactiveUtils.monitored;
import static md.reactive_messaging.reactive.Reconnect.CREATING_ASYNC_LISTENER_FAILED;
import static md.reactive_messaging.reactive.Reconnect.EXCEPTION_IN_CONTEXT_HEARD;
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
        Scheduler connectionSubscriber = boundedElastic();
        Scheduler connectionPublisher = newSingle("connection-publisher");

        Mono<ConnectionFactory> connectionFactoryM = monitored(
                Mono.fromCallable(() ->
                                connectionFactoryForUrl.apply(url)
                        ).
                        subscribeOn(connectionSubscriber).
                        publishOn(connectionPublisher).
                        cache(),
                "Connection factory"
        );

        Many<Reconnect> reconnectS = Sinks.many().multicast().onBackpressureBuffer();
        Flux<Reconnect> reconnectF = monitored(
                reconnectS.
                        asFlux().
                        subscribeOn(connectionPublisher).
                        publishOn(connectionPublisher),
                "Reconnect request"
        );

        Mono<JMSConsumer> consumerM = monitored(
                connectionFactoryM.flatMap(factory ->
                                contextAndSyncReceiver(factory, userName, password, queueName, reconnectS)
                        ).
                        subscribeOn(connectionSubscriber).
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

        Flux<JMSConsumer> repeatedF = monitored(
                retriedM.
                        repeatWhen(repeat -> reconnectF),
                "Possibly repeated context"
        );

        Scheduler messageSubscriber = newSingle("Message Subscriber");
        Scheduler messagePublisher = newSingle("Message Publisher");

        return monitored(
                repeatedF.flatMap(consumer ->
                        Flux.<M>generate(sink -> {
                                            try
                                            {
                                                Message message = consumer.receive();
                                                M converted = converter.apply(message);
                                                sink.next(converted);
                                            }
                                            catch (Exception e)
                                            {
                                                sink.complete();
                                            }
                                        }
                                ).
                                subscribeOn(messageSubscriber).
                                publishOn(messagePublisher)
                ),
                "Messages emitted"
        );
    }

    Mono<JMSConsumer> contextAndSyncReceiver
            (
                    ConnectionFactory factory,
                    String userName, String password,
                    String queueName,
                    Many<Reconnect> reconnectS
            )
    {
        return Mono.fromCallable(
                () -> {
                    JMSContext context = factory.createContext(userName, password);
                    try
                    {
                        context.setExceptionListener(exceptionHeard -> {
                                    log.error("Exception heard in context: '{}'", exceptionHeard.getMessage());
                                    logRunnable(context::close, "Closing context after error heard");
                                    reconnectS.emitNext(EXCEPTION_IN_CONTEXT_HEARD, alwaysRetrySending(EXCEPTION_IN_CONTEXT_HEARD));
                                    log.error("Emitted");
                                }
                        );
                        Queue queue = context.createQueue(queueName);
                        return context.createConsumer(queue);
                    }
                    catch (Throwable t)
                    {
                        context.close();
                        reconnectS.emitNext(CREATING_ASYNC_LISTENER_FAILED, alwaysRetrySending(CREATING_ASYNC_LISTENER_FAILED));
                        throw t;
                    }
                }
        );
    }
}
