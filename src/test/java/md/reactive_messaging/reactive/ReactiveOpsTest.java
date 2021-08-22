package md.reactive_messaging.reactive;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.jms.Jms2Ops;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

import javax.jms.*;
import java.time.Duration;
import java.util.Optional;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Thread.sleep;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static md.reactive_messaging.TestTibcoEmsConfig.*;
import static reactor.util.retry.Retry.backoff;

@Slf4j
@TestMethodOrder(OrderAnnotation.class)
final class ReactiveOpsTest
{
    private static final Jms2Ops JOPS = new Jms2Ops();
    private static final ReactiveOps ROPS = new ReactiveOps(JOPS);

    private static final long MAX_ATTEMPTS = MAX_VALUE;
    private static final Duration MIN_BACKOFF = ofSeconds(1);
    private static final Duration TEST_DURATION = ofSeconds(30);

    @Order(1)
    @Test
    void connection_factory()
    {
        final Mono<ConnectionFactory> mono = ROPS.factoryFromCallable(TibjmsConnectionFactory::new, URL);
        mono.subscribe(factory ->
                log.info("{}", factory
                )
        );
    }

    @Order(2)
    @Test
    @Disabled
    void context()
    {
        final Mono<JMSContext> mono =
                ROPS.factoryFromCallable(TibjmsConnectionFactory::new, URL).flatMap(factory ->
                        ROPS.contextFromCallable(factory, USER_NAME, PASSWORD)
                );
        mono.subscribe(context -> {
                    log.info("{}", context);
                    JOPS.closeContext(context).ifPresent(e ->
                            log.error("Error while closing connection: ", e)
                    );
                }
        );
    }

    @Order(3)
    @Test
    @Disabled
    void contexts() throws InterruptedException
    {
        final Mono<ConnectionFactory> factoryM =
                ROPS.factoryFromCallable(TibjmsConnectionFactory::new, URL);
        final Many<Object> reconnectS = Sinks.many().unicast().onBackpressureBuffer();
        final Object reconnect = new Object();
        final Mono<JMSContext> contextM = factoryM.flatMap(factory ->
                ROPS.contextFromCallable(factory, USER_NAME, PASSWORD).map(context -> {
                    final Optional<JMSRuntimeException> settingError =
                            JOPS.setExceptionListener(context, error -> reconnectS.tryEmitNext(reconnect));
                    settingError.ifPresent(error -> reconnectS.tryEmitNext(reconnect));
                    return context;
                })
        );
        final Flux<JMSContext> contextsF = contextM.
                retryWhen(backoff(MAX_ATTEMPTS, ofMillis(100))).
                repeatWhen(repeat -> reconnectS.asFlux());

        new Thread(() ->
                contextsF.subscribe(
                        context -> log.info("Context: {}", context),
                        error -> log.error("Error: ", error),
                        () -> log.info("Completed")
                )
        ).start();

        sleep(TEST_DURATION.toMillis());
    }

    @Order(4)
    @Test
    @Disabled
    void contexts2() throws InterruptedException
    {
        final Many<Object> reconnectS = Sinks.many().unicast().onBackpressureBuffer();
        final Object reconnect = new Object();
        final Mono<JMSContext> contextM =
                ROPS.contextFromCallable2(URL, USER_NAME, PASSWORD).map(context -> {
                    final Optional<JMSRuntimeException> settingError =
                            JOPS.setExceptionListener(context, error -> reconnectS.tryEmitNext(reconnect));
                    settingError.ifPresent(error -> reconnectS.tryEmitNext(reconnect));
                    return context;
                });
        final Flux<JMSContext> contextsF = contextM.
                retryWhen(backoff(MAX_ATTEMPTS, MIN_BACKOFF)).
                repeatWhen(repeat -> reconnectS.asFlux());

        new Thread(() ->
                contextsF.subscribe(
                        context -> log.info("Context: {}", context),
                        error -> log.error("Error: ", error),
                        () -> log.info("Completed")
                )
        ).start();

        sleep(TEST_DURATION.toMillis());
    }

    @Order(5)
    @Test
    @Disabled
    void contexts3() throws InterruptedException
    {
        final Many<Object> reconnectS = Sinks.many().unicast().onBackpressureBuffer();
        final Object reconnect = new Object();
        final Mono<JMSContext> contextM =
                ROPS.createContext(TibjmsConnectionFactory::new, URL, USER_NAME, PASSWORD).map(context -> {
                    final Optional<JMSRuntimeException> settingError =
                            JOPS.setExceptionListener(context, error -> reconnectS.tryEmitNext(reconnect));
                    settingError.ifPresent(error -> reconnectS.tryEmitNext(reconnect));
                    return context;
                });
        final Flux<JMSContext> contextsF = contextM.
                retryWhen(backoff(MAX_ATTEMPTS, MIN_BACKOFF)).
                repeatWhen(repeat -> reconnectS.asFlux());

        new Thread(() ->
                contextsF.subscribe(
                        context -> log.info("Context: {}", context),
                        error -> log.error("Error: ", error),
                        () -> log.info("Completed")
                )
        ).start();

        sleep(TEST_DURATION.toMillis());
    }

    @Test
    void receiving_bodies_synchronously() throws InterruptedException
    {
        final Mono<ConnectionFactory> factoryM =
                ROPS.factoryFromCallable2(TibjmsConnectionFactory::new, URL);
        final Many<Object> reconnectS = Sinks.many().unicast().onBackpressureBuffer();
        final Object reconnect = new Object();
        final Mono<JMSContext> contextM =
                factoryM.flatMap(factory ->
                        ROPS.contextFromCallable3(factory, USER_NAME, PASSWORD).map(context -> {
                            JOPS.setExceptionListener(context, errorInContext -> {
                                        log.error("Detected error in context {}, trying to reconnect", context, errorInContext);
                                        reconnectS.tryEmitNext(reconnect);
                                    }
                            ).ifPresent(settingListenerError -> {
                                        log.error("Setting exception listener on context {} failed", context, settingListenerError);
                                        reconnectS.tryEmitNext(reconnect);
                                    }
                            );
                            return context;
                        })
                );
        final Flux<JMSContext> contextsF =
                contextM.
                        retryWhen(backoff(MAX_ATTEMPTS, ofMillis(100))).
                        repeatWhen(repeat -> reconnectS.asFlux());
        final Flux<JMSConsumer> consumersF =
                contextsF.flatMap(context ->
                        JOPS.createQueue(context, QUEUE_NAME).flatMap(queue ->
                                JOPS.createConsumer(context, queue)
                        ).apply(
                                error -> {
                                    reconnectS.tryEmitNext(reconnect);
                                    return Flux.error(error);
                                },
                                Flux::just
                        )
                );
        final Flux<String> bodiesF =
                consumersF.flatMap(consumer ->
                        Flux.generate(sink ->
                                JOPS.receiveBody(consumer, String.class).consume(
                                        sink::error,
                                        sink::next
                                )
                        )
                );

        new Thread(() ->
                bodiesF.subscribe(
                        body -> log.info("Body: {}", body),
                        error -> log.error("Error: ", error),
                        () -> log.info("Completed")
                )
        ).start();

        sleep(TEST_DURATION.toMillis());
    }

    @Test
    void receiving_bodies_asynchronously() throws InterruptedException
    {
        final Mono<ConnectionFactory> factoryM =
                ROPS.factoryFromCallable2(TibjmsConnectionFactory::new, URL);
        final Many<Object> reconnectS = Sinks.many().unicast().onBackpressureBuffer();
        final Object reconnect = new Object();
        final Mono<JMSContext> contextM =
                factoryM.flatMap(factory ->
                        ROPS.contextFromCallable3(factory, USER_NAME, PASSWORD).map(context -> {
                                    JOPS.setExceptionListener(context, errorInContext -> {
                                                log.error("Detected error in context {}, trying to reconnect", context, errorInContext);
                                                reconnectS.tryEmitNext(reconnect);
                                            }
                                    ).ifPresent(settingListenerError -> {
                                                log.error("Setting exception listener on context {} failed", context, settingListenerError);
                                                reconnectS.tryEmitNext(reconnect);
                                            }
                                    );
                                    return context;
                                }
                        )
                );
        final Flux<JMSContext> contextsF =
                contextM.
                        retryWhen(backoff(MAX_ATTEMPTS, ofMillis(100))).
                        repeatWhen(repeat -> reconnectS.asFlux());
        final Flux<JMSConsumer> consumersF =
                contextsF.flatMap(context ->
                        JOPS.createQueue(context, QUEUE_NAME).flatMap(queue ->
                                JOPS.createConsumer(context, queue)
                        ).apply(
                                error -> {
                                    reconnectS.tryEmitNext(reconnect);
                                    return Flux.error(error);
                                },
                                Flux::just
                        )
                );
        final Many<Message> messagesS = Sinks.many().unicast().onBackpressureBuffer();
        consumersF.doOnNext(consumer ->
                JOPS.setMessageListener(consumer, message -> {
                                    log.trace("Received {}", message);
                                    messagesS.tryEmitNext(message);
                                }
                        ).
                        ifPresent(settingListenerError -> {
                                    log.error("Setting message listener on consumer {} failed", consumer, settingListenerError);
                                    reconnectS.tryEmitNext(reconnect);
                                }
                        )
        ).subscribe();
        final Flux<Message> messagesF = messagesS.asFlux();

        new Thread(() ->
                messagesF.subscribe(
                        message -> log.info("Message: {}", message),
                        error -> log.error("Error: ", error),
                        () -> log.info("Completed")
                )
        ).start();

        sleep(TEST_DURATION.toMillis());
    }
}
