package md.reactive_messaging.reactive;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Many;
import reactor.test.StepVerifier;

import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Message;

import static java.time.Duration.ofSeconds;
import static md.reactive_messaging.TestTibcoEmsConfig.*;
import static md.reactive_messaging.reactive.Reconnect.RECONNECT;
import static reactor.util.retry.Retry.backoff;

@Slf4j
final class ReconnectsTest
{

    @Test
    void skeleton()
    {
        Flux<?> f = Flux.empty().log();
        f.subscribe(
                success -> {
                },
                error -> {
                },
                () -> {
                }
        );

    }

    @Test
    void empty()
    {
        Flux<?> f = Flux.empty().log();
        StepVerifier.create(f).
                expectSubscription().
                verifyComplete();
    }

    @Test
    void error()
    {
        Flux<?> f = Flux.error(new RuntimeException()).log();
        StepVerifier.create(f).
                expectSubscription().
                verifyError();
    }

    @Test
    void just()
    {

        Flux<?> f = Flux.just().log();
        StepVerifier.create(f).
                expectSubscription().
                verifyComplete();
    }

    @Test
    void factory_can_be_instantiated_no_matter_if_broker_works()
    {
        final Mono<TibjmsConnectionFactory> factoryM =
                Mono.fromCallable(() ->
                        new TibjmsConnectionFactory(URL)
                ).log();
        StepVerifier.create(factoryM).
                expectSubscription().
                expectNextCount(1).
                verifyComplete();
    }

    @Test
    void creating_context()
    {
        final Mono<TibjmsConnectionFactory> factoryM =
                Mono.fromCallable(() ->
                        new TibjmsConnectionFactory(URL)
                ).log();
        final Mono<JMSContext> contextM =
                factoryM.flatMap(factory ->
                        Mono.fromCallable(() ->
                                factory.createContext(USER_NAME, PASSWORD)
                        )
                ).log();

        defaultSubscribe(contextM);
    }

    @Test
    void retrying_instantiating_factory() throws InterruptedException
    {
        final Mono<TibjmsConnectionFactory> factoryM =
                Mono.fromCallable(() ->
                        new TibjmsConnectionFactory(URL)
                ).log();
        final Many<Reconnect> reconnectsS =
                Sinks.many().unicast().onBackpressureBuffer();
        final Mono<JMSContext> contextM =
                factoryM.flatMap(factory ->
                        Mono.fromCallable(() -> {
                                    log.info("Creating context");
                                    final JMSContext context = factory.createContext(USER_NAME, PASSWORD);
                                    log.info("Created context");
                                    context.setExceptionListener(exception -> {
                                                final EmitResult result = reconnectsS.tryEmitNext(RECONNECT);
                                                log.error("Heard error in context {}, reconnect emitted {}", context, result, exception);
                                            }
                                    );
                                    return context;
                                }
                        )
                ).log();
        final Mono<JMSContext> retriedM =
                contextM.retryWhen(backoff(MAX_ATTEMPTS, MIN_BACKOFF)).log();
        final Flux<JMSContext> repeatedM =
                retriedM.repeatWhen(repeat -> reconnectsS.asFlux());

        defaultSubscribe(repeatedM);

        Thread.sleep(ofSeconds(3).toMillis());
    }

    @Test
    void listening_on_reconnects() throws InterruptedException
    {
        final Mono<TibjmsConnectionFactory> factoryM =
                Mono.fromCallable(() ->
                        new TibjmsConnectionFactory(URL)
                ).log();
        final Many<Reconnect> reconnectsS =
                Sinks.many().unicast().onBackpressureBuffer();
        final Mono<JMSContext> contextM =
                factoryM.flatMap(factory ->
                        Mono.fromCallable(() -> {
                                    log.info("Creating context");
                                    final JMSContext context = factory.createContext(USER_NAME, PASSWORD);
                                    log.info("Created context");
                                    context.setExceptionListener(exception -> {
                                                final EmitResult result = reconnectsS.tryEmitNext(RECONNECT);
                                                log.error("Heard error in context {}, reconnect emitted {}", context, result, exception);
                                            }
                                    );
                                    return context;
                                }
                        )
                ).log();
        final Mono<JMSContext> retriedM =
                contextM.retryWhen(backoff(MAX_ATTEMPTS, MIN_BACKOFF)).log();
        final Flux<JMSContext> repeatedM =
                retriedM.repeatWhen(repeat -> reconnectsS.asFlux());

        final Flux<JMSConsumer> consumerF =
                repeatedM.flatMap(context ->
                        Mono.fromCallable(() ->
                                context.createQueue(QUEUE_NAME)
                        ).flatMap(queue ->
                                Mono.fromCallable(() ->
                                        context.createConsumer(queue))
                        )
                );
        final Flux<Message> messagesF =
                consumerF.flatMap(consumer -> {
                            Many<Message> messagesS = Sinks.many().unicast().onBackpressureBuffer();
                            consumer.setMessageListener(message -> {
                                        final EmitResult result = messagesS.tryEmitNext(message);
                                        log.info("Heard message in consumer {}, emitted {}", consumer, result);
                                    }
                            );
                            return messagesS.asFlux();
                        }
                );


        defaultSubscribe(messagesF);
        Thread.sleep(ofSeconds(3).toMillis());
    }

    private static void defaultSubscribe(Mono<?> mono)
    {
        mono.subscribe(
                success ->
                        log.info("Mono success {}", success),
                error ->
                        log.error("Mono error", error),
                () ->
                        log.info("Mono completed")
        );
    }

    private static void defaultSubscribe(Flux<?> flux)
    {
        flux.subscribe(
                success ->
                        log.info("Flux success {}", success),
                error ->
                        log.error("Flux error", error),
                () ->
                        log.info("Flux completed")
        );
    }
}
