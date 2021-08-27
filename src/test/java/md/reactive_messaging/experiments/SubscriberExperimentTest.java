package md.reactive_messaging.experiments;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.throwing.ThrowingRunnable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.time.Duration.ofNanos;
import static java.time.Duration.ofSeconds;
import static java.util.Collections.nCopies;
import static md.reactive_messaging.functional.Functional.error;
import static md.reactive_messaging.reactive.GenericSubscribers.FluxSubscribers;
import static md.reactive_messaging.reactive.GenericSubscribers.FluxSubscribers.subscribeAndAwait;
import static md.reactive_messaging.reactive.GenericSubscribers.FluxSubscribers.subscribeAndAwaitAlt;
import static md.reactive_messaging.reactive.GenericSubscribers.MonoSubscribers.subscribeBusyWaitForDisposal;
import static md.reactive_messaging.reactive.GenericSubscribers.publisherSubscribeJoin;
import static org.apache.commons.lang3.StringUtils.repeat;
import static reactor.core.scheduler.Schedulers.*;

@TestMethodOrder(OrderAnnotation.class)
@Slf4j
final class SubscriberExperimentTest
{

    private static final RequestAllSubscriber<Object> REQUEST_ALL = new RequestAllSubscriber<>();

    @BeforeEach
    void setUp(TestInfo info)
    {
        log.info(repeat('#', 10) + " " + info.getDisplayName());
    }

    @Order(1)
    @DisplayName("No scheduling done")
    @Test
    void regular()
    {
        final Flux<Integer> flux = Flux.just(1, 2, 3);
        flux.subscribe(REQUEST_ALL);
    }

    @Order(2)
    @DisplayName("Published on different scheduler")
    @Test
    void publishedOn()
    {
        final Flux<Integer> flux =
                Flux.just(1, 2, 3).publishOn(newSingle("test-two"));
        flux.subscribe(REQUEST_ALL);
    }

    @Order(3)
    @DisplayName("Subscribed on different scheduler")
    @Test
    void subscribedOn_new_single()
    {
        final Flux<Integer> flux =
                Flux.just(1, 2, 3).subscribeOn(newSingle("test-three"));
        flux.subscribe(REQUEST_ALL);
    }

    @Order(4)
    @DisplayName("Subscribed on noop scheduler")
    @Test
    void subscribedOn_immediate()
    {
        final Flux<Integer> flux =
                Flux.just(1, 2, 3).subscribeOn(immediate());
        flux.subscribe(REQUEST_ALL);
    }


    @Order(5)
    @DisplayName("Failed trying to force subscribing earlier")
    @Test
    void subscribedOn_earlier_does_not_work()
    {
        final Flux<Integer> flux =
                Flux.just("").
                        subscribeOn(newSingle("test-four")).
                        flatMap(e -> Flux.just(1, 2, 3));
        flux.subscribe(REQUEST_ALL);
    }

    @Order(6)
    @DisplayName("Subscribing on self-created thread")
    @Test
    void running_in_separate_thread() throws InterruptedException
    {
        final Flux<Integer> flux =
                Flux.just(1, 2, 3).log();
        final Thread t = new Thread(
                () -> {
                    flux.subscribe(REQUEST_ALL);
                }, "separate");
        t.start();
        t.join();
    }

    @Order(7)
    @DisplayName("experiment")
    @Test
    void experiment() throws InterruptedException
    {
        final int items = 1_000;
        final Stream<Integer> stream = IntStream.iterate(items, i -> i - 1).boxed().limit(items);
        final Flux<Integer> sequence = Flux.fromStream(stream);
        final Flux<String> flux = sequence.
                subscribeOn(newParallel("par")).
                flatMap(n -> Flux.fromIterable(nCopies(n, n))).
                map(n -> {
                    log.trace("copy of n is {}", n);
                    return n;
                }).
                doOnNext(i -> {
                    log.trace("who will do it and when?");
                }).
                subscribeOn(newSingle("ser")).
                map(n -> n * n * n).
                map(n -> {
                    log.trace("n tripled is {}", n);
                    return n;
                }).
                subscribeOn(single()).
                map(n -> Integer.toString(n)).
                map(s -> {
                    log.trace("converted to string is '{}'", s);
                    return s;
                }).
                subscribeOn(immediate()).
                map(s -> format("%s has length %d", s, s.length())).
                map(s -> {
                    log.trace("formatted is '{}'", s);
                    return s;
                });

        final Runnable fluxBusyWait = () ->
                FluxSubscribers.subscribeBusyWaitForDisposal(flux);

        final Runnable monoBusyWait = () ->
                subscribeBusyWaitForDisposal(flux.last());


        final ThrowingRunnable<InterruptedException> fluxSleep = () -> {
            final Subscriber<String> subscriber =
                    FluxSubscribers.subscribeAndSleep(
                            flux,
                            new RequestAllSubscriber<>(),
                            s -> {
                                s.onNext("Right after subscribe");
                            },
                            ofSeconds(1)
                    );
            subscriber.onNext("After running");
        };

        final ThrowingRunnable<InterruptedException> joinSleep = () -> {
            publisherSubscribeJoin(flux, new RequestAllSubscriber<>());
            sleep(ofNanos(1).toMillis());
        };

        final Runnable block = () -> {
            Integer result = flux.
                    reduce(0, (i, n) -> i + 1).
                    block();
            log.info("Result is {}", result);
        };

        final Runnable blockLast = () -> {
            String last = flux.blockLast();
            log.info("Last is {}", last);
        };

        final Runnable lastBlockTimeout = () -> {
            Mono<String> last = flux.last();
            log.info("Last is {}", last.block(ofSeconds(1)));
        };

        final Runnable collectBlock = () -> {
            Mono<List<String>> collected = flux.collectList();
            final List<String> events = collected.block();
            log.info("Number of events {}", events.size());
        };

        final Runnable countBlock = () -> {
            Flux<String> transformed =
                    flux.
                            doOnNext(next -> log.info("next")).
                            doOnCancel(() -> log.warn("cancelled")).
                            doOnComplete(() -> log.info("complete")).
                            doOnError(error -> log.info("error", error)).
                            doOnDiscard(String.class, string ->
                                    error(new RuntimeException("discard"))
                            ).
                            doOnRequest(request -> log.info("request {}", request)).
                            doOnTerminate(() -> log.info("terminate"));
            log.info("Number of events {}", transformed.count().block());
        };

        final ThrowingRunnable<InterruptedException> r9 = () -> {
            subscribeAndAwait(flux);
        };

        final ThrowingRunnable<InterruptedException> r10 = () -> {
            subscribeAndAwaitAlt(flux);
        };

        r10.run();
    }

    @Order(8)
    @DisplayName("replicating of events works")
    @Test
    void replicating_works()
    {
        final Flux<Integer> flux =
                Flux.just(1, 2, 3).
                        flatMap(n -> Flux.fromIterable(nCopies(n, n)));
        flux.
                subscribe(n -> log.info("{}", n));
    }
}
