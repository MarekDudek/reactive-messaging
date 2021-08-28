package md.reactive_messaging.experiments;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.throwing.ThrowingRunnable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.time.Duration.ofSeconds;
import static java.util.Collections.nCopies;
import static md.reactive_messaging.functional.Functional.error;
import static md.reactive_messaging.reactive.GenericSubscribers.FluxSubscribers.subscribeAndAwait;
import static md.reactive_messaging.reactive.GenericSubscribers.FluxSubscribers.subscribeOnAnotherThreadAndAwait;
import static md.reactive_messaging.reactive.GenericSubscribers.MonoSubscribers.subscribeAndAwait;
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
        final int startCount = 10;
        final Stream<Integer> countDownFromStartCountToOne = IntStream.iterate(startCount, i -> i - 1).boxed().limit(startCount);
        final Stream<Integer> firstIntegersTill = IntStream.rangeClosed(1, 10).boxed();
        final Flux<Integer> sequence = Flux.fromStream(firstIntegersTill);
        final Flux<String> flux = sequence.

                subscribeOn(newParallel("subscriber")).

                publishOn(newParallel("multiplier")).
                flatMap(n -> Flux.fromIterable(nCopies(n, n))).
                map(n -> {
                    log.trace("copy of n is {}", n);
                    return n;
                }).
                doOnNext(n -> log.trace("next copy is {}", n)).

                publishOn(newSingle("tripler")).
                map(n -> n * n * n).
                map(n -> {
                    log.trace("n tripled is {}", n);
                    return n;
                }).
                doOnNext(n -> log.info("next tripled is {}", n)).

                publishOn(newBoundedElastic(1, 1, "converter")).
                map(n -> Integer.toString(n)).
                map(s -> {
                    log.trace("converted to string is '{}'", s);
                    return s;
                }).
                doOnNext(s -> log.trace("next converted is '{}'", s)).

                publishOn(Schedulers.newParallel("formatter")).
                map(s -> format("%s has length %d", s, s.length())).
                map(s -> {
                    log.trace("formatted is '{}'", s);
                    return s;
                }).
                doOnNext(s -> log.trace("next formatted is {}", s));

        final Runnable block = () ->
                log.info("Result is {}",
                        flux.
                                reduce(0, (i, n) -> i + 1).
                                block());

        final Runnable blockLast = () ->
                log.info("Last is {}",
                        flux.
                                blockLast());

        final Runnable lastBlockTimeout = () ->
                log.info("Last is {}",
                        flux.
                                last().
                                block(ofSeconds(1)));

        final Runnable collectBlock = () ->
                log.info("Number of events {}",
                        flux.
                                collectList().
                                block().
                                size());

        final Runnable countBlock = () ->
                log.info("Number of events {}",
                        flux.
                                doOnNext(next -> log.info("next")).
                                doOnCancel(() -> log.warn("cancelled")).
                                doOnComplete(() -> log.info("complete")).
                                doOnError(error -> log.info("error", error)).
                                doOnDiscard(String.class, string -> error(new RuntimeException("discard"))).
                                doOnRequest(request -> log.info("request {}", request)).
                                doOnTerminate(() -> log.info("terminate")).
                                count().block());

        final ThrowingRunnable<InterruptedException> monoSubscribedDefault = () ->
                subscribeAndAwait(
                        flux.
                                doOnSubscribe(s -> log.info("subscribed")).
                                last()
                );

        final ThrowingRunnable<InterruptedException> fluxSubscribedDefault = () ->
                subscribeAndAwait(
                        flux.
                                doOnSubscribe(s -> log.info("subscribed"))
                );

        final ThrowingRunnable<InterruptedException> monoSubscribedCustom = () ->
                subscribeAndAwait(
                        flux.
                                doOnSubscribe(s -> log.info("subscribed")).
                                last(), new RequestAllSubscriber<>());

        final ThrowingRunnable<InterruptedException> fluxSubscribedCustom = () ->
                subscribeAndAwait(
                        flux.
                                doOnSubscribe(s -> log.info("subscribed")),
                        new RequestAllSubscriber<>()
                );

        final ThrowingRunnable<InterruptedException> fluxSubscribedOnAnotherThread = () ->
                subscribeOnAnotherThreadAndAwait(
                        flux.
                                doOnSubscribe(s -> log.info("subscribed"))
                );

        fluxSubscribedDefault.run();
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
