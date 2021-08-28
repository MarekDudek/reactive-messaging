package md.reactive_messaging.reactive;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.throwing.ThrowingRunnable;
import org.apache.commons.lang3.function.TriFunction;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.lang.Thread.currentThread;
import static md.reactive_messaging.reactive.GenericSubscribers.Utils.*;

@Slf4j
public enum GenericSubscribers
{
    ;

    public enum MonoSubscribers
    {
        ;

        public static <T> void simpleSubscribeAndForget(Mono<T> mono)
        {
            subscribeAndForget(mono::subscribe, Mono.class);
        }

        public static void subscribeAndAwait(Mono<?> mono) throws InterruptedException
        {
            subscribeAndLatch(mono::doOnTerminate, Mono::subscribe).
                    await();
        }

        public static <T> void subscribeAndAwait(Mono<T> mono, Subscriber<T> subscriber) throws InterruptedException
        {
            subscribeAndLatch(mono::doOnTerminate, p -> p.subscribe(subscriber)).
                    await();
        }
    }

    public enum FluxSubscribers
    {
        ;

        public static <T> void simpleSubscribeAndForget(Flux<T> flux)
        {
            subscribeAndForget(flux::subscribe, Flux.class);
        }

        public static void subscribeAndAwait(Flux<?> flux) throws InterruptedException
        {
            subscribeAndLatch(flux::doOnTerminate, Flux::subscribe).
                    await();
        }

        public static <T> void subscribeAndAwait(Flux<T> flux, Subscriber<T> subscriber) throws InterruptedException
        {
            subscribeAndLatch(flux::doOnTerminate, p -> p.subscribe(subscriber)).
                    await();
        }

        public static void subscribeOnAnotherThreadAndAwait(Flux<?> flux) throws InterruptedException
        {
            onAnotherThread(() ->
                            subscribeAndLatch(flux::doOnTerminate, Flux::subscribe).
                                    await()
                    , "flux-subscriber-default");
        }

        public static <T> void subscribeOnAnotherThreadAndAwait(Flux<T> flux, Subscriber<T> subscriber) throws InterruptedException
        {
            onAnotherThread(() ->
                            subscribeAndLatch(flux::doOnTerminate, Flux::subscribe).
                                    await()
                    , "flux-subscriber-custom");
        }
    }

    enum Utils
    {
        ;

        static <NEXT, ERROR extends Throwable> Disposable subscribeAndForget
                (
                        TriFunction<
                                Consumer<NEXT>,
                                Consumer<ERROR>,
                                Runnable,
                                Disposable
                                > consumer,
                        Class<? extends Publisher> klass
                )
        {
            String type = klass.getSimpleName().toLowerCase();
            return consumer.apply(
                    next -> log.info("{}-next '{}'", type, next),
                    error -> log.error("{}-error '{}'", type, error.getMessage()),
                    () -> log.info("{}-complete", type)
            );
        }

        static <PUBLISHER> CountDownLatch subscribeAndLatch
                (
                        Function<Runnable, PUBLISHER> doOnTerminate,
                        Consumer<PUBLISHER> subscribe
                )
        {
            final CountDownLatch latch = new CountDownLatch(1);
            final PUBLISHER publisher = doOnTerminate.apply(latch::countDown);
            subscribe.accept(publisher);
            return latch;
        }

        static void onAnotherThread
                (
                        ThrowingRunnable<InterruptedException> runnable,
                        String name
                ) throws InterruptedException
        {
            final Thread thread =
                    new Thread(
                            () -> {
                                try
                                {
                                    runnable.run();
                                }
                                catch (InterruptedException e)
                                {
                                    currentThread().interrupt();
                                }
                            },
                            name
                    );
            thread.start();
            thread.join();
        }
    }
}
