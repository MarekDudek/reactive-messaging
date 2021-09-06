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
import static md.reactive_messaging.reactive.GenericSubscribers.Utils.onAnotherThread;
import static md.reactive_messaging.reactive.GenericSubscribers.Utils.subscribeAndForget;

@Slf4j
public enum GenericSubscribers
{
    ;

    public enum MonoSubscribers
    {
        ;

        public static <T> Disposable simpleSubscribeAndForget(Mono<T> mono)
        {
            return subscribeAndForget(mono::subscribe, Mono.class);
        }

        public static void subscribeAndAwait(Mono<?> mono) throws InterruptedException
        {
            Utils.subscribeWithAndLatch(mono::doOnTerminate, Mono::subscribe).
                    await();
        }

        @Deprecated
        public static <T> void subscribeAndAwait(Mono<T> mono, Subscriber<T> subscriber) throws InterruptedException
        {
            Utils.subscribeWithAndLatch(mono::doOnTerminate, m -> m.subscribe(subscriber)).
                    await();
        }

        public static <T> CountDownLatch subscribeWithAndLatch(Mono<T> mono, Subscriber<T> subscriber)
        {
            return Utils.subscribeWithAndLatch(mono::doOnTerminate, m -> m.subscribe(subscriber));
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
            Utils.subscribeWithAndLatch(flux::doOnTerminate, Flux::subscribe).
                    await();
        }

        @Deprecated
        public static <T> void subscribeAndAwait(Flux<T> flux, Subscriber<T> subscriber) throws InterruptedException
        {
            Utils.subscribeWithAndLatch(flux::doOnTerminate, f -> f.subscribe(subscriber)).
                    await();
        }

        public static <T> CountDownLatch subscribeWithAndLatch(Flux<T> flux, Subscriber<T> subscriber)
        {
            return Utils.subscribeWithAndLatch(flux::doOnTerminate, f -> f.subscribe(subscriber));
        }

        @Deprecated
        public static void subscribeOnAnotherThreadAndAwait(Flux<?> flux) throws InterruptedException
        {
            onAnotherThread(() ->
                            Utils.subscribeWithAndLatch(flux::doOnTerminate, Flux::subscribe).
                                    await()
                    , "flux-subscriber-default");
        }

        @Deprecated
        public static <T> void subscribeOnAnotherThreadAndAwait(Flux<T> flux, Subscriber<T> subscriber) throws InterruptedException
        {
            onAnotherThread(() ->
                            Utils.subscribeWithAndLatch(flux::doOnTerminate, f -> f.subscribe(subscriber)).
                                    await()
                    , "flux-subscriber-custom");
        }
    }

    protected enum Utils
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
                        @SuppressWarnings("rawtypes")
                                Class<? extends Publisher> klass
                )
        {
            char type = klass.getSimpleName().charAt(0);
            return consumer.apply(
                    next -> log.info("{}-N '{}'", type, next),
                    error -> log.error("{}-E '{}'", type, error.getMessage()),
                    () -> log.info("{}-C", type)
            );
        }

        static <PUBLISHER> CountDownLatch subscribeWithAndLatch
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
