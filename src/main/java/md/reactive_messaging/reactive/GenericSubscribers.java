package md.reactive_messaging.reactive;

import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

import static java.lang.Thread.sleep;

@Slf4j
public enum GenericSubscribers
{
    ;

    public static <T> void publisherSubscribeJoin
            (
                    Publisher<T> flux,
                    Subscriber<T> subscriber
            ) throws InterruptedException
    {
        final Thread t = new Thread(() -> flux.subscribe(subscriber), "separate");
        t.start();
        t.join();
    }

    public enum MonoSubscribers
    {
        ;

        public static void defaultSubscriber(Mono<?> mono)
        {
            mono.subscribe(
                    success ->
                            log.info("[M] Success {}", success),
                    error ->
                            log.error("[M] Error", error),
                    () ->
                            log.info("[M] Completed")
            );
        }

        @Deprecated
        public static void subscribeBusyWaitForDisposal(Mono<?> mono)
        {
            final Disposable disposable = mono.subscribe();
            while (!disposable.isDisposed())
                ;
        }

        public static void subscribeAndAwait(Mono<?> mono) throws InterruptedException
        {
            CountDownLatch latch = new CountDownLatch(1);
            mono.
                    doOnTerminate(latch::countDown).
                    subscribe();
            latch.await();
        }

        public static <T> void subscribeAndAwait(Mono<T> flux, Subscriber<T> subscriber) throws InterruptedException
        {
            CountDownLatch latch = new CountDownLatch(1);
            flux.
                    doOnTerminate(latch::countDown).
                    subscribe(subscriber);
            latch.await();
        }
    }

    public enum FluxSubscribers
    {
        ;

        public static void defaultSubscriber(Flux<?> flux)
        {
            flux.subscribe(
                    success ->
                            log.info("[F] Success {}", success),
                    error ->
                            log.error("[F] Error", error),
                    () ->
                            log.info("[F] Completed")
            );
        }

        @Deprecated
        public static void subscribeBusyWaitForDisposal(Flux<?> flux)
        {
            final Disposable disposable = flux.subscribe();
            while (!disposable.isDisposed())
                ;
        }

        public static void subscribeAndAwait(Flux<?> flux) throws InterruptedException
        {
            CountDownLatch latch = new CountDownLatch(1);
            flux.
                    doOnTerminate(latch::countDown).
                    subscribe();
            latch.await();
        }

        public static <T> void subscribeAndAwait(Flux<T> flux, Subscriber<T> subscriber) throws InterruptedException
        {
            CountDownLatch latch = new CountDownLatch(1);
            flux.
                    doOnTerminate(latch::countDown).
                    subscribe(subscriber);
            latch.await();
        }

        @Deprecated
        public static void subscribeAndAwaitAlt(Flux<?> flux) throws InterruptedException
        {
            final Semaphore semaphore = new Semaphore(0);
            flux.doOnTerminate(semaphore::release).subscribe();
            semaphore.acquire();
        }

        @Deprecated
        public static <T> Subscriber<T> subscribeAndSleep(
                Flux<T> flux,
                Subscriber<T> subscriber,
                Consumer<Subscriber<T>> after,
                Duration duration) throws InterruptedException
        {
            final Subscriber<T> with = flux.subscribeWith(subscriber);
            after.accept(with);
            sleep(duration.toMillis());
            return with;
        }
    }
}
