package md.reactive_messaging.reactive;

import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.CountDownLatch;

@Slf4j
public enum GenericSubscribers
{
    ;

    public enum MonoSubscribers
    {
        ;

        public static void simpleSubscribeAndForget(Mono<?> mono)
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

        public static void simpleSubscribeAndForget(Flux<?> flux)
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
    }
}
