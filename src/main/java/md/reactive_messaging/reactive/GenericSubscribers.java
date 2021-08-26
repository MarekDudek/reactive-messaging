package md.reactive_messaging.reactive;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
public final class GenericSubscribers
{
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
}
