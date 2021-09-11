package md.reactive_messaging.reactive;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.core.publisher.Sinks.EmitFailureHandler;

import java.util.Optional;
import java.util.function.Consumer;

import static java.util.Optional.ofNullable;

@Slf4j
@RequiredArgsConstructor
public enum ReactiveUtils
{
    ;

    public static <T> EmitFailureHandler alwaysRetrySending(T emitted)
    {
        return (signal, result) -> {
            log.error("Emit failed: signal {}, result {} - retrying {}", signal, result, emitted);
            return true;
        };
    }

    public static <T> Consumer<Signal<T>> genericOnEach(String name)
    {
        return signal -> {
            switch (signal.getType())
            {
                case ON_NEXT:
                    log.info("N {}: {}", name, signal.get());
                    break;
                case ON_COMPLETE:
                    log.info("C {}", name);
                    break;
                case ON_ERROR:
                    final Optional<String> message = ofNullable(signal.getThrowable()).map(Throwable::getMessage);
                    log.error("E {}: {}", name, message.orElse("!!! no message !!!"));
                    break;
                default:
                    log.warn("U: {}", signal);
                    break;
            }
        };
    }

    public static Consumer<Subscription> genericOnSubscribe(String name)
    {
        return subscription ->
                log.info("S {}: {}", name, subscription);
    }

    public static <T> Mono<T> monitored(Mono<T> mono, String name)
    {
        return
                mono.
                        doOnSubscribe(genericOnSubscribe(name)).
                        doOnEach(genericOnEach(name)).
                        name(name);
    }

    public static <T> Flux<T> monitored(Flux<T> flux, String name)
    {
        return
                flux.
                        doOnSubscribe(genericOnSubscribe(name)).
                        doOnEach(genericOnEach(name)).
                        name(name);
    }
}
