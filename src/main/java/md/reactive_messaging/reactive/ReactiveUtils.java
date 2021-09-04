package md.reactive_messaging.reactive;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.Either;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Many;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import static java.util.Optional.ofNullable;
import static md.reactive_messaging.functional.Either.right;
import static md.reactive_messaging.functional.Functional.error;
import static md.reactive_messaging.functional.Functional.ignore;
import static md.reactive_messaging.functional.LoggingFunctional.logCallable;
import static md.reactive_messaging.reactive.Reconnect.RECONNECT;
import static reactor.core.publisher.Sinks.EmitResult.OK;

@Slf4j
@RequiredArgsConstructor
public enum ReactiveUtils
{
    ;

    public static final EmitFailureHandler ALWAYS_RETRY =
            (signal, result) ->
            {
                log.error("Emit failed: signal {}, result {} - retrying", signal, result);
                return true;
            };

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

    @Deprecated
    public static <T> void emit
            (
                    Many<T> sink,
                    T t,
                    Consumer<Either<EmitResult, EmitResult>> continueC
            )
    {
        log.info("Emitting {}", t);
        final EmitResult emitted = sink.tryEmitNext(t);
        final Either<EmitResult, EmitResult> result =
                right(emitted).filter(r -> r == OK);
        continueC.accept(result);
    }

    @Deprecated
    public static void reconnect
            (
                    Many<Reconnect> reconnect,
                    Consumer<Either<EmitResult, EmitResult>> continueC
            )
    {
        emit(reconnect, RECONNECT, continueC);
    }

    @Deprecated
    public static void reportFailure(Either<EmitResult, EmitResult> result)
    {
        result.consume(
                failure ->
                        log.error("Failed to emit {}", failure),
                success ->
                        log.info("Emitted OK")
        );
    }

    @Deprecated
    public static void failOnFailure(Either<EmitResult, EmitResult> result)
    {
        result.consume(
                failure ->
                        error(new IllegalStateException(failure.toString())),
                success ->
                        log.trace("Emitted")
        );
    }

    public static <T> Mono<T> fromCallable(Callable<T> callable, Consumer<Throwable> listener, String name)
    {
        return Mono.fromCallable(() -> {
                    try
                    {
                        return logCallable(callable, listener, name);
                    }
                    catch (Throwable e)
                    {
                        throw Exceptions.bubble(e);
                    }
                }
        );
    }

    public static <T> Mono<T> fromCallable(Callable<T> callable, String name)
    {
        return fromCallable(callable, ignore(), name);
    }
}
