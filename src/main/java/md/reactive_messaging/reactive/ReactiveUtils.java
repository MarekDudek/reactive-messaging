package md.reactive_messaging.reactive;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.Either;
import reactor.core.publisher.Signal;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Many;

import java.util.function.Consumer;

import static java.util.Optional.ofNullable;
import static md.reactive_messaging.functional.Either.right;
import static md.reactive_messaging.functional.Functional.error;
import static md.reactive_messaging.reactive.Reconnect.RECONNECT;
import static reactor.core.publisher.Sinks.EmitResult.OK;

@Slf4j
@RequiredArgsConstructor
public enum ReactiveUtils
{
    ;

    public static <T> Consumer<Signal<T>> onEach(String name)
    {
        return signal -> {
            switch (signal.getType())
            {
                case ON_NEXT:
                    log.info("NEXT     {} - {}", name, signal.get());
                    break;
                case ON_COMPLETE:
                    log.info("COMPLETE {}", name);
                    break;
                case ON_ERROR:
                    log.info("ERROR    {} - {}", name, ofNullable(signal.getThrowable()).map(
                            Throwable::getMessage
                    ).orElse(
                            "!NO MESSAGE!"
                    ));
                    break;
                default:
                    log.warn("Unknown signal - {}", signal);
                    break;
            }
        };
    }

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

    public static void reconnect
            (
                    Many<Reconnect> reconnect,
                    Consumer<Either<EmitResult, EmitResult>> continueC
            )
    {
        emit(reconnect, RECONNECT, continueC);
    }

    public static void reportFailure(Either<EmitResult, EmitResult> result)
    {
        result.consume(
                failure ->
                        log.error("Failed to emit {}", failure),
                success ->
                        log.info("Emitted OK")
        );
    }

    public static void failOnFailure(Either<EmitResult, EmitResult> result)
    {
        result.consume(
                failure ->
                        error(new IllegalStateException(failure.toString())),
                success ->
                        log.trace("Emitted")
        );
    }
}
