package md.reactive_messaging.functional;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public enum Functional
{
    ;

    public static <T> T error(final Throwable e)
    {
        throw new RuntimeException(e);
    }

    public static <T> T error()
    {
        throw new RuntimeException();
    }

    public static <T, R> Function<T, R> constant(R value)
    {
        return ignored -> value;
    }

    public static <T> Consumer<T> ignore()
    {
        return ignored -> {
        };
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static <T> void consume(final Optional<T> optional, final Consumer<T> present, final Runnable absent)
    {
        if (optional.isPresent())
            present.accept(optional.get());
        else
            absent.run();
    }
}
