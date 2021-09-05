package md.reactive_messaging.functional;

import lombok.NonNull;
import md.reactive_messaging.functional.throwing.ThrowingFunction;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public enum Functional
{
    ;

    public static <T> T error(@NonNull Throwable e)
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

    public static <T, R, E extends Exception> ThrowingFunction<T, R, E> toThrowing(Function<T, R> function)
    {
        return function::apply;
    }

    public static <T> Consumer<T> ignore()
    {
        return ignored -> {
        };
    }

    public static <A, B> Function<A, B> asFunction(@NonNull Consumer<A> c)
    {
        return a -> {
            c.accept(a);
            return null;
        };
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static <T> void consume(@NonNull Optional<T> optional, Consumer<T> present, Runnable absent)
    {
        if (optional.isPresent())
            present.accept(optional.get());
        else
            absent.run();
    }
}
