package md.reactive_messaging.functional;

import java.util.Optional;
import java.util.function.Consumer;

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

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static <T> void consume(final Optional<T> optional, final Consumer<T> present, final Runnable absent)
    {
        if (optional.isPresent())
            present.accept(optional.get());
        else
            absent.run();
    }
}
