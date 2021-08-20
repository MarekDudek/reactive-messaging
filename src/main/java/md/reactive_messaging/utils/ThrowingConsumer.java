package md.reactive_messaging.utils;

@FunctionalInterface
public interface ThrowingConsumer<T, E extends Exception>
{
    void accept(T t) throws E;
}
