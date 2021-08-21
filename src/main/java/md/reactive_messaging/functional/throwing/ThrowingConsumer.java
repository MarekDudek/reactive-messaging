package md.reactive_messaging.functional.throwing;

@FunctionalInterface
public interface ThrowingConsumer<T, E extends Exception>
{
    void accept(T t) throws E;
}
