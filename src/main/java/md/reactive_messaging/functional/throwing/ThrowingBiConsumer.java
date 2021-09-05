package md.reactive_messaging.functional.throwing;

@FunctionalInterface
public interface ThrowingBiConsumer<T, U, E extends Exception>
{
    void accept(T t, U u) throws E;
}
