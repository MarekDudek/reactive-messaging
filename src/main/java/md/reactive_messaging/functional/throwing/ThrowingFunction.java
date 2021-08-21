package md.reactive_messaging.functional.throwing;

@FunctionalInterface
public interface ThrowingFunction<T, R, E extends Exception>
{
    R apply(T t) throws E;
}
