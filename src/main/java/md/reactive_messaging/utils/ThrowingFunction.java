package md.reactive_messaging.utils;

@FunctionalInterface
public interface ThrowingFunction<T, R, E extends Exception>
{
    R apply(T t) throws E;
}
