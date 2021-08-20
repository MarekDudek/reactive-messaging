package md.reactive_messaging.utils;

@FunctionalInterface
public interface ThrowingTriFunction<T1, T2, T3, R, E extends Exception>
{
    R apply(T1 t1, T2 t2, T3 t3) throws E;
}
