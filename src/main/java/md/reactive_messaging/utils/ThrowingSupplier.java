package md.reactive_messaging.utils;

@FunctionalInterface
public interface ThrowingSupplier<R, E extends Exception>
{
    R get() throws E;
}
