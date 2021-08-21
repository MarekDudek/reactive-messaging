package md.reactive_messaging.functional.throwing;

@FunctionalInterface
public interface ThrowingSupplier<R, E extends Exception>
{
    R get() throws E;
}
