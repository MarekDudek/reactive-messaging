package md.reactive_messaging.functional.throwing;

@FunctionalInterface
public interface ThrowingRunnable<E extends Exception>
{
    void run() throws E;
}
