package md.reactive_messaging.utils;

@FunctionalInterface
public interface ThrowingRunnable<E extends Exception>
{
    void run() throws E;
}
