package md.reactive_messaging.functional;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.throwing.ThrowingFunction;

import javax.annotation.Nonnull;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static md.reactive_messaging.functional.Functional.ignore;

@Slf4j
public enum LoggingFunctional
{
    ;

    public static <T> T logSupplier(@Nonnull Supplier<T> supplier, @Nonnull Consumer<Throwable> listener, @Nonnull String name)
    {
        try
        {
            log.info("Attempt {}", name);
            final T result = supplier.get();
            log.info("Success {}: {}", name, result);
            return result;
        }
        catch (Throwable t)
        {
            log.error("Failure {}: '{}'", name, t.getMessage());
            listener.accept(t);
            throw t;
        }
    }

    public static void logRunnable(@Nonnull Runnable runnable, @Nonnull Consumer<Throwable> listener, @Nonnull String name)
    {
        try
        {
            log.info("Attempt {}", name);
            runnable.run();
            log.info("Success {}", name);
        }
        catch (Throwable t)
        {
            log.error("Failure {}: '{}'", name, t.getMessage());
            listener.accept(t);
            throw t;
        }
    }

    public static void logRunnable(@Nonnull Runnable runnable, @Nonnull String name)
    {
        logRunnable(runnable, ignore(), name);
    }

    public static <T, R> R logThrowingFunction(@Nonnull ThrowingFunction<T, R, Exception> function, @Nonnull T argument, @Nonnull String name) throws Exception
    {
        try
        {
            log.info("Attempt {}", name);
            final R result = function.apply(argument);
            log.info("Success {}: {}", name, result);
            return result;
        }
        catch (Exception t)
        {
            log.error("Failure {}: '{}'", name, t.getMessage());
            throw t;
        }
    }

    public static <T> T logCallable(@Nonnull Callable<T> callable, @Nonnull Consumer<Throwable> listener, @Nonnull String name) throws Throwable
    {
        try
        {
            log.info("Attempt {}", name);
            final T result = callable.call();
            log.info("Success {}: {}", name, result);
            return result;
        }
        catch (Throwable t)
        {
            log.error("Failure {}: '{}'", name, t.getMessage());
            listener.accept(t);
            throw t;
        }
    }
}
