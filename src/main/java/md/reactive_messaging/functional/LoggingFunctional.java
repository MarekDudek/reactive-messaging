package md.reactive_messaging.functional;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Supplier;

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
