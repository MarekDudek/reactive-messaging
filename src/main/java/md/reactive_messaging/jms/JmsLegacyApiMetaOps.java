package md.reactive_messaging.jms;

import md.reactive_messaging.functional.Either;
import md.reactive_messaging.functional.consumer.TriConsumer;
import md.reactive_messaging.functional.throwing.*;

import javax.jms.JMSException;
import java.util.Optional;
import java.util.function.BiConsumer;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static md.reactive_messaging.functional.Either.left;
import static md.reactive_messaging.functional.Either.right;
import static org.springframework.util.StringUtils.capitalize;

enum JmsLegacyApiMetaOps
{
    ;

    public static <R> Either<JMSException, R> supplieer
            (
                    ThrowingSupplier<R, JMSException> supplier,
                    String name,
                    BiConsumer<String, Object[]> log,
                    TriConsumer<String, Object, Object> failure
            )
    {
        try
        {
            log.accept("Attempting '{}'", asArray(capitalize(name)));
            final R result = supplier.get();
            log.accept("Succeeded '{}' with {}", asArray(capitalize(name), result));
            return right(result);
        }
        catch (JMSException e)
        {
            failure.accept("Failed '{}' - '{}'", capitalize(name), e.getMessage());
            return left(e);
        }
    }

    public static Optional<JMSException> runnable
            (
                    ThrowingRunnable<JMSException> runnable,
                    String name,
                    BiConsumer<String, Object[]> log,
                    TriConsumer<String, Object, Object> failure
            )
    {
        try
        {
            log.accept("Attempting '{}'", asArray(capitalize(name)));
            runnable.run();
            log.accept("Succeeded '{}'", asArray(capitalize(name)));
            return empty();
        }
        catch (JMSException e)
        {
            failure.accept("Failed '{}' - '{}'", capitalize(name), e.getMessage());
            return of(e);
        }
    }

    public static <T> Either<JMSException, T> consumer
            (
                    ThrowingConsumer<T, JMSException> consumer,
                    T argument,
                    String name,
                    BiConsumer<String, Object[]> log,
                    TriConsumer<String, Object, Object> failure
            )
    {
        try
        {
            log.accept("Attempting '{}' with {}", asArray(capitalize(name), argument));
            consumer.accept(argument);
            log.accept("Succeeded '{}'", asArray(capitalize(name)));
            return right(argument);
        }
        catch (JMSException e)
        {
            failure.accept("Failed '{}' - '{}'", capitalize(name), e.getMessage());
            return left(e);
        }
    }

    public static <T, R> Either<JMSException, R> function
            (
                    ThrowingFunction<T, R, JMSException> function,
                    T argument,
                    String name,
                    BiConsumer<String, Object[]> log,
                    TriConsumer<String, Object, Object> failure
            )
    {
        try
        {
            log.accept("Attempting '{}' with {}", asArray(capitalize(name), argument));
            final R result = function.apply(argument);
            log.accept("Succeeded '{}' with '{}'", asArray(capitalize(name), result));
            return right(result);
        }
        catch (JMSException e)
        {
            failure.accept("Failed '{}' - '{}'", capitalize(name), e.getMessage());
            return left(e);
        }
    }

    public static <T1, T2, R> Either<JMSException, R> biFunction
            (
                    ThrowingBiFunction<T1, T2, R, JMSException> biFunction,
                    T1 argument1,
                    T2 argument2,
                    String name,
                    BiConsumer<String, Object[]> log,
                    TriConsumer<String, Object, Object> failure
            )
    {
        try
        {
            log.accept("Attempting '{}' with {} and {}", asArray(capitalize(name), argument1, argument2));
            final R result = biFunction.apply(argument1, argument2);
            log.accept("Succeeded '{}' with '{}'", asArray(capitalize(name), result));
            return right(result);
        }
        catch (JMSException e)
        {
            failure.accept("Failed '{}' - '{}'", capitalize(name), e.getMessage());
            return left(e);
        }
    }

    private static Object[] asArray(Object... objects)
    {
        return objects;
    }
}