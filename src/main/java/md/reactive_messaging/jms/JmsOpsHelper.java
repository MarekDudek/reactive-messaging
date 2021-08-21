package md.reactive_messaging.jms;

import md.reactive_messaging.utils.*;

import javax.jms.JMSException;
import java.util.Optional;
import java.util.function.BiConsumer;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static md.reactive_messaging.utils.Either.left;
import static md.reactive_messaging.utils.Either.right;
import static org.springframework.util.StringUtils.capitalize;

enum JmsOpsHelper
{
    ;

    public static <R> Either<JMSException, R> get
            (
                    ThrowingSupplier<R, JMSException> supplier,
                    String name,
                    BiConsumer<String, Object> attempt,
                    Consumer3<String, Object, Object> success,
                    Consumer3<String, Object, Object> failure
            )
    {
        try
        {
            attempt.accept("Attempting '{}'", capitalize(name));
            final R result = supplier.get();
            success.accept("Succeeded '{}' with '{}'", capitalize(name), result);
            return right(result);
        }
        catch (JMSException e)
        {
            failure.accept("Failed '{}' - '{}'", capitalize(name), e.getMessage());
            return left(e);
        }
    }

    public static Optional<JMSException> run
            (
                    ThrowingRunnable<JMSException> runnable,
                    String name,
                    BiConsumer<String, Object> log,
                    Consumer3<String, Object, Object> failure
            )
    {
        try
        {
            log.accept("Attempting '{}'", capitalize(name));
            runnable.run();
            log.accept("Succeeded '{}'", capitalize(name));
            return empty();
        }
        catch (JMSException e)
        {
            failure.accept("Failed '{}' - '{}'", capitalize(name), e.getMessage());
            return of(e);
        }
    }

    public static <T> Either<JMSException, T> accept
            (
                    ThrowingConsumer<T, JMSException> consumer,
                    T argument,
                    String name,
                    Consumer3<String, Object, Object> attempt,
                    BiConsumer<String, Object> success,
                    Consumer3<String, Object, Object> failure
            )
    {
        try
        {
            attempt.accept("Attempting '{}' with {}", capitalize(name), argument);
            consumer.accept(argument);
            success.accept("Succeeded '{}'", capitalize(name));
            return right(argument);
        }
        catch (JMSException e)
        {
            failure.accept("Failed '{}' - '{}'", capitalize(name), e.getMessage());
            return left(e);
        }
    }

    public static <T, R> Either<JMSException, R> apply
            (
                    ThrowingFunction<T, R, JMSException> function,
                    T argument,
                    String name,
                    Consumer3<String, Object, Object> log,
                    Consumer3<String, Object, Object> failure
            )
    {
        try
        {
            log.accept("Attempting '{}' with {}", capitalize(name), argument);
            final R result = function.apply(argument);
            log.accept("Succeeded '{}' with '{}'", capitalize(name), result);
            return right(result);
        }
        catch (JMSException e)
        {
            failure.accept("Failed '{}' - '{}'", capitalize(name), e.getMessage());
            return left(e);
        }
    }

    public static <T1, T2, R> Either<JMSException, R> apply
            (
                    ThrowingBiFunction<T1, T2, R, JMSException> biFunction,
                    T1 argument1,
                    T2 argument2,
                    String name,
                    Consumer4<String, Object, Object, Object> attempt,
                    Consumer3<String, Object, Object> success,
                    Consumer3<String, Object, Object> failure
            )
    {
        try
        {
            attempt.accept("Attempting '{}' with {} and {}", capitalize(name), argument1, argument2);
            final R result = biFunction.apply(argument1, argument2);
            success.accept("Succeeded '{}' with '{}'", capitalize(name), result);
            return right(result);
        }
        catch (JMSException e)
        {
            failure.accept("Failed '{}' - '{}'", capitalize(name), e.getMessage());
            return left(e);
        }
    }
}