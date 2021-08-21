package md.reactive_messaging.jms;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.utils.*;

import javax.jms.JMSException;

import static md.reactive_messaging.utils.Either.left;
import static md.reactive_messaging.utils.Either.right;
import static org.springframework.util.StringUtils.capitalize;

@Slf4j
enum JmsOpsHelper
{
    ;

    public static <T> Either<JMSException, T> consume
            (
                    final ThrowingConsumer<T, JMSException> consumer,
                    final T argument,
                    final String name
            )
    {
        try
        {
            log.info("Attempting '{}'", capitalize(name));
            consumer.accept(argument);
            log.info("Succeeded '{}'", capitalize(name));
            return right(argument);
        }
        catch (final JMSException e)
        {
            log.error("Failed '{}' - '{}'", capitalize(name), e.getMessage());
            return left(e);
        }
    }

    public static <R> Either<JMSException, R> get
            (
                    final ThrowingSupplier<R, JMSException> supplier,
                    final String name
            )
    {
        try
        {
            log.info("Attempting '{}'", capitalize(name));
            final R result = supplier.get();
            log.info("Succeeded '{}' with '{}'", capitalize(name), result);
            return right(result);
        }
        catch (final JMSException e)
        {
            log.error("Failed '{}' - '{}'", capitalize(name), e.getMessage());
            return left(e);
        }
    }

    public static <T, R> Either<JMSException, R> apply
            (
                    final ThrowingFunction<T, R, JMSException> function,
                    final T argument,
                    final String name
            )
    {
        try
        {
            log.info("Attempting '{}'", capitalize(name));
            final R result = function.apply(argument);
            log.info("Succeeded '{}' with '{}'", capitalize(name), result);
            return right(result);
        }
        catch (final JMSException e)
        {
            log.error("Failed '{}' - '{}'", capitalize(name), e.getMessage());
            return left(e);
        }
    }

    public static <T1, T2, R> Either<JMSException, R> apply
            (
                    final ThrowingBiFunction<T1, T2, R, JMSException> biFunction,
                    final T1 argument1,
                    final T2 argument2,
                    final String name
            )
    {
        try
        {
            log.info("Attempting '{}'", capitalize(name));
            final R result = biFunction.apply(argument1, argument2);
            log.info("Succeeded '{}' with '{}'", capitalize(name), result);
            return right(result);
        }
        catch (final JMSException e)
        {
            log.error("Failed '{}' - '{}'", capitalize(name), e.getMessage());
            return left(e);
        }
    }

    public static <T1, T2, T3, R> Either<JMSException, R> apply
            (
                    final ThrowingTriFunction<T1, T2, T3, R, JMSException> triFunction,
                    final T1 argument1,
                    final T2 argument2,
                    final T3 argument3,
                    final String name
            )
    {
        try
        {
            log.info("Attempting '{}'", capitalize(name));
            final R result = triFunction.apply(argument1, argument2, argument3);
            log.info("Succeeded '{}' with '{}'", capitalize(name), result);
            return right(result);
        }
        catch (final JMSException e)
        {
            log.error("Failed '{}' - '{}'", capitalize(name), e.getMessage());
            return left(e);
        }
    }
}
