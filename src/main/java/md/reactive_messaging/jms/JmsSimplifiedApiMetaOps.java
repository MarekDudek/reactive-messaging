package md.reactive_messaging.jms;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.Either;
import md.reactive_messaging.functional.throwing.ThrowingFunction;

import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import java.util.Optional;
import java.util.function.*;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static md.reactive_messaging.functional.Either.left;
import static md.reactive_messaging.functional.Either.right;

@Slf4j
public enum JmsSimplifiedApiMetaOps
{
    ;

    public static <A, R> Either<JMSRuntimeException, R> function
            (
                    Function<A, R> f,
                    A a,
                    String name,
                    BiConsumer<String, Object[]> out,
                    BiConsumer<String, Object[]> err
            )
    {
        try
        {
            out.accept("Attempt {} ({})", asArray(name, a));
            final R r = f.apply(a);
            out.accept("Success {} {}", asArray(name, r));
            return right(r);
        }
        catch (JMSRuntimeException e)
        {
            err.accept("Failure {}", asArray(name, e));
            return left(e);
        }
    }

    public static <A, R> Either<JMSException, R> throwingFunction
            (
                    ThrowingFunction<A, R, JMSException> f,
                    A a,
                    String name,
                    BiConsumer<String, Object[]> out,
                    BiConsumer<String, Object[]> err
            )
    {
        try
        {
            out.accept("Attempt {} ({})", asArray(name, a));
            final R r = f.apply(a);
            out.accept("Success {} {}", asArray(name, r));
            return right(r);
        }
        catch (JMSException e)
        {
            err.accept("Failure {}", asArray(name, e));
            return left(e);
        }
    }

    public static <A, B, R> Either<JMSRuntimeException, R> biFunction
            (
                    BiFunction<A, B, R> f,
                    A a,
                    B b,
                    String name,
                    BiConsumer<String, Object[]> out,
                    BiConsumer<String, Object[]> err
            )
    {
        try
        {
            out.accept("Attempt {} ({}, {})", asArray(name, a, b));
            final R r = f.apply(a, b);
            out.accept("Success {} {}", asArray(name, r));
            return right(r);
        }
        catch (JMSRuntimeException e)
        {
            err.accept("Failure {}", asArray(name, e));
            return left(e);
        }
    }

    public static <A> Optional<JMSRuntimeException> consumer
            (
                    Consumer<A> c,
                    A a,
                    String name,
                    BiConsumer<String, Object[]> out,
                    BiConsumer<String, Object[]> err
            )
    {
        try
        {
            out.accept("Attempt {} ({})", asArray(name, a));
            c.accept(a);
            out.accept("Success {}", asArray(name));
            return empty();
        }
        catch (JMSRuntimeException e)
        {
            err.accept("Failure {}", asArray(name, e));
            return of(e);
        }
    }

    public static <A, B> Optional<JMSRuntimeException> biConsumer
            (
                    BiConsumer<A, B> c,
                    A a,
                    B b,
                    String name,
                    BiConsumer<String, Object[]> out,
                    BiConsumer<String, Object[]> err
            )
    {
        try
        {
            out.accept("Attempt {} ({}, {})", asArray(name, a, b));
            c.accept(a, b);
            out.accept("Success {}", asArray(name));
            return empty();
        }
        catch (JMSRuntimeException e)
        {
            err.accept("Failure {}", asArray(name, e));
            return of(e);
        }
    }

    public static <R> Either<JMSRuntimeException, R> supplier
            (
                    Supplier<R> s,
                    String name,
                    BiConsumer<String, Object[]> out,
                    BiConsumer<String, Object[]> err
            )
    {
        try
        {
            out.accept("Attempt {}", asArray(name));
            final R r = s.get();
            out.accept("Success {} {}", asArray(name, r));
            return right(r);
        }
        catch (JMSRuntimeException e)
        {
            err.accept("Failure {}", asArray(name, e));
            return left(e);
        }
    }

    public static Optional<JMSRuntimeException> runnable
            (
                    Runnable r,
                    String name,
                    BiConsumer<String, Object[]> out,
                    BiConsumer<String, Object[]> err
            )
    {
        try
        {
            out.accept("Attempt {}", asArray(name));
            r.run();
            out.accept("Success {}", asArray(name));
            return empty();
        }
        catch (JMSRuntimeException e)
        {
            err.accept("Failure {}", asArray(name, e));
            return of(e);
        }
    }

    private static Object[] asArray(Object... os)
    {
        return os;
    }
}
