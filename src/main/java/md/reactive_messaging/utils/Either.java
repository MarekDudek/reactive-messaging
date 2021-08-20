package md.reactive_messaging.utils;

import lombok.NonNull;
import lombok.Value;

import java.util.function.Consumer;
import java.util.function.Function;

public interface Either<L, R>
{
    static <L, R> Either<L, R> right(final R value)
    {
        return new Right<>(value);
    }

    static <L, R> Either<L, R> left(final L value)
    {
        return new Left<>(value);
    }

    default <T> T apply
            (
                    final Function<L, T> left,
                    final Function<R, T> right
            )
    {
        if (this instanceof Left)
            return left.apply(((Left<L, R>) this).value);
        if (this instanceof Right)
            return right.apply(((Right<L, R>) this).value);
        return null;
    }

    default void consume
            (
                    final Consumer<L> left,
                    final Consumer<R> right
            )
    {
        if (this instanceof Left)
            left.accept(((Left<L, R>) this).value);
        if (this instanceof Right)
            right.accept(((Right<L, R>) this).value);
    }

    default boolean isLeft()
    {
        return apply(l -> true, r -> false);
    }

    default boolean isRight()
    {
        return apply(l -> false, r -> true);
    }

    default L leftOrDefault(final L value)
    {
        return apply(left -> left, right -> value);
    }

    default R rightOrDefault(final R value)
    {
        return apply(left -> value, right -> right);
    }

    default <T> Either<L, T> map(final Function<R, T> function)
    {
        return apply(Either::left, right -> right(function.apply(right)));
    }

    default <T> Either<L, T> flatMap(final Function<R, Either<L, T>> function)
    {
        return apply(Either::left, function);
    }

    default <T, U> Either<T, U> bimap(final Function<L, T> leftFunction, final Function<R, U> rightFunction)
    {
        return apply(left -> left(leftFunction.apply(left)), right -> right(rightFunction.apply(right)));
    }
}

@Value
class Left<L, R> implements Either<L, R>
{
    @NonNull
    public L value;
}

@Value
class Right<L, R> implements Either<L, R>
{
    @NonNull
    public R value;
}
