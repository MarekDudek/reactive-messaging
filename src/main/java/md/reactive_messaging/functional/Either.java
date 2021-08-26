package md.reactive_messaging.functional;

import lombok.NonNull;
import lombok.Value;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static md.reactive_messaging.functional.Functional.constant;

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
        return apply(constant(true), constant(false));
    }

    default boolean isRight()
    {
        return apply(constant(false), constant(true));
    }

    default L leftOr(final L value)
    {
        return apply(left -> left, constant(value));
    }

    default R rightOr(final R value)
    {
        return apply(constant(value), right -> right);
    }

    default <T> Either<T, T> filter(final Predicate<T> predicate)
    {
        return apply(
                left -> left((T) left),
                right -> {
                    final T cast = (T) right;
                    return predicate.test(cast)
                            ? right(cast)
                            : left(cast);
                }
        );
    }

    default Either<L, R> filter(final Predicate<R> predicate, L value)
    {
        return apply(
                constant(left(value)),
                right ->
                        predicate.test(right)
                                ? right(right)
                                : left(value)
        );
    }


    default <T> Either<L, T> map(final Function<R, T> function)
    {
        return apply(Either::left, right -> right(function.apply(right)));
    }

    default <T> Either<L, T> flatMap(final Function<R, Either<L, T>> function)
    {
        return apply(Either::left, function);
    }

    default <T, U> Either<T, U> biMap(final Function<L, T> leftFunction, final Function<R, U> rightFunction)
    {
        return apply(left -> left(leftFunction.apply(left)), right -> right(rightFunction.apply(right)));
    }

    default Either<R, L> flip()
    {
        return apply(Either::right, Either::left);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    static <L, R> Either<L, R> fromOptional(final Optional<R> optional, final L left)
    {
        return optional.<Either<L, R>>map(Either::right).orElse(left(left));
    }

    default Optional<R> toOptional()
    {
        return apply(constant(Optional.empty()), Optional::of);
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
