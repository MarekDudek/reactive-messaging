package md.reactive_messaging.functional;

import lombok.NonNull;
import lombok.Value;

import java.util.function.Consumer;
import java.util.function.Function;

import static md.reactive_messaging.functional.Functional.asFunction;

@Value
public class Continuation<R, A>
{
    @NonNull
    public Function<Function<A, R>, R> c;

    public static <R, A> Continuation<R, A> cont(Function<Function<A, R>, R> cont)
    {
        return new Continuation<>(cont);
    }

    public R runCont(Function<A, R> f)
    {
        return c.apply(f);
    }

    public void run(Consumer<A> c)
    {
        runCont(asFunction(c));
    }

    public static <R, A> Continuation<R, A> pure(A a)
    {
        return cont(k -> k.apply(a));
    }

    public static <R, A, B> Continuation<R, A> callCC(
            Function<
                    Function<A, Continuation<R, B>>,
                    Continuation<R, A>
                    > f
    )
    {
        return cont(k -> f.apply(x -> cont(g -> k.apply(x))).runCont(k));
    }

    public <B> Continuation<R, B> map(Function<A, B> f)
    {
        return cont(k -> runCont(f.andThen(k)));
    }

    public <B> Continuation<R, B> sequence
            (
                    Continuation<R, Function<A, B>> fab
            )
    {
        return null;
    }

    public <B> Continuation<R, B> sequenceDiscardFirst(Continuation<R, B> m)
    {
        return flatMap(ignored -> m);
    }

    public Continuation<R, A> sequenceDiscardSecond(Continuation<R, A> m)
    {
        return null;
    }

    public <B> Continuation<R, B> flatMap(Function<A, Continuation<R, B>> f)
    {
        return cont(k -> runCont(b -> f.apply(b).runCont(k)));
    }
}