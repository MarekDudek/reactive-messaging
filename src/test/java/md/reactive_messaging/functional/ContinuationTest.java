package md.reactive_messaging.functional;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static md.reactive_messaging.functional.Continuation.pure;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
final class ContinuationTest
{
    private static final Function<Integer, String> TO_STRING = n -> Integer.toString(n);
    private static final Function<Integer, Integer> SQUARE = n -> n * n;
    private static final Function<Integer, Continuation<String, Integer>> SQUARE_EVEN = n -> (n % 2 == 0) ? pure(n * n) : pure(0);

    @Test
    void create()
    {
        final Continuation<String, Integer> cont = pure(23);
        final String result = cont.runCont(TO_STRING);
        assertThat(result).isEqualTo("23");
        cont.run(r -> log.info("{}", r));
    }

    @Test
    void functor()
    {
        final Continuation<Integer, Integer> c = pure(23);
        assertThat(
                c.runCont(SQUARE)
        ).isEqualTo(
                529
        );
    }

    @Test
    void monad()
    {
        final Continuation<String, Integer> even = pure(22);
        assertThat(
                even.flatMap(SQUARE_EVEN).runCont(TO_STRING)
        ).isEqualTo(
                "484"
        );
        final Continuation<String, Integer> odd = pure(23);
        assertThat(
                odd.flatMap(SQUARE_EVEN).runCont(TO_STRING)
        ).isEqualTo(
                "0"
        );
    }
}
