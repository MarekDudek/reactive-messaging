package md.reactive_messaging.functional;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Function;

import static java.lang.System.out;
import static java.util.Collections.nCopies;
import static java.util.function.Function.identity;
import static md.reactive_messaging.functional.Continuation.pure;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
final class ContinuationExamplesTest
{
    private static <R> Continuation<R, Integer> calculateLength(List<?> list)
    {
        return pure(list.size());
    }

    @Test
    void calculate_length()
    {
        final Continuation<Integer, Integer> c =
                calculateLength(nCopies(5, ""));
        final Integer result = c.runCont(identity());
        assertThat(result).isEqualTo(5);
        c.run(out::println);
    }

    private static <R> Continuation<R, Integer> makeDouble(Integer n)
    {
        return pure(n * 2);
    }

    @Test
    void chain_blocks()
    {
        final Function<Integer, Continuation<Object, Integer>> makeDouble = ContinuationExamplesTest::makeDouble;
        final Continuation<Object, Integer> c = calculateLength(nCopies(5, " "));
        c.flatMap(makeDouble).run(out::println);
    }

    private static String whatsYourName(String name)
    {
        final Continuation<String, String> response =
                Continuation.<String, String, String>callCC(exit -> {
                            validateName(name, exit);
                            return pure("Welcome, " + name + "!");
                        }
                );
        return response.runCont(identity());
    }

    @Test
    void never_see_this()
    {
        pure(5).flatMap(x -> {
                    out.println(x);
                    return Continuation.callCC(k -> k.apply("Ha ha").sequenceDiscardFirst(pure("never see this")));
                }
        ).run(out::println);
    }

    private static String validateName(String name, Function<String, Continuation<String, String>> exit)
    {
        if (name == null)
        {
            final Continuation<String, String> c = exit.apply("You forgot to tell me your name!");
            return c.runCont(identity());
        }
        return name;
    }

    @Test
    void callCC()
    {
        log.info("{}", whatsYourName("Marek"));
        log.info("{}", whatsYourName(null));
    }
}
