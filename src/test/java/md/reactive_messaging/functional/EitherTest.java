package md.reactive_messaging.functional;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static md.reactive_messaging.functional.Either.left;
import static md.reactive_messaging.functional.Either.right;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
final class EitherTest
{
    private static final Either<String, Integer> LEFT = left("error");
    private static final Either<String, Integer> RIGHT = right(23);

    @Test
    void construct()
    {
        assertThat(right(23)).isEqualTo(new Right<>(23));
        assertThat(left("error")).isEqualTo(new Left<>("error"));
    }

    @Test
    void destruct()
    {
        assertThat(RIGHT.rightOr(null)).isEqualTo(23);
        assertThat(LEFT.rightOr(0)).isEqualTo(0);
        assertThat(RIGHT.leftOr("default")).isEqualTo("default");
        assertThat(LEFT.leftOr(null)).isEqualTo("error");
    }

    @Test
    void consume()
    {
        RIGHT.consume(l -> log.error("{}", l), r -> log.trace("{}", r));
        LEFT.consume(l -> log.trace("{}", l), r -> log.error("{}", r));
    }

    @Test
    void function()
    {
        final Integer r = RIGHT.apply(null, n -> n * n);
        assertThat(r).isEqualTo(529);
        final String l = LEFT.apply(String::toUpperCase, null);
        assertThat(l).isEqualTo("ERROR");
    }

    @Test
    void functor()
    {
        final Either<String, Integer> r = RIGHT.map(n -> n * n);
        assertThat(r).isEqualTo(right(529));
        final Either<String, Integer> l = LEFT.map(n -> n * n);
        assertThat(l).isEqualTo(left("error"));
    }

    @Test
    void monad()
    {
        final Either<String, Integer> r1 = RIGHT.flatMap(n -> right(n * n));
        assertThat(r1).isEqualTo(right(529));
        final Either<String, Integer> r2 = RIGHT.flatMap(n -> left("error"));
        assertThat(r2).isEqualTo(left("error"));
        final Either<String, Integer> l = LEFT.flatMap(n -> right(n * n));
        assertThat(l).isEqualTo(left("error"));
    }

    @Test
    void bifunctor()
    {
        final Either<Integer, String> l = LEFT.biMap(e -> e.toUpperCase().length(), n -> Integer.toString(n * n));
        assertThat(l).isEqualTo(left(5));
        final Either<Integer, String> r = RIGHT.biMap(e -> e.toUpperCase().length(), n -> Integer.toString(n * n));
        assertThat(r).isEqualTo(right("529"));
    }
}
