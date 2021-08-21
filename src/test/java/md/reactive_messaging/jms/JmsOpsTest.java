package md.reactive_messaging.jms;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.utils.Either;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import javax.jms.JMSException;

import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@TestMethodOrder(OrderAnnotation.class)
final class JmsOpsTest
{
    private static final String URL = "tcp://localhost:7222";
    private static final String USER = "some-user";
    private static final String PASSWORD = "some-password";
    private static final String QUEUE = "some-queue";

    // System under test
    private static final JmsOps OPS = new JmsOps();
    private static final JmsManager MANAGER = new JmsManager(OPS);

    @Order(1)
    @Test
    void produce_one_message()
    {
        final Either<JMSException, Object> either = MANAGER.produceOneTextMessageToQueue(URL, USER, PASSWORD, QUEUE, "some text", false, AUTO_ACKNOWLEDGE);
        assertThat(either.isRight()).isTrue();
    }

    @Order(2)
    @Test
    void consume_one_message()
    {
        final Either<JMSException, String> either = MANAGER.consumeOneTextMessageFromQueue(URL, USER, PASSWORD, QUEUE, false, AUTO_ACKNOWLEDGE);
        assertThat(either.isRight()).isTrue();
    }

    @Order(3)
    @RepeatedTest(100)
    void produce_multiple_messages()
    {
        produce_one_message();
    }

    @Order(4)
    @RepeatedTest(100)
    void consume_multiple_messages()
    {
        consume_one_message();
    }
}
