package md.reactive_messaging.jms;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.Either;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;

import javax.jms.JMSException;

import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static md.reactive_messaging.TestTibcoEmsConfig.*;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@TestMethodOrder(OrderAnnotation.class)
@Disabled
final class JmsLegacyApiOpsTest
{
    // System under test
    private static final JmsLegacyApiOps OPS = new JmsLegacyApiOps();
    private static final JmsLegacyApiManager MANAGER = new JmsLegacyApiManager(OPS);

    @Order(1)
    @Test
    void produce_one_message()
    {
        final Either<JMSException, Object> either = MANAGER.sendOneQueueApi(URL, USER_NAME, PASSWORD, QUEUE_NAME, "some text", false, AUTO_ACKNOWLEDGE);
        assertThat(either.isRight()).isTrue();
    }

    @Order(2)
    @Test
    void consume_one_message()
    {
        final Either<JMSException, String> either = MANAGER.receiveOneQueueApi(URL, USER_NAME, PASSWORD, QUEUE_NAME, false, AUTO_ACKNOWLEDGE);
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
