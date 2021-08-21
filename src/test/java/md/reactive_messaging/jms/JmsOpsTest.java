package md.reactive_messaging.jms;

import com.tibco.tibjms.TibjmsQueueConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.utils.Either;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import javax.jms.JMSException;
import javax.jms.TextMessage;

import static javax.jms.Session.AUTO_ACKNOWLEDGE;

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

    @Order(1)
    @Test
    void produce_one_message()
    {
        final Either<JMSException, Object> either =
                OPS.createQueueConnection(new TibjmsQueueConnectionFactory(URL), USER, PASSWORD).flatMap(newConnection ->
                        OPS.setExceptionListenerOnQueueConnection(newConnection, anyError -> log.error("", anyError)).flatMap(listenedConnection ->
                                OPS.createQueueSession(listenedConnection, false, AUTO_ACKNOWLEDGE).flatMap(session ->
                                        OPS.createQueue(session, QUEUE).flatMap(queue ->
                                                OPS.createProducer(session, queue).flatMap(producer ->
                                                        OPS.startQueueConnection(listenedConnection).flatMap(startedConnection ->
                                                                OPS.createTextMessage(session).map(newMessage ->
                                                                        OPS.consumeTextMessage(newMessage, message -> message.setText("some text")).flatMap(updatedMessage ->
                                                                                OPS.sendMessage(producer, updatedMessage).map(sentMessage -> {
                                                                                            OPS.stopConnection(startedConnection).flatMap(
                                                                                                    OPS::closeConnection
                                                                                            ).consume(
                                                                                                    errorClosing -> log.warn("", errorClosing),
                                                                                                    closedConnection -> log.trace("{}", closedConnection)
                                                                                            );
                                                                                            return sentMessage;
                                                                                        }
                                                                                )
                                                                        )
                                                                )
                                                        )
                                                )
                                        )
                                )
                        )
                );
        either.consume(
                error -> log.error("", error),
                message -> log.info("{}", message)
        );
    }

    @Order(2)
    @Test
    void consume_one_message()
    {

        final Either<JMSException, String> either =
                OPS.createQueueConnection(new TibjmsQueueConnectionFactory(URL), USER, PASSWORD).flatMap(newConnection ->
                        OPS.setExceptionListenerOnQueueConnection(newConnection, anyError -> log.error("", anyError)).flatMap(listenedConnection ->
                                OPS.createQueueSession(listenedConnection, false, AUTO_ACKNOWLEDGE).flatMap(session ->
                                        OPS.createQueue(session, QUEUE).flatMap(queue ->
                                                OPS.createConsumer(session, queue).flatMap(consumer ->
                                                        OPS.startQueueConnection(listenedConnection).flatMap(startedConnection ->
                                                                OPS.receiveMessage(consumer).flatMap(receivedMessage -> {
                                                                            final Either<JMSException, String> extractedMessage =
                                                                                    OPS.applyMessage(receivedMessage, message -> ((TextMessage) message).getText());
                                                                            OPS.stopConnection(startedConnection).flatMap(
                                                                                    OPS::closeConnection
                                                                            ).consume(
                                                                                    errorClosing -> log.error("", errorClosing),
                                                                                    closedConnection -> log.trace("{}", closedConnection)
                                                                            );
                                                                            return extractedMessage;
                                                                        }
                                                                )
                                                        )
                                                )
                                        )
                                )
                        )
                );
        either.consume(
                error -> log.error("", error),
                message -> log.info("{}", message)
        );
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
