package md.reactive_messaging.utils;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.jms.JmsOps;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

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
        final Either<JMSException, TextMessage> either =
                OPS.createConnection(new TibjmsConnectionFactory(URL), USER, PASSWORD).flatMap(newConnection ->
                        OPS.setExceptionListener(
                                newConnection,
                                anyError -> log.error("", anyError)
                        ).flatMap(listenedConnection ->
                                OPS.createSession(listenedConnection).flatMap(session ->
                                        OPS.createQueue(session, QUEUE).flatMap(queue ->
                                                OPS.createProducer(session, queue).flatMap(producer ->
                                                        OPS.startConnection(listenedConnection).flatMap(startedConnection ->
                                                                OPS.createTextMessage(session).map(message -> {
                                                                            OPS.sendMessage(producer, message).ifPresent(
                                                                                    errorSending -> log.error("", errorSending)
                                                                            );
                                                                            OPS.closeConnection(startedConnection).consume(
                                                                                    errorClosing -> log.error("", errorClosing),
                                                                                    closedConnection -> log.trace("{}", closedConnection)
                                                                            );
                                                                            return message;
                                                                        }
                                                                )
                                                        )
                                                )
                                        )
                                ))
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
        final Either<JMSException, Message> either =
                OPS.createConnection(new TibjmsConnectionFactory(URL), USER, PASSWORD).flatMap(newConnection ->
                        OPS.setExceptionListener(
                                newConnection,
                                anyError -> log.error("", anyError)
                        ).flatMap(listenedConnection ->
                                OPS.createSession(listenedConnection).flatMap(session ->
                                        OPS.createQueue(session, QUEUE).flatMap(queue ->
                                                OPS.createConsumer(session, queue)).flatMap(consumer ->
                                                OPS.startConnection(listenedConnection).flatMap(startedConnection ->
                                                        OPS.receiveMessage(consumer).map(message -> {
                                                                    OPS.closeConnection(startedConnection).consume(
                                                                            errorClosing -> log.error("", errorClosing),
                                                                            closedConnection -> log.trace("{}", closedConnection)
                                                                    );
                                                                    return message;
                                                                }
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
}
