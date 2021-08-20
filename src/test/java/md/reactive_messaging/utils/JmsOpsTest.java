package md.reactive_messaging.utils;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.jms.JmsOps;
import org.junit.jupiter.api.Test;

@Slf4j
final class JmsOpsTest
{
    private static final String URL = "tcp://localhost:7222";
    private static final String USER = "some-user";
    private static final String PASSWORD = "some-password";
    private static final String QUEUE = "demo-queue";

    // System under test
    private static final JmsOps OPS = new JmsOps();

    @Test
    void consume_one_message()
    {
        OPS.createConnection(new TibjmsConnectionFactory(URL), USER, PASSWORD).flatMap(newConnection ->
                OPS.setExceptionListener(
                        newConnection,
                        exception -> log.error("", exception)
                ).flatMap(listenedConnection ->
                        OPS.createSession(listenedConnection).flatMap(session ->
                                OPS.createQueue(session, QUEUE).flatMap(queue ->
                                        OPS.createConsumer(session, queue)).flatMap(consumer ->
                                        OPS.startConnection(listenedConnection).flatMap(startedConnection -> {
                                                    OPS.receive(consumer).consume(
                                                            exception -> log.error("", exception),
                                                            message -> log.info("{}", message)
                                                    );
                                                    return OPS.closeConnection(startedConnection);
                                                }
                                        )
                                )
                        )
                )
        );
    }
}
