package md.reactive_messaging.jms;

import com.tibco.tibjms.TibjmsQueueConnectionFactory;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.Either;

import javax.jms.JMSException;

@Slf4j
@RequiredArgsConstructor
public final class JmsLegacyApiManager
{
    @NonNull
    private final JmsLegacyApiOps ops;

    public Either<JMSException, Object> sendOneQueueApi
            (
                    String url,
                    String user,
                    String password,
                    String queueName,
                    String text,
                    boolean transacted,
                    int acknowledgeMode
            )
    {
        return ops.createQueueConnection(new TibjmsQueueConnectionFactory(url), user, password).flatMap(newConnection ->
                ops.setExceptionListenerOnQueueConnection(newConnection, exception -> log.error("", exception)).flatMap(listenedConnection ->
                        ops.createQueueSession(listenedConnection, transacted, acknowledgeMode).flatMap(session ->
                                ops.createQueue(session, queueName).flatMap(queue ->
                                        ops.createProducer(session, queue).flatMap(producer ->
                                                ops.startQueueConnection(listenedConnection).flatMap(startedConnection ->
                                                        ops.createTextMessage(session).map(newMessage ->
                                                                ops.consumeTextMessage(newMessage, message -> message.setText(text)).flatMap(updatedMessage ->
                                                                        ops.sendMessage(producer, updatedMessage).map(sentMessage -> {
                                                                                    ops.stopQueueConnection(startedConnection).flatMap(
                                                                                            ops::closeQueueConnection
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
    }

    public Either<JMSException, String> receiveOneQueueApi
            (
                    String url,
                    String user,
                    String password,
                    String queueName,
                    boolean transacted,
                    int acknowledgeMode
            )
    {
        return ops.createQueueConnection(new TibjmsQueueConnectionFactory(url), user, password).flatMap(newConnection ->
                ops.setExceptionListenerOnQueueConnection(newConnection, exception -> log.error("", exception)).flatMap(listenedConnection ->
                        ops.createQueueSession(listenedConnection, transacted, acknowledgeMode).flatMap(session ->
                                ops.createQueue(session, queueName).flatMap(queue ->
                                        ops.createConsumer(session, queue).flatMap(consumer ->
                                                ops.startQueueConnection(listenedConnection).flatMap(startedConnection ->
                                                        ops.receiveMessage(consumer).flatMap(receivedMessage -> {
                                                                    final Either<JMSException, String> extractedMessage =
                                                                            ops.applyMessage(receivedMessage, MessageConverters::getText);
                                                                    ops.stopQueueConnection(startedConnection).flatMap(
                                                                            ops::closeConnection
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
    }
}
