package md.reactive_messaging.jms;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.utils.Either;
import md.reactive_messaging.utils.ThrowingConsumer;
import md.reactive_messaging.utils.ThrowingFunction;

import javax.jms.*;
import java.util.function.Consumer;

import static md.reactive_messaging.jms.JmsOpsHelper.*;


@Slf4j
public class JmsOps
{

    public Either<JMSException, Connection> createConnection(ConnectionFactory factory, String username, String password)
    {
        return apply(factory::createConnection, username, password, "create connection");
    }

    public Either<JMSException, QueueConnection> createQueueConnection(QueueConnectionFactory factory, String username, String password)
    {
        return apply(factory::createQueueConnection, username, password, "create queue connection");
    }

    public Either<JMSException, ? extends Connection> setExceptionListener(Connection connection, Consumer<JMSException> onException)
    {
        return consume(c -> c.setExceptionListener(onException::accept), connection, "set exception listener");
    }

    public Either<JMSException, QueueConnection> setExceptionListenerOnQueueConnection(QueueConnection queueConnection, Consumer<JMSException> onException)
    {
        return consume(connection -> connection.setExceptionListener(onException::accept), queueConnection, "set exception listener on queue connection");
    }

    public Either<JMSException, Connection> startConnection(Connection connection)
    {
        return consume(Connection::start, connection, "start connection");
    }

    public Either<JMSException, QueueConnection> startQueueConnection(QueueConnection queueConnection)
    {
        return consume(Connection::start, queueConnection, "start queue connection");
    }

    public Either<JMSException, Connection> stopConnection(Connection connection)
    {
        return consume(Connection::stop, connection, "stop queue connection");
    }

    public Either<JMSException, Connection> closeConnection(Connection connection)
    {
        return consume(Connection::close, connection, "close queue connection");
    }

    public Either<JMSException, Session> createSession(Connection connection)
    {
        return get(connection::createSession, "create session");
    }

    public Either<JMSException, QueueSession> createQueueSession(QueueConnection connection, boolean transacted, int acknowledgeMode)
    {
        return apply(connection::createQueueSession, transacted, acknowledgeMode, "create queue session");
    }

    public Either<JMSException, Queue> createQueue(Session session, String queueName)
    {
        return apply(session::createQueue, queueName, "create queue");
    }

    public Either<JMSException, MessageProducer> createProducer(Session session, Queue queue)
    {
        return apply(session::createProducer, queue, "create producer");
    }

    public Either<JMSException, TextMessage> createTextMessage(Session session)
    {
        return get(session::createTextMessage, "create text message");
    }

    public Either<JMSException, TextMessage> consumeTextMessage(TextMessage message, ThrowingConsumer<TextMessage, JMSException> consumer)
    {
        return consume(consumer, message, "consume text message");
    }

    public <R> Either<JMSException, R> applyMessage(Message message, ThrowingFunction<Message, R, JMSException> function)
    {
        return apply(function, message, "apply text message");
    }

    public Either<JMSException, Message> sendMessage(MessageProducer producer, Message message)
    {
        return consume(producer::send, message, "send message");
    }

    public Either<JMSException, MessageConsumer> createConsumer(Session session, Queue queue)
    {
        return apply(session::createConsumer, queue, "create consumer");
    }

    public Either<JMSException, Message> receiveMessage(final MessageConsumer consumer)
    {
        return get(consumer::receive, "receive message");
    }
}
