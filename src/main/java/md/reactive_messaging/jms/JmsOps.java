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
        return apply(factory::createConnection, username, password, "create connection", log::info, log::error);
    }

    public Either<JMSException, QueueConnection> createQueueConnection(QueueConnectionFactory factory, String username, String password)
    {
        return apply(factory::createQueueConnection, username, password, "create queue connection", log::info, log::error);
    }

    public Either<JMSException, ? extends Connection> setExceptionListener(Connection connection, Consumer<JMSException> onException)
    {
        return accept(c -> c.setExceptionListener(onException::accept), connection, "set exception listener", log::debug, log::error);
    }

    public Either<JMSException, QueueConnection> setExceptionListenerOnQueueConnection(QueueConnection queueConnection, Consumer<JMSException> onException)
    {
        return accept(c -> c.setExceptionListener(onException::accept), queueConnection, "set exception listener on queue connection", log::debug, log::error);
    }

    public Either<JMSException, Connection> startConnection(Connection connection)
    {
        return accept(Connection::start, connection, "start connection", log::debug, log::error);
    }

    public Either<JMSException, QueueConnection> startQueueConnection(QueueConnection queueConnection)
    {
        return accept(QueueConnection::start, queueConnection, "start queue connection", log::debug, log::error);
    }

    public Either<JMSException, Connection> stopConnection(Connection connection)
    {
        return accept(Connection::stop, connection, "stop connection", log::debug, log::error);
    }

    public Either<JMSException, QueueConnection> stopQueueConnection(QueueConnection queueConnection)
    {
        return accept(QueueConnection::stop, queueConnection, "stop queue connection", log::debug, log::error);
    }

    public Either<JMSException, Connection> closeConnection(Connection connection)
    {
        return accept(Connection::close, connection, "close queue connection", log::info, log::error);
    }

    public Either<JMSException, QueueConnection> closeQueueConnection(QueueConnection queueConnection)
    {
        return accept(QueueConnection::close, queueConnection, "close queue connection", log::info, log::error);
    }

    public Either<JMSException, Session> createSession(Connection connection)
    {
        return get(connection::createSession, "create session", log::debug, log::error);
    }

    public Either<JMSException, QueueSession> createQueueSession(QueueConnection connection, boolean transacted, int acknowledgeMode)
    {
        return apply(connection::createQueueSession, transacted, acknowledgeMode, "create queue session", log::debug, log::error);
    }

    public Either<JMSException, Queue> createQueue(Session session, String queueName)
    {
        return apply(session::createQueue, queueName, "create queue", log::debug, log::error);
    }

    public Either<JMSException, MessageProducer> createProducer(Session session, Queue queue)
    {
        return apply(session::createProducer, queue, "create producer", log::debug, log::error);
    }

    public Either<JMSException, TextMessage> createTextMessage(Session session)
    {
        return get(session::createTextMessage, "create text message", log::trace, log::error);
    }

    public Either<JMSException, TextMessage> consumeTextMessage(TextMessage message, ThrowingConsumer<TextMessage, JMSException> consumer)
    {
        return accept(consumer, message, "consume text message", log::trace, log::error);
    }

    public <R> Either<JMSException, R> applyMessage(Message message, ThrowingFunction<Message, R, JMSException> function)
    {
        return apply(function, message, "apply text message", log::trace, log::error);
    }

    public Either<JMSException, Message> sendMessage(MessageProducer producer, Message message)
    {
        return accept(producer::send, message, "send message", log::trace, log::error);
    }

    public Either<JMSException, MessageConsumer> createConsumer(Session session, Queue queue)
    {
        return apply(session::createConsumer, queue, "create consumer", log::debug, log::error);
    }

    public Either<JMSException, Message> receiveMessage(final MessageConsumer consumer)
    {
        return get(consumer::receive, "receive message", log::trace, log::error);
    }
}
