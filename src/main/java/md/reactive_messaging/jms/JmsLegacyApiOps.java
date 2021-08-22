package md.reactive_messaging.jms;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.Either;
import md.reactive_messaging.functional.throwing.ThrowingConsumer;
import md.reactive_messaging.functional.throwing.ThrowingFunction;

import javax.jms.*;

import static md.reactive_messaging.jms.JmsLegacyApiMetaOps.*;


@Slf4j
public class JmsLegacyApiOps
{
    public Either<JMSException, Connection> createConnection(ConnectionFactory factory, String username, String password)
    {
        return biFunction(factory::createConnection, username, password, "create connection", log::info, log::error);
    }

    public Either<JMSException, QueueConnection> createQueueConnection(QueueConnectionFactory factory, String username, String password)
    {
        return biFunction(factory::createQueueConnection, username, password, "create queue connection", log::info, log::error);
    }

    public Either<JMSException, ? extends Connection> setExceptionListener(Connection connection, ExceptionListener listener)
    {
        return consumer(c -> c.setExceptionListener(listener), connection, "set exception listener", log::debug, log::error);
    }

    public Either<JMSException, QueueConnection> setExceptionListenerOnQueueConnection(QueueConnection queueConnection, ExceptionListener listener)
    {
        return consumer(c -> c.setExceptionListener(listener), queueConnection, "set exception listener on queue connection", log::debug, log::error);
    }

    public Either<JMSException, Connection> startConnection(Connection connection)
    {
        return consumer(Connection::start, connection, "start connection", log::debug, log::error);
    }

    public Either<JMSException, QueueConnection> startQueueConnection(QueueConnection queueConnection)
    {
        return consumer(QueueConnection::start, queueConnection, "start queue connection", log::debug, log::error);
    }

    public Either<JMSException, Connection> stopConnection(Connection connection)
    {
        return consumer(Connection::stop, connection, "stop connection", log::debug, log::error);
    }

    public Either<JMSException, QueueConnection> stopQueueConnection(QueueConnection queueConnection)
    {
        return consumer(QueueConnection::stop, queueConnection, "stop queue connection", log::debug, log::error);
    }

    public Either<JMSException, Connection> closeConnection(Connection connection)
    {
        return consumer(Connection::close, connection, "close queue connection", log::info, log::error);
    }

    public Either<JMSException, QueueConnection> closeQueueConnection(QueueConnection queueConnection)
    {
        return consumer(QueueConnection::close, queueConnection, "close queue connection", log::info, log::error);
    }

    public Either<JMSException, Session> createSession(Connection connection)
    {
        return supplier(connection::createSession, "create session", log::debug, log::error);
    }

    public Either<JMSException, QueueSession> createQueueSession(QueueConnection connection, boolean transacted, int acknowledgeMode)
    {
        return biFunction(connection::createQueueSession, transacted, acknowledgeMode, "create queue session", log::debug, log::error);
    }

    public Either<JMSException, Queue> createQueue(Session session, String queueName)
    {
        return function(session::createQueue, queueName, "create queue", log::debug, log::error);
    }

    public Either<JMSException, MessageProducer> createProducer(Session session, Queue queue)
    {
        return function(session::createProducer, queue, "create producer", log::debug, log::error);
    }

    public Either<JMSException, TextMessage> createTextMessage(Session session)
    {
        return supplier(session::createTextMessage, "create text message", log::trace, log::error);
    }

    public Either<JMSException, TextMessage> consumeTextMessage(TextMessage message, ThrowingConsumer<TextMessage, JMSException> consumer)
    {
        return consumer(consumer, message, "consume text message", log::trace, log::error);
    }

    public <R> Either<JMSException, R> applyMessage(Message message, ThrowingFunction<Message, R, JMSException> function)
    {
        return function(function, message, "apply text message", log::trace, log::error);
    }

    public Either<JMSException, Message> sendMessage(MessageProducer producer, Message message)
    {
        return consumer(producer::send, message, "send message", log::trace, log::error);
    }

    public Either<JMSException, MessageConsumer> createConsumer(Session session, Queue queue)
    {
        return function(session::createConsumer, queue, "create consumer", log::debug, log::error);
    }

    public Either<JMSException, Message> receiveMessage(final MessageConsumer consumer)
    {
        return supplier(consumer::receive, "receive message", log::trace, log::error);
    }
}
