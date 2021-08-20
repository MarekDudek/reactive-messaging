package md.reactive_messaging.jms;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.utils.*;

import javax.jms.*;
import java.util.function.Consumer;

import static md.reactive_messaging.utils.Either.left;
import static md.reactive_messaging.utils.Either.right;


@Slf4j
public class JmsOps
{
    public static <T> Either<JMSException, T> consume
            (
                    final ThrowingConsumer<T, JMSException> consumer,
                    final T argument,
                    final String name
            )
    {
        try
        {
            log.info("Attempting {}", name);
            consumer.accept(argument);
            log.info("Succeeded {}", name);
            return right(argument);
        }
        catch (final JMSException e)
        {
            log.error("Failed {} - '{}'", name, e.getMessage());
            return left(e);
        }
    }

    public static <T, R> Either<JMSException, R> apply
            (
                    final ThrowingFunction<T, R, JMSException> function,
                    final T argument,
                    final String name
            )
    {
        try
        {
            log.info("Attempting {}", name);
            final R result = function.apply(argument);
            log.info("Succeeded {}", name);
            return right(result);
        }
        catch (final JMSException e)
        {
            log.error("Failed {} - '{}'", name, e.getMessage());
            return left(e);
        }
    }

    public static <T1, T2, R> Either<JMSException, R> applyBi
            (
                    final ThrowingBiFunction<T1, T2, R, JMSException> biFunction,
                    final T1 argument1,
                    final T2 argument2,
                    final String name
            )
    {
        try
        {
            log.info("Attempting {}", name);
            final R result = biFunction.apply(argument1, argument2);
            log.info("Succeeded {}", name);
            return right(result);
        }
        catch (final JMSException e)
        {
            log.error("Failed {} - '{}'", name, e.getMessage());
            return left(e);
        }
    }

    public static <T1, T2, T3, R> Either<JMSException, R> applyTri
            (
                    final ThrowingTriFunction<T1, T2, T3, R, JMSException> triFunction,
                    final T1 argument1,
                    final T2 argument2,
                    final T3 argument3,
                    final String name
            )
    {
        try
        {
            log.info("Attempting {}", name);
            final R result = triFunction.apply(argument1, argument2, argument3);
            log.info("Succeeded {}", name);
            return right(result);
        }
        catch (final JMSException e)
        {
            log.error("Failed {} - '{}'", name, e.getMessage());
            return left(e);
        }
    }

    public Either<JMSException, Connection> createConnection
            (
                    final ConnectionFactory factory,
                    final String username,
                    final String password
            )
    {
        try
        {
            log.info("Creating connection");
            final Connection connection = factory.createConnection(username, password);
            log.info("Created connection {}", connection);
            return right(connection);
        }
        catch (final JMSException e)
        {
            log.error("Failed creating connection: {}", e.getMessage());
            return left(e);
        }
    }

    public Either<JMSException, QueueConnection> createQueueConnection
            (
                    final QueueConnectionFactory factory,
                    final String username,
                    final String password
            )
    {
        return applyBi(factory::createQueueConnection, username, password, "create queue connection");
    }

    public Either<JMSException, ? extends Connection> setExceptionListener
            (
                    final Connection connection,
                    final Consumer<JMSException> onException
            )
    {
        try
        {
            log.info("Setting exception listener on {}", connection);
            connection.setExceptionListener(
                    exception -> {
                        log.error("Detected exception: {}", exception.getMessage());
                        onException.accept(exception);
                        closeConnection(connection);
                    }
            );
            log.info("Exception listener set on {}", connection);
            return right(connection);
        }
        catch (final JMSException e)
        {
            log.error("Failed setting exception listener: {}", e.getMessage());
            closeConnection(connection);
            return left(e);
        }
    }

    public Either<JMSException, QueueConnection> setExceptionListener
            (
                    final QueueConnection queueConnection,
                    final Consumer<JMSException> onException
            )
    {

        return consume(connection -> connection.setExceptionListener(onException::accept), queueConnection, "set exception listener on queue connection");
    }

    public Either<JMSException, QueueConnection> startQueueConnection(final QueueConnection queueConnection)
    {
        return consume(Connection::start, queueConnection, "start queue connection");
    }

    public Either<JMSException, Connection> startConnection(final Connection connection)
    {
        return consume(Connection::start, connection, "start connection");
    }

    public Either<JMSException, Connection> stopConnection(final Connection connection)
    {
        return consume(Connection::stop, connection, "stop queue connection");
    }

    public Either<JMSException, Connection> closeConnection(final Connection connection)
    {
        try
        {
            log.info("Closing connection {}", connection);
            connection.close();
            log.info("Closed connection {}", connection);
            return right(connection);
        }
        catch (final JMSException e)
        {
            log.error("Failed closing {}, {}", connection, e.getMessage());
            return left(e);
        }
    }

    public Either<JMSException, Session> createSession(final Connection connection)
    {
        try
        {
            log.info("Creating session");
            final Session session = connection.createSession();
            log.info("Created session {}", session);
            return right(session);
        }
        catch (final JMSException e)
        {
            log.error("Failed creating session: {}", e.getMessage());
            return left(e);
        }
    }

    public Either<JMSException, QueueSession> createQueueSession
            (
                    final QueueConnection connection,
                    final boolean transacted,
                    final int acknowledgeMode
            )
    {
        try
        {
            log.info("Creating queue session");
            final QueueSession session = connection.createQueueSession(transacted, acknowledgeMode);
            log.info("Created queue session {}", session);
            return right(session);
        }
        catch (final JMSException e)
        {
            log.error("Failed creating queue session: {}", e.getMessage());
            return left(e);
        }
    }

    public Either<JMSException, Queue> createQueue
            (
                    final Session session,
                    final String queueName
            )
    {
        try
        {
            log.info("Creating queue");
            final Queue queue = session.createQueue(queueName);
            log.info("Created queue {}", queue);
            return right(queue);
        }
        catch (final JMSException e)
        {
            log.error("Failed creating queue: {}", e.getMessage());
            return left(e);
        }
    }

    public Either<JMSException, MessageProducer> createProducer
            (
                    final Session session,
                    final Queue queue
            )
    {
        try
        {
            log.info("Creating producer");
            final MessageProducer producer = session.createProducer(queue);
            log.info("Created producer");
            return right(producer);
        }
        catch (JMSException e)
        {
            log.error("Failed creating producer: {}", e.getMessage());
            return left(e);
        }
    }

    public Either<JMSException, TextMessage> createTextMessage(final Session session)
    {
        try
        {
            log.trace("Creating text message");
            final TextMessage message = session.createTextMessage();
            log.trace("Created text message");
            return right(message);
        }
        catch (final JMSException e)
        {
            log.error("Failed creating text message: {}", e.getMessage());
            return left(e);
        }
    }

    public Either<JMSException, TextMessage> consumeTextMessage
            (
                    final TextMessage message,
                    final ThrowingConsumer<TextMessage, JMSException> consumer
            )
    {
        try
        {
            log.trace("Consuming text message");
            consumer.accept(message);
            log.trace("Consumed text message");
            return right(message);
        }
        catch (JMSException e)
        {
            log.error("Error consuming text message");
            return left(e);
        }
    }

    public <R> Either<JMSException, R> applyMessage
            (
                    final Message message,
                    final ThrowingFunction<Message, R, JMSException> function
            )
    {
        try
        {
            log.trace("Applying text message");
            final R result = function.apply(message);
            log.trace("Applied text message");
            return right(result);
        }
        catch (final JMSException e)
        {
            log.error("Error applying text message");
            return left(e);
        }
    }

    public Either<JMSException, Message> sendMessage
            (
                    final MessageProducer producer,
                    final Message message
            )
    {
        try
        {
            log.trace("Sending message");
            producer.send(message);
            log.trace("Sent message");
            return right(message);
        }
        catch (final JMSException e)
        {
            log.error("Failed sending message: {}", e.getMessage());
            return left(e);
        }
    }

    public Either<JMSException, MessageConsumer> createConsumer
            (
                    final Session session,
                    final Queue queue
            )
    {
        try
        {
            log.info("Creating consumer");
            final MessageConsumer consumer = session.createConsumer(queue);
            log.info("Created consumer {}", consumer);
            return right(consumer);
        }
        catch (final JMSException e)
        {
            log.error("Failed creating consumer: {}", e.getMessage());
            return left(e);
        }
    }

    public Either<JMSException, Message> receiveMessage(final MessageConsumer consumer)
    {
        try
        {
            log.trace("Receiving message");
            final Message message = consumer.receive();
            log.trace("Received message");
            return right(message);
        }
        catch (final JMSException e)
        {
            log.error("Failed receiving message: {}", e.getMessage());
            return left(e);
        }
    }
}
