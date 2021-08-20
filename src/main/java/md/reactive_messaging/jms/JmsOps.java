package md.reactive_messaging.jms;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.utils.Either;

import javax.jms.*;
import java.util.function.Consumer;

import static md.reactive_messaging.utils.Either.left;
import static md.reactive_messaging.utils.Either.right;


@Slf4j
public class JmsOps
{
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

    public Either<JMSException, Connection> setExceptionListener
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

    public Either<JMSException, Connection> startConnection(final Connection connection)
    {
        try
        {
            log.info("Starting connection {}", connection);
            connection.start();
            log.info("Started connection {}", connection);
            return right(connection);
        }
        catch (final JMSException e)
        {
            log.error("Failed starting connection: {}", e.getMessage());
            return left(e);
        }
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

    public Either<JMSException, Message> receive(final MessageConsumer consumer)
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
