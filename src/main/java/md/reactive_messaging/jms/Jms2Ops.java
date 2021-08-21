package md.reactive_messaging.jms;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.Either;

import javax.jms.*;
import java.util.Optional;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static md.reactive_messaging.functional.Either.left;
import static md.reactive_messaging.functional.Either.right;

@Slf4j
public class Jms2Ops
{
    public Either<JMSRuntimeException, JMSContext> createContext(ConnectionFactory factory, String userName, String password)
    {
        try
        {
            log.info("Creating context for {} and {}", userName, password);
            final JMSContext context = factory.createContext(userName, password);
            log.info("Created context succeeded {}", context);
            return right(context);
        }
        catch (JMSRuntimeException e)
        {
            log.error("Creating context failed", e);
            return left(e);
        }
    }

    public Optional<JMSRuntimeException> closeContext(JMSContext context)
    {
        try
        {
            log.info("Closing context {}", context);
            context.close();
            log.info("Closed context");
            return empty();
        }
        catch (JMSRuntimeException e)
        {
            log.error("Closing context failed", e);
            return of(e);
        }
    }

    public Optional<JMSRuntimeException> setExceptionListener(JMSContext context, ExceptionListener listener)
    {
        try
        {
            log.debug("Setting exception listener {}", listener);
            context.setExceptionListener(listener);
            log.debug("Set exception listener");
            return empty();
        }
        catch (JMSRuntimeException e)
        {
            log.error("Setting exception listener failed", e);
            return of(e);
        }
    }

    public Either<JMSRuntimeException, JMSProducer> createProducer(JMSContext context)
    {
        try
        {
            log.debug("Creating producer");
            final JMSProducer producer = context.createProducer();
            log.debug("Created producer succeeded {}", producer);
            return right(producer);
        }
        catch (JMSRuntimeException e)
        {
            log.debug("Creating producer failed", e);
            return left(e);
        }
    }

    public Either<JMSRuntimeException, Queue> createQueue(JMSContext context, String queueName)
    {
        try
        {
            log.debug("Creating queue named {}", queueName);
            final Queue queue = context.createQueue(queueName);
            log.debug("Created queue {}", queue);
            return right(queue);
        }
        catch (JMSRuntimeException e)
        {
            log.error("Creating queue failed", e);
            return left(e);
        }
    }

    public Optional<JMSRuntimeException> sendTextMessage(JMSProducer producer, Destination destination, String text)
    {
        try
        {
            log.trace("Sending text message to {} with content '{}'", destination, text);
            producer.send(destination, text);
            log.trace("Sending text message succeeded");
            return empty();
        }
        catch (JMSRuntimeException e)
        {
            log.error("Sending text message failed", e);
            return of(e);
        }
    }
}
