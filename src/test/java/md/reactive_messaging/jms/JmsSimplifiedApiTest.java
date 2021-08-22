package md.reactive_messaging.jms;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import javax.jms.*;

import static md.reactive_messaging.TestTibcoEmsConfig.*;

@Slf4j
@TestMethodOrder(OrderAnnotation.class)
@Disabled
final class JmsSimplifiedApiTest
{
    @Order(1)
    @Test
    void synchronous_send_text_message()
    {
        final ConnectionFactory factory = new TibjmsConnectionFactory(URL);
        try (final JMSContext context = factory.createContext(USER_NAME, PASSWORD))
        {
            context.setExceptionListener(exception -> log.error("Caught by exception listener", exception));
            final Queue queue = context.createQueue(QUEUE_NAME);
            final JMSProducer producer = context.createProducer();
            producer.send(queue, "synchronous text message");
            log.info("Synchronously sent");
        }
        catch (JMSRuntimeException exc)
        {
            log.error("Caught by client code", exc);
        }
    }

    @Order(2)
    @Test
    void synchronous_receive_text_message()
    {
        final ConnectionFactory factory = new TibjmsConnectionFactory(URL);
        try (final JMSContext context = factory.createContext(USER_NAME, PASSWORD))
        {
            context.setExceptionListener(exception -> log.error("Caught by exception listener", exception));
            final Queue queue = context.createQueue(QUEUE_NAME);
            final JMSConsumer consumer = context.createConsumer(queue);
            final String body = consumer.receiveBody(String.class);
            log.info("Synchronously received '{}'", body);
        }
        catch (JMSRuntimeException exc)
        {
            log.error("Caught by client code", exc);
        }
    }

    @Order(3)
    @Test
    void asynchronous_send_text_message()
    {
        final ConnectionFactory factory = new TibjmsConnectionFactory(URL);
        try (final JMSContext context = factory.createContext(USER_NAME, PASSWORD))
        {
            final Queue queue = context.createQueue(QUEUE_NAME);
            final JMSProducer producer = context.createProducer();
            producer.setAsync(
                    new CompletionListener()
                    {
                        @Override
                        public void onCompletion(Message message)
                        {
                            try
                            {
                                final String body = message.getBody(String.class);
                                log.info("Asynronously completed sending '{}'", body);
                            }
                            catch (JMSException e)
                            {
                                log.error("Error getting body {}", e.getMessage());
                            }
                        }

                        @Override
                        public void onException(Message message, Exception exception)
                        {
                            log.error("Exception {}", message, exception);
                        }
                    }
            );
            producer.send(queue, "asynchronous text message");
        }
        catch (JMSRuntimeException exc)
        {
            log.error("Caught by client code", exc);
        }
    }

    @Order(4)
    @Test
    void asynchronous_receive_text_message() throws InterruptedException
    {
        final ConnectionFactory factory = new TibjmsConnectionFactory(URL);
        final JMSContext context = factory.createContext(USER_NAME, PASSWORD);
        final Queue queue = context.createQueue(QUEUE_NAME);
        context.setExceptionListener(exception -> log.error("Caught by exception listener", exception));
        final JMSConsumer consumer = context.createConsumer(queue);
        consumer.setMessageListener(
                message -> {
                    try
                    {
                        final String body = message.getBody(String.class);
                        log.info("Asynchronously heard '{}'", body);
                    }
                    catch (JMSException e)
                    {
                        log.error("Error getting body {}", e.getMessage());
                    }
                }
        );
        Thread.sleep(1);
        consumer.close();
    }
}