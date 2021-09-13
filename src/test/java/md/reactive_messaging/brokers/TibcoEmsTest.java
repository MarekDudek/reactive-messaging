package md.reactive_messaging.brokers;

import com.tibco.tibjms.TibjmsConnectionFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Description;

import javax.jms.*;

import static com.tibco.tibjms.Tibjms.EXPLICIT_CLIENT_ACKNOWLEDGE;
import static javax.jms.Session.CLIENT_ACKNOWLEDGE;
import static md.reactive_messaging.TestTibcoEmsConfig.*;

@Disabled
final class TibcoEmsTest
{
    private final String QUEUE = QUEUE_NAME;

    private Connection createConnection() throws JMSException
    {
        ConnectionFactory factory = new TibjmsConnectionFactory(URL);
        return factory.createConnection(USER_NAME, PASSWORD);
    }

    @Test
    void send() throws JMSException
    {
        Connection connection = createConnection();
        Session session = connection.createSession();
        Queue queue = session.createQueue(QUEUE);
        MessageProducer producer = session.createProducer(queue);
        Message message = session.createMessage();
        producer.send(message);
        connection.close();
    }

    @Test
    @Description("Requires at least one message on broker")
    void receive_default() throws JMSException
    {
        Connection connection = createConnection();
        Session session = connection.createSession();
        Queue queue = session.createQueue(QUEUE);
        MessageConsumer consumer = session.createConsumer(queue);
        connection.start();
        consumer.receive();
        connection.close();
    }

    @Test
    @Description("Requires at least one message on broker but does not consume it")
    void receive_no_client_acknowledge() throws JMSException
    {
        Connection connection = createConnection();
        Session session = connection.createSession(CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(QUEUE);
        MessageConsumer consumer = session.createConsumer(queue);
        connection.start();
        consumer.receive();
        connection.close();
    }

    @Test
    @Description("Requires at least one message on broker and consumes it")
    void receive_client_acknowledge() throws JMSException
    {
        Connection connection = createConnection();
        Session session = connection.createSession(CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(QUEUE);
        MessageConsumer consumer = session.createConsumer(queue);
        connection.start();
        Message message = consumer.receive();
        message.acknowledge();
        connection.close();
    }

    @Test
    @Description("Requires at least three message on broker and consumes all of them")
    void receive_client_acknowledge_multiple() throws JMSException
    {
        Connection connection = createConnection();
        Session session = connection.createSession(CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(QUEUE);
        MessageConsumer consumer = session.createConsumer(queue);
        connection.start();
        consumer.receive();
        consumer.receive();
        Message message = consumer.receive();
        message.acknowledge();
        connection.close();
    }

    @Test
    @Description("Requires at least three message on broker and consumes only one of them")
    void receive_explicit_client_acknowledge_multiple() throws JMSException
    {
        Connection connection = createConnection();
        Session session = connection.createSession(EXPLICIT_CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(QUEUE);
        MessageConsumer consumer = session.createConsumer(queue);
        connection.start();
        consumer.receive();
        consumer.receive();
        Message message = consumer.receive();
        message.acknowledge();
        connection.close();
    }
}
