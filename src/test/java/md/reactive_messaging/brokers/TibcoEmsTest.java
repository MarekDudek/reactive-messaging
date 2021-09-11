package md.reactive_messaging.brokers;

import com.tibco.tibjms.TibjmsConnectionFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.jms.*;

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
    void receive() throws JMSException
    {
        Connection connection = createConnection();
        Session session = connection.createSession();
        Queue queue = session.createQueue(QUEUE);
        MessageConsumer consumer = session.createConsumer(queue);
        connection.start();
        consumer.receive();
        connection.close();
    }
}
