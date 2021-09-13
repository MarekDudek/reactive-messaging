package md.reactive_messaging.brokers;

import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Description;

import javax.jms.*;

import static com.ibm.msg.client.wmq.common.CommonConstants.*;

@Disabled
final class IbmMqTest
{
    public static final String QUEUE = "DEV.QUEUE.1";

    private Connection createConnection() throws JMSException
    {
        JmsFactoryFactory factoryFactory = JmsFactoryFactory.getInstance("com.ibm.msg.client.wmq");
        JmsConnectionFactory factory = factoryFactory.createConnectionFactory("tcp://localhost:1414");
        factory.setStringProperty(WMQ_QUEUE_MANAGER, "QM1");
        factory.setStringProperty(WMQ_CHANNEL, "DEV.APP.SVRCONN");
        factory.setIntProperty(WMQ_CONNECTION_MODE, 1);
        return factory.createConnection("app", "passw0rd");
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
}
