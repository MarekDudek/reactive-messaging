package md.reactive_messaging.jms;

import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.jms.*;

import static com.ibm.msg.client.jms.JmsConstants.*;
import static com.ibm.msg.client.wmq.common.CommonConstants.*;

@Slf4j
@Disabled
final class IbmMqTest
{
    @Test
    void test() throws JMSException
    {
        JmsFactoryFactory factoryFactory = JmsFactoryFactory.getInstance(WMQ_PROVIDER);
        JmsConnectionFactory factory = factoryFactory.createConnectionFactory();

        factory.setStringProperty(WMQ_HOST_NAME, "localhost");
        factory.setIntProperty(WMQ_PORT, 1414);

        factory.setStringProperty(WMQ_QUEUE_MANAGER, "QM1");
        factory.setStringProperty(WMQ_CHANNEL, "DEV.APP.SVRCONN");
        factory.setIntProperty(WMQ_CONNECTION_MODE, WMQ_CM_CLIENT);
        factory.setStringProperty(WMQ_APPLICATIONNAME, "my-app-name");
        factory.setBooleanProperty(USER_AUTHENTICATION_MQCSP, true);
        factory.setStringProperty(USERID, "app");
        factory.setStringProperty(PASSWORD, "passw0rd");

        JMSContext context = factory.createContext();
        Destination destination = context.createQueue("queue:///" + "DEV.QUEUE.1");

        TextMessage message = context.createTextMessage("Message to IBM");
        JMSProducer producer = context.createProducer();
        producer.send(destination, message);

        JMSConsumer consumer = context.createConsumer(destination);
        String body = consumer.receiveBody(String.class, 15000);
        log.info("Received {}", body);

        context.close();
    }

    @Test
    void test2() throws JMSException
    {
        JmsFactoryFactory factoryFactory = JmsFactoryFactory.getInstance(WMQ_PROVIDER);
        JmsConnectionFactory factory = factoryFactory.createConnectionFactory("tcp://localhost:1414");

        factory.setStringProperty(WMQ_QUEUE_MANAGER, "QM1");
        factory.setStringProperty(WMQ_CHANNEL, "DEV.APP.SVRCONN");
        factory.setIntProperty(WMQ_CONNECTION_MODE, WMQ_CM_CLIENT);

        JMSContext context = factory.createContext("app", "passw0rd");
        Destination destination = context.createQueue("DEV.QUEUE.1");

        TextMessage message = context.createTextMessage("Message to IBM");
        JMSProducer producer = context.createProducer();
        producer.send(destination, message);

        JMSConsumer consumer = context.createConsumer(destination);
        String body = consumer.receiveBody(String.class, 15000);
        log.info("Received {}", body);

        context.close();
    }
}
