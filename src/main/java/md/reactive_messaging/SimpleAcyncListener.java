package md.reactive_messaging;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;

import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Queue;

import static md.reactive_messaging.TestTibcoEmsConfig.*;

@Slf4j
public final class SimpleAcyncListener
{
    public static void main(String[] args)
    {
        final TibjmsConnectionFactory factory = new TibjmsConnectionFactory(URL);
        final JMSContext context = factory.createContext(USER_NAME, PASSWORD);
        final Queue queue = context.createQueue(QUEUE_NAME);
        final JMSConsumer consumer = context.createConsumer(queue);
        consumer.setMessageListener(
                message ->
                        log.info("Got {}", message)
        );
    }
}
