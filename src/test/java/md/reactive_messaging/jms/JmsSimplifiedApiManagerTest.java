package md.reactive_messaging.jms;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static md.reactive_messaging.TestTibcoEmsConfig.*;

@Slf4j
final class JmsSimplifiedApiManagerTest
{
    private static final JmsSimplifiedApiOps OPS = new JmsSimplifiedApiOps();
    private static final JmsSimplifiedApiManager MANAGER = new JmsSimplifiedApiManager(OPS);

    @Test
    void send_one_message()
    {
        MANAGER.sendTextMessage(TibjmsConnectionFactory::new, URL, USER_NAME, PASSWORD, QUEUE_NAME, "text");
    }

    @Test
    void send_many_messages()
    {
        final Stream<String> texts = IntStream.rangeClosed(1, 1_000).mapToObj(Integer::toString);
        MANAGER.sendTextMessages(TibjmsConnectionFactory::new, URL, USER_NAME, PASSWORD, QUEUE_NAME, texts);
    }
}
