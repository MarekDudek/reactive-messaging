package md.reactive_messaging.jms.utils;

import lombok.extern.slf4j.Slf4j;

import javax.jms.JMSRuntimeException;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public enum FailJmsErrorConsumer implements Consumer<JMSRuntimeException>
{
    FailJmsErrorConsumer;

    @Override
    public void accept(JMSRuntimeException exception)
    {
        log.error("Error ", exception);
        fail();
    }
}
