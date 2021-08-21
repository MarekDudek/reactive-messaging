package md.reactive_messaging.jms.utils;

import lombok.extern.slf4j.Slf4j;

import javax.jms.Message;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public enum FailErrorConsumer implements BiConsumer<Message, Exception>
{
    FailErrorConsumer;

    @Override
    public void accept(Message message, Exception exception)
    {
        log.error("Error {}", message, exception);
        fail();
    }
}
