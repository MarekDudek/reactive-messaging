package md.reactive_messaging.jms.utils;

import lombok.extern.slf4j.Slf4j;

import javax.jms.Message;
import java.util.function.Consumer;

@Slf4j
public enum LogMessageConsumer implements Consumer<Message>
{
    LogMessageConsumer;

    @Override
    public void accept(Message message)
    {
        log.info("Message received {}", message);
    }
}
