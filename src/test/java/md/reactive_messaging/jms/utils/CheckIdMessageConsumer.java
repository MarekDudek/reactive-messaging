package md.reactive_messaging.jms.utils;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.jms.JmsSimplifiedApiOps;

import javax.jms.Message;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public enum CheckIdMessageConsumer implements Consumer<Message>
{
    CheckIdMessageConsumer;

    private static final JmsSimplifiedApiOps OPS = new JmsSimplifiedApiOps();

    @Override
    public void accept(Message message)
    {
        log.info("Message received {}", message);
        OPS.applyToMessage(message, Message::getJMSMessageID).consume(
                e -> fail(),
                id -> assertThat(id).isNotBlank()
        );
    }
}
