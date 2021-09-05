package md.reactive_messaging.apps;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.throwing.ThrowingBiConsumer;
import md.reactive_messaging.functional.throwing.ThrowingFunction;
import md.reactive_messaging.jms.JmsSimplifiedApiManager;

import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import java.time.Duration;
import java.util.function.Function;
import java.util.stream.LongStream;

import static java.lang.Thread.sleep;

@Builder
@Slf4j
public final class JmsSyncSender implements Runnable
{
    @NonNull
    private final JmsSimplifiedApiManager manager;
    @NonNull
    private final ThrowingFunction<String, ConnectionFactory, JMSException> connectionFactory;
    @NonNull
    private final String url;
    @NonNull
    private final String userName;
    @NonNull
    private final String password;
    @NonNull
    private final String queueName;
    @NonNull
    private final String text;

    private final int count;

    @NonNull
    private final Duration sleep;
    @NonNull
    private final Function<JMSContext, Message> createMessage;
    @NonNull
    private final ThrowingBiConsumer<Message, Long, JMSException> prepareMessage;

    @Override
    public void run()
    {
        log.info("Start");
        while (true)
        {
            try
            {
                log.info("Attempt sending text message, count: {}", count);
                manager.sendTextMessages(
                        connectionFactory, url,
                        userName, password,
                        queueName,
                        LongStream.rangeClosed(1, count),
                        createMessage,
                        prepareMessage
                );
                log.info("Success sending text message");
                sleep(sleep.toMillis());
            }
            catch (InterruptedException e)
            {
                log.info("Interrupted");
                break;
            }
        }
        log.info("Finish");
    }
}
