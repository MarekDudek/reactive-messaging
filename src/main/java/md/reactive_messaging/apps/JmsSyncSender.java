package md.reactive_messaging.apps;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.throwing.ThrowingFunction;
import md.reactive_messaging.jms.JmsSimplifiedApiManager;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import java.time.Duration;

import static java.lang.Thread.sleep;
import static java.util.stream.IntStream.rangeClosed;

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
    @NonNull
    private final Duration sleep;

    @Override
    public void run()
    {
        log.info("Start");
        while (true)
        {
            try
            {
                final int count = 15_000;
                log.info("Attempt sending text message, count: {}", count);
                manager.sendTextMessages(
                        connectionFactory, url,
                        userName, password,
                        queueName,
                        rangeClosed(1, count).mapToObj(i -> "text-" + i)
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
