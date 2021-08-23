package md.reactive_messaging.apps;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.jms.JmsSimplifiedApiManager;

import javax.jms.ConnectionFactory;
import java.time.Duration;
import java.util.function.Function;

import static java.lang.Thread.sleep;

@Builder
@Slf4j
public final class JmsSyncSender implements Runnable
{
    @NonNull
    private final JmsSimplifiedApiManager manager;
    @NonNull
    private final Function<String, ConnectionFactory> connectionFactory;
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
                log.info("Sending text message");
                manager.sendTextMessage(
                        connectionFactory, url,
                        userName, password,
                        queueName, text
                );
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
