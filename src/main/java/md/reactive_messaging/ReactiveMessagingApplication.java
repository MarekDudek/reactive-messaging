package md.reactive_messaging;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.jms.JmsSimplifiedApiManager;
import md.reactive_messaging.jms.JmsSimplifiedApiOps;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.TaskExecutor;

import javax.jms.ConnectionFactory;
import java.util.function.Function;

import static java.lang.Thread.sleep;
import static java.time.Duration.ofMillis;
import static md.reactive_messaging.Profiles.JMS_SYNC_SENDER;
import static md.reactive_messaging.Profiles.TIBCO;

@SpringBootApplication
@Slf4j
public class ReactiveMessagingApplication
{
    public static void main(String[] args)
    {
        SpringApplication.run(ReactiveMessagingApplication.class, args);
    }

    @Bean
    JmsSimplifiedApiManager jmsManager()
    {
        final JmsSimplifiedApiOps ops = new JmsSimplifiedApiOps();
        return new JmsSimplifiedApiManager(ops);
    }

    @Profile(TIBCO)
    @Bean
    Function<String, ConnectionFactory> tibcoConnectionFactory()
    {
        return TibjmsConnectionFactory::new;
    }

    @Profile(TIBCO)
    @Qualifier("url")
    @Bean
    String tibcoUrl(@Value("${tibco.url}") String url)
    {
        return url;
    }

    @Profile(TIBCO)
    @Qualifier("user-name")
    @Bean
    String tibcoUserName(@Value("${tibco.user-name}") String userName)
    {
        return userName;
    }

    @Profile(TIBCO)
    @Qualifier("password")
    @Bean
    String tibcoPassword(@Value("${tibco.password}") String password)
    {
        return password;
    }

    @Profile(TIBCO)
    @Qualifier("queue-name")
    @Bean
    String tibcoQueueName(@Value("${tibco.queue-name}") String queueName)
    {
        return queueName;
    }

    @Profile(JMS_SYNC_SENDER)
    @Bean
    ApplicationRunner jmsSyncSender
            (
                    TaskExecutor taskExecutor,
                    JmsSimplifiedApiManager jmsManager,
                    Function<String, ConnectionFactory> connectonFactory,
                    @Qualifier("url") String url,
                    @Qualifier("user-name") String userName,
                    @Qualifier("password") String password,
                    @Qualifier("queue-name") String queueName
            )
    {
        return args ->
                taskExecutor.execute(() ->
                        {
                            while (true)
                            {
                                try
                                {
                                    jmsManager.sendTextMessage(connectonFactory, url, userName, password, queueName, "text");
                                    sleep(ofMillis(100).toMillis());
                                }
                                catch (InterruptedException e)
                                {
                                    log.info("Requested to stop");
                                    break;
                                }
                            }
                        }
                );
    }
}
