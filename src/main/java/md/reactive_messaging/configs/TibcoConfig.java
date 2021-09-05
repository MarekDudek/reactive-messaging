package md.reactive_messaging.configs;

import com.tibco.tibjms.TibjmsConnectionFactory;
import md.reactive_messaging.functional.throwing.ThrowingFunction;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import static md.reactive_messaging.Profiles.TIBCO;

@Configuration
@Profile(TIBCO)
public class TibcoConfig
{
    public static final String SEQUENTIAL_ID = "sequential-id";

    @Bean
    ThrowingFunction<String, ConnectionFactory, JMSException> connectionFactory()
    {
        return TibjmsConnectionFactory::new;
    }

    @Qualifier("url")
    @Bean
    String url(@Value("${tibco.url}") String url)
    {
        return url;
    }

    @Qualifier("user-name")
    @Bean
    String userName(@Value("${tibco.user-name}") String userName)
    {
        return userName;
    }

    @Qualifier("password")
    @Bean
    String password(@Value("${tibco.password}") String password)
    {
        return password;
    }

    @Qualifier("queue-name")
    @Bean
    String queueName(@Value("${tibco.queue-name}") String queueName)
    {
        return queueName;
    }
}
