package md.reactive_messaging.configs;

import com.tibco.tibjms.TibjmsConnectionFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.jms.ConnectionFactory;
import java.util.function.Function;

import static md.reactive_messaging.Profiles.TIBCO;

@Configuration
@Profile(TIBCO)
public class TibcoConfig
{
    @Bean
    Function<String, ConnectionFactory> connectionFactory()
    {
        return TibjmsConnectionFactory::new;
    }

    @Qualifier("url")
    @Bean
    String tibcoUrl(@Value("${tibco.url}") String url)
    {
        return url;
    }

    @Qualifier("user-name")
    @Bean
    String tibcoUserName(@Value("${tibco.user-name}") String userName)
    {
        return userName;
    }

    @Qualifier("password")
    @Bean
    String tibcoPassword(@Value("${tibco.password}") String password)
    {
        return password;
    }

    @Qualifier("queue-name")
    @Bean
    String tibcoQueueName(@Value("${tibco.queue-name}") String queueName)
    {
        return queueName;
    }
}
