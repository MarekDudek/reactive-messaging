package md.reactive_messaging.configs;

import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import md.reactive_messaging.functional.throwing.ThrowingFunction;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import static com.ibm.msg.client.wmq.common.CommonConstants.*;
import static md.reactive_messaging.Profiles.IBM_MQ;

@Configuration
@Profile(IBM_MQ)
public class IbmMqConfig
{
    @Bean
    ThrowingFunction<String, ConnectionFactory, JMSException> connectionFactory
            (
                    @Qualifier("provider") String provider,
                    @Qualifier("queue-manager") String queueManager,
                    @Qualifier("channel") String channel,
                    @Qualifier("connection-mode") int connectionMode
            )
    {
        return url -> {
            JmsFactoryFactory factoryFactory = JmsFactoryFactory.getInstance(provider);
            final JmsConnectionFactory factory = factoryFactory.createConnectionFactory(url);
            factory.setStringProperty(WMQ_QUEUE_MANAGER, queueManager);
            factory.setStringProperty(WMQ_CHANNEL, channel);
            factory.setIntProperty(WMQ_CONNECTION_MODE, connectionMode);
            return factory;
        };
    }

    @Qualifier("provider")
    @Bean
    String provider(@Value("${ibm-mq.provider}") String provider)
    {
        return provider;
    }

    @Qualifier("url")
    @Bean
    String url(@Value("${ibm-mq.url}") String url)
    {
        return url;
    }

    @Qualifier("queue-manager")
    @Bean
    String queueManager(@Value("${ibm-mq.queue-manager}") String queueManager)
    {
        return queueManager;
    }

    @Qualifier("channel")
    @Bean
    String channel(@Value("${ibm-mq.channel}") String channel)
    {
        return channel;
    }

    @Qualifier("connection-mode")
    @Bean
    int connectionMode(@Value("${ibm-mq.connection-mode}") int connectionMode)
    {
        return connectionMode;
    }

    @Qualifier("user-name")
    @Bean
    String userName(@Value("${ibm-mq.user-id}") String userName)
    {
        return userName;
    }

    @Qualifier("password")
    @Bean
    String password(@Value("${ibm-mq.password}") String password)
    {
        return password;
    }

    @Qualifier("queue-name")
    @Bean
    String queueName(@Value("${ibm-mq.queue-name}") String queueName)
    {
        return queueName;
    }
}
