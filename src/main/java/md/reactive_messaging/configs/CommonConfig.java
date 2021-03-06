package md.reactive_messaging.configs;

import md.reactive_messaging.jms.JmsSimplifiedApiManager;
import md.reactive_messaging.jms.JmsSimplifiedApiOps;
import md.reactive_messaging.reactive.ReactiveOps;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CommonConfig
{
    @Bean
    JmsSimplifiedApiOps jmsSimplifiedApiOps()
    {
        return new JmsSimplifiedApiOps();
    }

    @Bean
    JmsSimplifiedApiManager jmsSimplifiedApiManager(JmsSimplifiedApiOps ops)
    {
        return new JmsSimplifiedApiManager(ops);
    }

    @Bean
    ReactiveOps reactiveOps()
    {
        return new ReactiveOps();
    }
}
