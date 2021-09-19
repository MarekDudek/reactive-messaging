package md.reactive_messaging.configs;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import static md.reactive_messaging.Profiles.JMS_SYNC_RECEIVER;

@Profile(JMS_SYNC_RECEIVER)
@Configuration
public class KafkaConfig
{
    @Qualifier("bootstrap-servers")
    @Bean
    String bootstrapServers(@Value("${kafka.bootstrap-servers}") String bootstrapServers)
    {
        return bootstrapServers;
    }

    @Qualifier("kafka-topic")
    @Bean
    String kafkaTopic(@Value("${kafka.topic}") String kafkaTopic)
    {
        return kafkaTopic;
    }
}
