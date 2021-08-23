package md.reactive_messaging.configs;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class ReactivePublishersConfig
{
    @Qualifier("max-attempts")
    @Bean
    long maxAttempts(@Value("${async.receiver.max-attempts}") long maxAttempts)
    {
        return maxAttempts;
    }

    @Qualifier("min-backoff")
    @Bean
    Duration minBackoff(@Value("${async.receiver.min-backoff}") Duration minBackoff)
    {
        return minBackoff;
    }
}
