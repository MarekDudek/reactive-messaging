package md.reactive_messaging.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.Properties;

import static java.time.Duration.ofMillis;
import static md.reactive_messaging.reactive.GenericSubscribers.FluxSubscribers.subscribeAndAwait;
import static md.reactive_messaging.reactive.ReactiveUtils.monitored;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@Slf4j
@Disabled
final class ReactiveKafkaTest
{
    public static final String BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092";

    @Test
    public void sender() throws InterruptedException
    {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        config.put(KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        config.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        SenderOptions<Long, String> options = SenderOptions.create(config);
        KafkaSender<Long, String> sender = KafkaSender.create(options);
        Flux<Long> interval = Flux.interval(ofMillis(100));
        Flux<Long> ticks = monitored(interval, "ticks");
        subscribeAndAwait(ticks);
    }
}
