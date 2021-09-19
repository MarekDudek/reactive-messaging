package md.reactive_messaging.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static java.util.Optional.empty;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@Slf4j
final class BasicKafkaTest
{
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "test-topic";

    private AdminClient createAdmin()
    {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        return AdminClient.create(config);
    }

    @Test
    void createTopic() throws ExecutionException, InterruptedException
    {
        AdminClient admin = createAdmin();
        CreateTopicsResult result = admin.createTopics(singletonList(new NewTopic(TOPIC, empty(), empty())));
        result.all().get();
        admin.close();
    }

    @Test
    void producer() throws ExecutionException, InterruptedException
    {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        config.put(KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        config.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        Producer<Long, String> producer = new KafkaProducer<>(config);
        ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, 1L, "test value");
        Future<RecordMetadata> future = producer.send(record);
        producer.flush();
        RecordMetadata metadata = future.get();
        log.info("Record metadata: {}", metadata);
        producer.close();
    }

    @Test
    void consumer()
    {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        config.put(KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        config.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(GROUP_ID_CONFIG, "test-group");
        //config.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        Consumer<Long, String> consumer = new KafkaConsumer<>(config);
        consumer.subscribe(singletonList(TOPIC));
        ConsumerRecords<Long, String> records = consumer.poll(ofSeconds(1));
        records.forEach(record -> log.info("Record: {}", record));
    }

    @Test
    void deleteTopic() throws ExecutionException, InterruptedException
    {
        AdminClient admin = createAdmin();
        DeleteTopicsResult result = admin.deleteTopics(singletonList(TOPIC));
        result.all().get();
        admin.close();
    }
}
