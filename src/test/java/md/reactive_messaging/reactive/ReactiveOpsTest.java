package md.reactive_messaging.reactive;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.jms.JmsSimplifiedApiOps;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import reactor.core.publisher.Flux;

import javax.jms.Message;
import java.time.Duration;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Thread.sleep;
import static java.time.Duration.ofSeconds;
import static md.reactive_messaging.TestTibcoEmsConfig.*;

@Slf4j
@TestMethodOrder(OrderAnnotation.class)
final class ReactiveOpsTest
{
    private static final ReactiveOps OPS = new ReactiveOps(new JmsSimplifiedApiOps());
    private static final ReactivePublishers publishers = new ReactivePublishers(OPS);

    private static final long MAX_ATTEMPTS = MAX_VALUE;
    private static final Duration MIN_BACKOFF = ofSeconds(1);
    private static final Duration TEST_DURATION = ofSeconds(3);

    @Test
    void receiving_bodies_synchronously() throws InterruptedException
    {
        final Flux<String> messageBodies =
                publishers.syncMessages(
                        TibjmsConnectionFactory::new, URL, URL,
                        USER_NAME, PASSWORD,
                        String.class,
                        MAX_ATTEMPTS, MIN_BACKOFF
                );

        new Thread(() ->
                messageBodies.subscribe(
                        body -> log.info("Body: {}", body),
                        error -> log.error("Error: ", error),
                        () -> log.info("Completed")
                )
        ).start();

        sleep(TEST_DURATION.toMillis());
    }

    @Test
    void receiving_messages_asynchronously() throws InterruptedException
    {
        final Flux<Message> messageFlux = publishers.asyncMessages(
                TibjmsConnectionFactory::new, URL, URL,
                USER_NAME, PASSWORD,
                MAX_ATTEMPTS, MIN_BACKOFF
        );

        new Thread(() ->
                messageFlux.subscribe(
                        message -> log.info("Message: {}", message),
                        error -> log.error("Error: ", error),
                        () -> log.info("Completed")
                )
        ).start();

        sleep(TEST_DURATION.toMillis());
    }
}
