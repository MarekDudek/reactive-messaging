package md.reactive_messaging.reactive;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.jms.JmsSimplifiedApiOps;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import javax.jms.Message;
import java.time.Duration;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Thread.sleep;
import static java.time.Duration.ofSeconds;
import static md.reactive_messaging.TestTibcoEmsConfig.*;

@Slf4j
@TestMethodOrder(OrderAnnotation.class)
final class ReactivePublishersTest
{
    private static final ReactiveOps OPS = new ReactiveOps(new JmsSimplifiedApiOps());
    private static final ReactivePublishers publishers = new ReactivePublishers(OPS);

    private static final long MAX_ATTEMPTS = MAX_VALUE;
    private static final Duration MIN_BACKOFF = ofSeconds(1);
    private static final Duration TEST_DURATION = ofSeconds(1);

    @Test
    void receiving_bodies_synchronously() throws InterruptedException
    {
        final Flux<String> messageBodies =
                publishers.syncMessages(
                        TibjmsConnectionFactory::new, URL,
                        USER_NAME, PASSWORD,
                        QUEUE_NAME,
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
                TibjmsConnectionFactory::new, URL,
                USER_NAME, PASSWORD,
                QUEUE_NAME, message -> message,
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

    @Test
    void wellBehavedWorksCorrectlyOnBrokerDown() throws InterruptedException
    {
        final Flux<?> flux =
                publishers.asyncMessages5(
                        TibjmsConnectionFactory::new, URL,
                        USER_NAME, PASSWORD,
                        QUEUE_NAME, message -> "test message",
                        MAX_ATTEMPTS, MIN_BACKOFF
                );
        StepVerifier.create(flux).
                expectSubscription().
                verifyTimeout(ofSeconds(10));
    }

    @Test
    void wellBehavedWorksCorrectlyOnBrokerUp() throws InterruptedException
    {
        final Flux<?> flux =
                publishers.asyncMessages5(
                        TibjmsConnectionFactory::new, URL,
                        USER_NAME, PASSWORD,
                        QUEUE_NAME, message -> "test message",
                        MAX_ATTEMPTS, MIN_BACKOFF
                );
        StepVerifier.create(flux).
                expectSubscription().
                expectNextCount(1).
                verifyComplete();
    }

    @Test
    void wellBehavedWorksCorrectlyOnBrokerRestarting() throws InterruptedException
    {
        final Flux<?> flux =
                publishers.asyncMessages5(
                        TibjmsConnectionFactory::new, URL,
                        USER_NAME, PASSWORD,
                        QUEUE_NAME, message -> "test message",
                        MAX_ATTEMPTS, MIN_BACKOFF
                );
        StepVerifier.create(flux).
                expectSubscription().
                expectNextMatches(next -> true).
                thenRequest(1).
                verifyComplete();
    }

    @Test
    void wellBehavedInTheFree() throws InterruptedException
    {
        final Flux<?> flux =
                publishers.asyncMessages5(
                        TibjmsConnectionFactory::new, URL,
                        USER_NAME, PASSWORD,
                        QUEUE_NAME, message -> "CONSTANT MESSAGE",
                        MAX_ATTEMPTS, MIN_BACKOFF
                );
        flux.subscribe(
                success ->
                        log.info("Success {}", success),
                error ->
                        log.error("Error", error),
                () ->
                        log.warn("Completed")
        );
        log.info("Finish");
        Thread.sleep(Duration.ofMinutes(60).toMillis());
    }
}
