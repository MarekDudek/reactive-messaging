package md.reactive_messaging.reactive;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.jms.JmsSimplifiedApiOps;
import md.reactive_messaging.reactive.ReactiveOps.Reconnect;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

import javax.jms.JMSConsumer;
import javax.jms.Message;
import java.time.Duration;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Thread.sleep;
import static java.time.Duration.ofSeconds;
import static md.reactive_messaging.TestTibcoEmsConfig.*;
import static reactor.util.retry.Retry.backoff;

@Slf4j
@TestMethodOrder(OrderAnnotation.class)
final class ReactiveOpsTest
{
    private static final ReactiveOps OPS = new ReactiveOps(new JmsSimplifiedApiOps());

    private static final long MAX_ATTEMPTS = MAX_VALUE;
    private static final Duration MIN_BACKOFF = ofSeconds(1);
    private static final Duration TEST_DURATION = ofSeconds(3);

    @Test
    void receiving_bodies_synchronously() throws InterruptedException
    {
        final Many<Reconnect> reconnect = Sinks.many().unicast().onBackpressureBuffer();

        final Flux<JMSConsumer> consumers =
                OPS.factory(TibjmsConnectionFactory::new, URL).flatMap(factory ->
                                OPS.context(factory, USER_NAME, PASSWORD).map(context ->
                                        OPS.setExceptionListener(context, reconnect)
                                )
                        ).
                        retryWhen(backoff(MAX_ATTEMPTS, MIN_BACKOFF)).
                        repeatWhen(repeat -> reconnect.asFlux()).flatMap(context ->
                                OPS.createQueueConsumer(context, QUEUE_NAME, reconnect)
                        );

        final Flux<String> messageBodies =
                consumers.flatMap(consumer ->
                        OPS.receiveMessageBodies(consumer, String.class, reconnect)
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
        final Many<Reconnect> reconnect = Sinks.many().unicast().onBackpressureBuffer();

        final Flux<JMSConsumer> consumers =
                OPS.factory(TibjmsConnectionFactory::new, URL).flatMap(factory ->
                                OPS.context(factory, USER_NAME, PASSWORD).map(context ->
                                        OPS.setExceptionListener(context, reconnect)
                                )
                        ).
                        retryWhen(backoff(MAX_ATTEMPTS, MIN_BACKOFF)).
                        repeatWhen(repeat -> reconnect.asFlux()).flatMap(context ->
                                OPS.createQueueConsumer(context, QUEUE_NAME, reconnect)
                        );

        final Many<Message> messages = Sinks.many().unicast().onBackpressureBuffer();

        consumers.doOnNext(consumer ->
                OPS.setMessageListener(consumer, reconnect, messages)
        ).subscribe();

        final Flux<Message> flux = messages.asFlux();

        new Thread(() ->
                flux.subscribe(
                        message -> log.info("Message: {}", message),
                        error -> log.error("Error: ", error),
                        () -> log.info("Completed")
                )
        ).start();

        sleep(TEST_DURATION.toMillis());
    }
}
