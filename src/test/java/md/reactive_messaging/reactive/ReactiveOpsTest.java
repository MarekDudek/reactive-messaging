package md.reactive_messaging.reactive;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.TestTibcoEmsConfig;
import md.reactive_messaging.functional.throwing.ThrowingFunction;
import md.reactive_messaging.jms.MessageConverters;
import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.test.StepVerifier;

import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;

import static md.reactive_messaging.TestTibcoEmsConfig.*;
import static md.reactive_messaging.functional.Functional.ignore;
import static md.reactive_messaging.reactive.GenericSubscribers.FluxSubscribers.subscribeAndAwait;
import static md.reactive_messaging.reactive.ReactiveUtils.fromCallable;
import static org.apache.commons.lang3.StringUtils.repeat;
import static reactor.core.scheduler.Schedulers.newSingle;

@Slf4j
@Disabled
final class ReactiveOpsTest
{
    private static final Scheduler CONNECTION_FACTORY_PUBLISHER =
            newSingle("connection-factory-publisher");

    private static final ReactiveOps OPS =
            new ReactiveOps();


    private static final String BROKER_UP = "broker-up";
    private static final String BROKER_DOWN = "broker-down";


    @BeforeEach
    void setUp(TestInfo info)
    {
        log.info(repeat('#', 10) + " " + info.getDisplayName());
    }

    @Tag(BROKER_UP)
    @Tag(BROKER_DOWN)
    @DisplayName("Creating connection factory works no matter if broker is up or down")
    @Test
    void connection_factory()
    {
        final Mono<ConnectionFactory> factoryM =
                fromCallable(() ->
                                ((ThrowingFunction<String, ConnectionFactory, JMSException>) TibjmsConnectionFactory::new).apply(URL),
                        ignore(), "Creating connection factory for URL"
                );

        StepVerifier.create(factoryM).
                expectSubscription().
                expectNextCount(1).
                verifyComplete();
    }

    @Tag(BROKER_UP)
    @DisplayName("Creating context produces result and completes")
    @Test
    void context_broker_up()
    {
        Mono<JMSContext> contextM =
                fromCallable(() ->
                                ((ThrowingFunction<String, ConnectionFactory, JMSException>) TibjmsConnectionFactory::new).apply(URL),
                        ignore(), "Creating connection factory for URL"
                ).flatMap(factory ->
                        OPS.contextForCredentials(factory, USER_NAME, PASSWORD, null)
                );

        StepVerifier.create(contextM).
                expectSubscription().
                expectNextCount(1).
                verifyComplete();
    }

    @Tag(BROKER_DOWN)
    @DisplayName("Creating context errors")
    @Test
    void context__broker_down()
    {
        Mono<JMSContext> contextM =
                fromCallable(() ->
                                ((ThrowingFunction<String, ConnectionFactory, JMSException>) TibjmsConnectionFactory::new).apply(URL),
                        ignore(), "Creating connection factory for URL"
                ).flatMap(factory ->
                        OPS.contextForCredentials(factory, USER_NAME, PASSWORD, null)
                );

        StepVerifier.create(contextM).
                expectSubscription().
                verifyError();
    }


    @Tag(BROKER_UP)
    @DisplayName("Listening to messages")
    @Test
    <T> void messages() throws InterruptedException
    {
        final Flux<String> messages =
                OPS.messages(
                        TibjmsConnectionFactory::new, URL,
                        USER_NAME, PASSWORD,
                        QUEUE_NAME,
                        MessageConverters::formatStringBodyWithDeliveryDelay,
                        MAX_ATTEMPTS, MIN_BACKOFF, TestTibcoEmsConfig.MAX_BACKOFF
                );
        subscribeAndAwait(messages);
    }
}
