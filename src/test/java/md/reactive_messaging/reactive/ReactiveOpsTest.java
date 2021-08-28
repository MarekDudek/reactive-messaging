package md.reactive_messaging.reactive;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.test.StepVerifier;

import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;

import static md.reactive_messaging.TestTibcoEmsConfig.*;
import static org.apache.commons.lang3.StringUtils.repeat;
import static reactor.core.scheduler.Schedulers.newSingle;

@Slf4j
final class ReactiveOpsTest
{
    private static Scheduler CONNECTION_FACTORY_PUBLISHER =
            newSingle("connection-factory-publisher");
    private static final ReactiveOps OPS =
            new ReactiveOps(CONNECTION_FACTORY_PUBLISHER);

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
        final Mono<ConnectionFactory> factory =
                OPS.connectionFactoryForUrl(TibjmsConnectionFactory::new, URL);

        StepVerifier.create(factory).
                expectSubscription().
                expectNextCount(1).
                verifyComplete();
    }

    @Tag(BROKER_UP)
    @DisplayName("Creating context produces result and completes")
    @Test
    void context_broker_up()
    {
        Mono<JMSContext> context =
                OPS.connectionFactoryForUrl(TibjmsConnectionFactory::new, URL).flatMap(factory ->
                        OPS.contextForCredentials(factory, USER_NAME, PASSWORD)
                );

        StepVerifier.create(context).
                expectSubscription().
                expectNextCount(1).
                verifyComplete();
    }

    @Tag(BROKER_DOWN)
    @DisplayName("Creating context errors")
    @Test
    void context__broker_down()
    {
        Mono<JMSContext> context =
                OPS.connectionFactoryForUrl(TibjmsConnectionFactory::new, URL).flatMap(factory ->
                        OPS.contextForCredentials(factory, USER_NAME, PASSWORD)
                );

        StepVerifier.create(context).
                expectSubscription().
                verifyError();
    }
}
