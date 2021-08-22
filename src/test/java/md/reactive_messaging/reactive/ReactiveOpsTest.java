package md.reactive_messaging.reactive;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.jms.Jms2Ops;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSRuntimeException;
import java.util.Optional;

import static md.reactive_messaging.TestTibcoEmsConfig.*;

@Slf4j
@TestMethodOrder(OrderAnnotation.class)
final class ReactiveOpsTest
{
    private static final Jms2Ops JOPS = new Jms2Ops();
    private final ReactiveOps ROPS = new ReactiveOps(JOPS);

    @Order(1)
    @Test
    void connection_factory()
    {
        final Mono<ConnectionFactory> mono = ROPS.factoryFromCallable(TibjmsConnectionFactory::new, URL);
        mono.subscribe(factory ->
                log.info("{}", factory
                )
        );
    }

    @Order(2)
    @Test
    void context()
    {
        final Mono<JMSContext> mono =
                ROPS.factoryFromCallable(TibjmsConnectionFactory::new, URL).flatMap(factory ->
                        ROPS.contextFromCallable(factory, USER_NAME, PASSWORD)
                );
        mono.subscribe(context -> {
                    log.info("{}", context);
                    JOPS.closeContext(context).ifPresent(e ->
                            log.error("Error while closing connection: ", e)
                    );
                }
        );
    }

    @Order(3)
    @Test
    void contexts()
    {
        final Mono<ConnectionFactory> factoryM =
                ROPS.factoryFromCallable(TibjmsConnectionFactory::new, URL);
        final Many<Object> reconnectM = Sinks.many().unicast().onBackpressureBuffer();
        final Object reconnect = new Object();
        final Mono<JMSContext> contextM = factoryM.flatMap(factory ->
                ROPS.contextFromCallable(factory, USER_NAME, PASSWORD).map(context -> {
                    final Optional<JMSRuntimeException> settingError =
                            JOPS.setExceptionListener(context, error -> reconnectM.tryEmitNext(reconnect));
                    settingError.ifPresent(error -> reconnectM.tryEmitNext(reconnect));
                    return context;
                })
        );

        contextM.subscribe(
                context -> {
                    log.info("Context: {}", context);
                },
                error -> {
                    log.error("Error: ", error);
                },
                () -> {
                    log.info("Completed");
                }
        );

    }
}
