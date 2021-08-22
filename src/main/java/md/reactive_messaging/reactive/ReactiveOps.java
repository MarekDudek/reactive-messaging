package md.reactive_messaging.reactive;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.jms.Jms2Ops;
import reactor.core.publisher.Mono;

import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public class ReactiveOps
{
    private final Jms2Ops ops;

    enum Reconnect
    {
        RECONNECT
    }

    @Deprecated // does not use ops
    public Mono<ConnectionFactory> factoryFromCallable(Function<String, ConnectionFactory> constructor, String url)
    {
        return Mono.fromCallable(
                () -> {
                    log.info("Instantiating connection factory");
                    final ConnectionFactory factory = constructor.apply(url);
                    log.info("Instantiated connection factory");
                    return factory;
                }
        );
    }

    public Mono<ConnectionFactory> factoryFromCallable2(Function<String, ConnectionFactory> constructor, String url)
    {
        return Mono.fromCallable(
                () ->
                        ops.instantiateConnectionFactory(constructor, url).apply(
                                error -> {
                                    log.error("Instantiating connection factory failed", error);
                                    throw error;
                                },
                                factory -> factory
                        )
        );
    }

    @Deprecated // does not use ops
    public Mono<JMSContext> contextFromCallable(ConnectionFactory factory, String userName, String password)
    {
        return Mono.fromCallable(
                () -> {
                    log.info("Creating context");
                    final JMSContext context = factory.createContext(userName, password);
                    log.info("Created context");
                    return context;
                }
        );
    }

    @Deprecated // does not use ops; but it logs underlying error
    public Mono<JMSContext> contextFromCallable2(String url, String userName, String password)
    {
        return Mono.fromCallable(() -> {
                    log.info("Creating factory to {}", url);
                    final TibjmsConnectionFactory factory = new TibjmsConnectionFactory(url);
                    log.info("Created factory {}", factory);
                    log.info("Creating context for {} ({})", userName, password);
                    final JMSContext context = factory.createContext(userName, password);
                    log.info("Created context {}", context);
                    return context;
                }
        );
    }

    public Mono<JMSContext> contextFromCallable3(ConnectionFactory factory, String userName, String password)
    {
        return Mono.fromCallable(
                () ->
                        ops.createContext(factory, userName, password).apply(
                                error -> {
                                    log.error("Creating JMS context failed", error);
                                    throw error;
                                },
                                context -> context
                        )
        );
    }

    @Deprecated
    public Mono<JMSContext> createContext(Function<String, ConnectionFactory> constructor, String url, String userName, String password)
    {
        return Mono.create(sink -> {
                    ops.instantiateConnectionFactory(constructor, url).flatMap(factory ->
                            ops.createContext(factory, userName, password)
                    ).consume(
                            sink::error,
                            sink::success
                    );
                }
        );
    }
}
