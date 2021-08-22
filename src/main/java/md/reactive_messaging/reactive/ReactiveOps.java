package md.reactive_messaging.reactive;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.jms.JmsSimplifiedApiOps;
import reactor.core.publisher.Mono;

import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public class ReactiveOps
{
    private final JmsSimplifiedApiOps ops;

    public Mono<ConnectionFactory> factoryFromCallable(Function<String, ConnectionFactory> constructor, String url)
    {
        return Mono.fromCallable(() ->
                ops.instantiateConnectionFactory(constructor, url).apply(
                        error -> {
                            log.error("Instantiating connection factory failed", error);
                            throw error;
                        },
                        factory -> factory
                )
        );
    }

    public Mono<JMSContext> contextFromCallable(ConnectionFactory factory, String userName, String password)
    {
        return Mono.fromCallable(() ->
                ops.createContext(factory, userName, password).apply(
                        error -> {
                            log.error("Creating JMS context failed", error);
                            throw error;
                        },
                        context -> context
                )
        );
    }
}
