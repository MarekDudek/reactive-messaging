package md.reactive_messaging.reactive;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.jms.Jms2Ops;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;

import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import java.time.Duration;
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

    public Flux<JMSContext> contexts(ConnectionFactory factory, String userName, String password, long maxAttempts, Duration minBackoff)
    {
        final Sinks.Many<Reconnect> reconnect = Sinks.many().unicast().onBackpressureBuffer();
        return contextFromCallable(factory, userName, password).
                retryWhen(Retry.backoff(maxAttempts, minBackoff)).
                repeatWhen(repeat -> reconnect.asFlux());
    }
}
