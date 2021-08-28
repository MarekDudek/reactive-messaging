package md.reactive_messaging.reactive;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.throwing.ThrowingFunction;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;

import static md.reactive_messaging.reactive.ReactiveUtils.onEach;
import static reactor.core.scheduler.Schedulers.boundedElastic;

@RequiredArgsConstructor
@Slf4j
public class ReactiveOps
{
    @NonNull
    public final Scheduler publisher;

    public Mono<ConnectionFactory> connectionFactoryForUrl
            (
                    ThrowingFunction<String, ConnectionFactory, JMSException> connectionFactory,
                    String url
            )
    {
        String name = "connection-factory-for-url";
        String wrapper = name + "-wrapper";
        final Mono<ConnectionFactory> blockingWrapper =
                Mono.fromCallable(() ->
                        connectionFactory.apply(url)
                ).doOnEach(onEach(wrapper)).name(wrapper);
        return blockingWrapper.
                subscribeOn(boundedElastic()).
                publishOn(publisher).
                cache().
                doOnEach(onEach(name)).name(name);
    }

    public Mono<JMSContext> contextForCredentials
            (
                    ConnectionFactory factory,
                    String userName, String password
            )
    {
        String name = "context-for-credentials";
        String wrapper = name + "-wrapper";
        final Mono<JMSContext> blockingWrapper =
                Mono.fromCallable(() ->
                        factory.createContext(userName, password)
                ).doOnEach(onEach(wrapper)).name(wrapper);
        return blockingWrapper.
                subscribeOn(boundedElastic()).
                cache().
                doOnEach(onEach(name)).name(name);
    }
}
