package md.reactive_messaging.reactive;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import javax.jms.ConnectionFactory;
import javax.jms.JMSConsumer;
import javax.jms.Message;
import java.time.Duration;
import java.util.function.Function;

import static reactor.util.retry.Retry.backoff;

@RequiredArgsConstructor
@Slf4j
public class ReactivePublishers
{
    @NonNull
    public final ReactiveOps ops;

    public <T> Flux<T> syncMessages
            (
                    Function<String, ConnectionFactory> connectionFactory,
                    String url,
                    String userName,
                    String password,
                    String queueName,
                    Class<T> klass,
                    long maxAttempts,
                    Duration minBackoff
            )
    {

        final Sinks.Many<ReactiveOps.Reconnect> reconnect = Sinks.many().unicast().onBackpressureBuffer();

        final Mono<JMSConsumer> monitoredConsumer =
                ops.factory(connectionFactory, url).flatMap(factory ->
                        ops.context(factory, userName, password).map(context ->
                                ops.setExceptionListener(context, reconnect)
                        )
                ).flatMap(context ->
                        ops.createQueueConsumer(context, queueName, reconnect)
                );

        final Flux<T> single =
                monitoredConsumer.flatMapMany(consumer ->
                        ops.receiveMessageBodies(consumer, klass, reconnect)
                );

        return single.
                retryWhen(backoff(maxAttempts, minBackoff)).
                repeatWhen(repeat -> reconnect.asFlux());
    }

    public Flux<Message> asyncMessages
            (
                    Function<String, ConnectionFactory> connectionFactory,
                    String url,
                    String userName,
                    String password,
                    String queueName,
                    long maxAttempts,
                    Duration minBackoff
            )
    {
        final Sinks.Many<ReactiveOps.Reconnect> reconnect = Sinks.many().unicast().onBackpressureBuffer();

        final Mono<JMSConsumer> monitoredConsumer =
                ops.factory(connectionFactory, url).flatMap(factory ->
                        ops.context(factory, userName, password).map(context ->
                                ops.setExceptionListener(context, reconnect)
                        )
                ).flatMap(monitoredContext ->
                        ops.createQueueConsumer(monitoredContext, queueName, reconnect)
                );

        final Flux<JMSConsumer> reconnections =
                monitoredConsumer.
                        retryWhen(backoff(maxAttempts, minBackoff)).
                        repeatWhen(repeat -> reconnect.asFlux());

        return
                reconnections.flatMap(consumer -> {
                            Sinks.Many<Message> messages = Sinks.many().unicast().onBackpressureBuffer();
                            ops.ops.setMessageListener(consumer, messages::tryEmitNext);
                            return messages.asFlux();
                        }
                );
    }
}
