package md.reactive_messaging.reactive;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import javax.jms.JMSConsumer;
import javax.jms.Message;
import java.time.Duration;

import static reactor.util.retry.Retry.backoff;

@RequiredArgsConstructor
@Slf4j
public class ReactivePublishers
{
    @NonNull
    private final ReactiveOps ops;

    public Flux<Message> asyncMessages
            (
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
                ops.factory(TibjmsConnectionFactory::new, url).flatMap(factory ->
                        ops.context(factory, userName, password).map(context ->
                                ops.setExceptionListener(context, reconnect)
                        )
                ).flatMap(context ->
                        ops.createQueueConsumer(context, queueName, reconnect)
                );
        final Flux<JMSConsumer> reconnections =
                monitoredConsumer.
                        retryWhen(backoff(maxAttempts, minBackoff)).
                        repeatWhen(repeat -> reconnect.asFlux());

        final Sinks.Many<Message> messages = Sinks.many().unicast().onBackpressureBuffer();

        reconnections.doOnNext(consumer ->
                ops.setMessageListener(consumer, reconnect, messages)
        ).subscribe(
                context -> {
                    log.info("Context {}", context);
                },
                error -> {
                    log.error("Error ", error);
                },
                () -> {
                    log.error("Complete");
                }
        );

        return messages.asFlux();
    }
}
