package md.reactive_messaging.reactive;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.Either;
import md.reactive_messaging.jms.JmsSimplifiedApiOps;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Many;

import javax.jms.ConnectionFactory;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Message;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Optional.ofNullable;
import static md.reactive_messaging.functional.Either.right;
import static md.reactive_messaging.functional.Functional.error;
import static md.reactive_messaging.reactive.Reconnect.RECONNECT;
import static reactor.core.publisher.Sinks.EmitResult.OK;

@Slf4j
@RequiredArgsConstructor
public class ReactiveOps
{
    @NonNull
    public final JmsSimplifiedApiOps ops;


    public Mono<ConnectionFactory> factory(Function<String, ConnectionFactory> constructor, String url)
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

    public Mono<JMSContext> context(ConnectionFactory factory, String userName, String password)
    {
        return Mono.fromCallable(() ->
                ops.createContext(factory, userName, password).apply(
                        error -> {
                            log.error("Creating context failed", error);
                            throw error;
                        },
                        context -> context
                )
        );
    }

    public JMSContext setExceptionListener(JMSContext context, Many<Reconnect> reconnect)
    {
        ops.setExceptionListener(context,
                errorInContext -> {
                    log.error("Heard error in context {}, trying to reconnect", context, errorInContext);
                    reconnect.tryEmitNext(RECONNECT);
                }
        ).ifPresent(settingListenerError -> {
                    log.error("Setting exception listener on context {} failed", context, settingListenerError);
                    reconnect.tryEmitNext(RECONNECT);
                }
        );
        return context;
    }

    public Mono<JMSConsumer> createQueueConsumer(JMSContext context, String queueName, Many<Reconnect> reconnect)
    {
        return ops.createQueue(context, queueName).flatMap(queue ->
                ops.createConsumer(context, queue)
        ).apply(
                error -> {
                    log.error("Creating consumer or queue {} in context {} failed", queueName, context, error);
                    reconnect.tryEmitNext(RECONNECT);
                    return Mono.error(error);
                },
                Mono::just
        );
    }

    public <T> Flux<T> receiveMessageBodies(JMSConsumer consumer, Class<T> klass, Many<Reconnect> reconnect)
    {
        return Flux.generate(sink ->
                ops.receiveBody(consumer, klass).consume(
                        error -> {
                            log.error("Receiving message body failed on consumer {}", consumer, error);
                            reconnect.tryEmitNext(RECONNECT);
                            sink.error(error);
                        },
                        sink::next
                )
        );
    }

    public void setMessageListener(JMSConsumer consumer, Many<Reconnect> reconnect, Many<Message> messages)
    {
        ops.setMessageListener(consumer,
                message -> {
                    log.trace("Received {}", message);
                    messages.tryEmitNext(message);
                }
        ).ifPresent(settingListenerError -> {
                    log.error("Setting message listener on consumer {} failed", consumer, settingListenerError);
                    reconnect.tryEmitNext(RECONNECT);
                }
        );
    }

    public static <T> Consumer<Signal<T>> onEach(String name)
    {
        return signal -> {
            switch (signal.getType())
            {
                case ON_NEXT:
                    log.info("Next in {} - {}", name, signal.get());
                    break;
                case ON_COMPLETE:
                    log.info("Completed {} with {}", name, signal.get());
                    break;
                case ON_ERROR:
                    log.info("Error in {} - {}", name, ofNullable(signal.getThrowable()).map(
                            Throwable::getMessage
                    ).orElse(
                            "!NO MESSAGE!"
                    ));
                    break;
                default:
                    log.warn("Unknown signal - {}", signal);
                    break;
            }
        };
    }

    public static Either<EmitResult, EmitResult> nextReconnect(Many<Reconnect> reconnect)
    {
        return tryNextEmission(reconnect, RECONNECT);
    }

    public static <T> Either<EmitResult, EmitResult> tryNextEmission(Many<T> sink, T decoded)
    {
        final EmitResult emitted = sink.tryEmitNext(decoded);
        log.info("Emitting {}", decoded);
        return right(emitted).filter(result -> result == OK);
    }

    public static void notifyOnFailure(Either<EmitResult, EmitResult> result)
    {
        result.consume(
                failure ->
                        log.error("Failed to emit {}", failure),
                success ->
                        log.info("Emitted OK")
        );
    }

    public static void failOnFailure(Either<EmitResult, EmitResult> result)
    {
        result.consume(
                failure ->
                        error(new IllegalStateException(failure.toString())),
                success ->
                        log.trace("Emitted")
        );
    }
}
