package md.reactive_messaging.jms;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.Either;

import javax.jms.ConnectionFactory;
import javax.jms.JMSRuntimeException;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Optional.empty;
import static md.reactive_messaging.functional.Either.fromOptional;

@Slf4j
@RequiredArgsConstructor
public final class JmsSimplifiedApiManager
{
    private static final Object NO_ERROR = new Object();

    @NonNull
    private final JmsSimplifiedApiOps ops;

    public Optional<JMSRuntimeException> sendTextMessage
            (
                    Function<String, ConnectionFactory> constructor,
                    String url,
                    String userName,
                    String password,
                    String queueName,
                    String text
            )
    {
        return
                ops.instantiateConnectionFactory(constructor, url).flatMap(factory ->
                        ops.createContext(factory, userName, password).flatMap(context -> {
                                    final Either<JMSRuntimeException, Object> sent =
                                            ops.createQueue(context, queueName).flatMap(queue ->
                                                    ops.createProducer(context).flatMap(producer -> {
                                                                final Optional<JMSRuntimeException> error =
                                                                        ops.sendTextMessage(producer, queue, text);
                                                                return fromOptional(error, NO_ERROR);
                                                            }
                                                    )
                                            );
                                    final Optional<JMSRuntimeException> closed = ops.closeContext(context);
                                    return sent.isLeft() ? sent : fromOptional(closed, NO_ERROR);
                                }
                        )
                ).toOptional();
    }

    public Optional<JMSRuntimeException> sendTextMessages
            (
                    Function<String, ConnectionFactory> constructor,
                    String url,
                    String userName,
                    String password,
                    String queueName,
                    Stream<String> texts
            )
    {
        return
                ops.instantiateConnectionFactory(constructor, url).flatMap(factory ->
                        ops.createContext(factory, userName, password).flatMap(context -> {
                                    final Either<JMSRuntimeException, Object> sent =
                                            ops.createQueue(context, queueName).flatMap(queue ->
                                                    ops.createProducer(context).flatMap(producer -> {
                                                                final Optional<JMSRuntimeException> error =
                                                                        texts.map(text ->
                                                                                        ops.sendTextMessage(producer, queue, text)
                                                                                ).
                                                                                filter(Optional::isPresent).findFirst().orElse(empty());
                                                                return fromOptional(error, NO_ERROR);
                                                            }
                                                    )
                                            );
                                    final Optional<JMSRuntimeException> closed = ops.closeContext(context);
                                    return sent.isLeft() ? sent : fromOptional(closed, NO_ERROR);
                                }
                        )
                ).toOptional();
    }
}
