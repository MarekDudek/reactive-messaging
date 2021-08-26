package md.reactive_messaging.jms;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.Either;
import md.reactive_messaging.functional.throwing.ThrowingFunction;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Optional.empty;
import static java.util.function.Function.identity;
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
                    @NonNull Function<String, ConnectionFactory> constructor,
                    @NonNull String url,
                    @NonNull String userName,
                    @NonNull String password,
                    @NonNull String queueName,
                    @NonNull String text
            )
    {
        return
                ops.connectionFactoryForUrl(constructor, url).flatMap(factory ->
                        ops.createContext(factory, userName, password).flatMap(context -> {
                                    Either<JMSRuntimeException, Object> sent =
                                            ops.createQueue(context, queueName).flatMap(queue ->
                                                    ops.createProducer(context).flatMap(producer -> {
                                                                Optional<JMSRuntimeException> error =
                                                                        ops.sendTextMessage(producer, queue, text);
                                                                return fromOptional(error, NO_ERROR);
                                                            }
                                                    )
                                            );
                                    Optional<JMSRuntimeException> closed = ops.closeContext(context);
                                    return sent.isLeft() ? sent : fromOptional(closed, NO_ERROR);
                                }
                        )
                ).toOptional();
    }

    public Optional<JMSRuntimeException> sendTextMessages
            (
                    @NonNull ThrowingFunction<String, ConnectionFactory, JMSException> constructor,
                    @NonNull String url,
                    @NonNull String userName,
                    @NonNull String password,
                    @NonNull String queueName,
                    @NonNull Stream<String> texts
            )
    {
        return
                ops.connectionFactoryForUrlChecked(constructor, url).biMap(
                        jmsException ->
                                new JMSRuntimeException(jmsException.getMessage()),
                        identity()
                ).flatMap(factory ->
                        ops.createContext(factory, userName, password).flatMap(context -> {
                                    Either<JMSRuntimeException, Object> sent =
                                            ops.createQueue(context, queueName).flatMap(queue ->
                                                    ops.createProducer(context).flatMap(producer -> {
                                                                Optional<JMSRuntimeException> error =
                                                                        texts.map(text ->
                                                                                        ops.sendTextMessage(producer, queue, text)
                                                                                ).
                                                                                filter(Optional::isPresent).findFirst().orElse(empty());
                                                                return fromOptional(error, NO_ERROR);
                                                            }
                                                    )
                                            );
                                    Optional<JMSRuntimeException> closed = ops.closeContext(context);
                                    return sent.isLeft() ? sent : fromOptional(closed, NO_ERROR);
                                }
                        )
                ).toOptional();
    }
}
