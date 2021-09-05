package md.reactive_messaging.jms;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.Either;
import md.reactive_messaging.functional.throwing.ThrowingBiConsumer;
import md.reactive_messaging.functional.throwing.ThrowingConsumer;
import md.reactive_messaging.functional.throwing.ThrowingFunction;

import javax.jms.*;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.stream.LongStream;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.function.Function.identity;
import static md.reactive_messaging.functional.Either.right;

@Slf4j
@RequiredArgsConstructor
public final class JmsSimplifiedApiManager
{
    private static final Object NO_ERROR = new Object();

    @NonNull
    private final JmsSimplifiedApiOps ops;

    public Optional<JMSRuntimeException> sendTextMessage
            (
                    @NonNull ThrowingFunction<String, ConnectionFactory, JMSException> constructor,
                    @NonNull String url,
                    @NonNull String userName,
                    @NonNull String password,
                    @NonNull String queueName,
                    @NonNull String text
            )
    {
        return
                ops.connectionFactoryForUrlChecked(constructor, url).biMap(
                        checked ->
                                new JMSRuntimeException(checked.getMessage()),
                        identity()
                ).flatMap(factory ->
                        ops.createContext(factory, userName, password).flatMap(context -> {
                                    Either<JMSRuntimeException, Object> sent =
                                            ops.createQueue(context, queueName).flatMap(queue ->
                                                    ops.createProducer(context).flatMap(producer -> {
                                                                Optional<JMSRuntimeException> error =
                                                                        ops.sendTextMessage(producer, queue, text);
                                                                return error.map(Either::left).orElse(right(NO_ERROR));
                                                            }
                                                    )
                                            );
                                    Optional<JMSRuntimeException> closed = ops.closeContext(context);
                                    return sent.isLeft()
                                            ? sent
                                            : closed.map(Either::left).orElse(right(NO_ERROR));
                                }
                        )
                ).flip().toOptional();
    }

    public Optional<JMSRuntimeException> sendTextMessages
            (
                    @NonNull ThrowingFunction<String, ConnectionFactory, JMSException> constructor,
                    @NonNull String url,
                    @NonNull String userName,
                    @NonNull String password,
                    @NonNull String queueName,
                    @NonNull LongStream ids,
                    @NonNull Function<JMSContext, Message> createMessage,
                    @NonNull ThrowingBiConsumer<Message, Long, JMSException> updateMessage
            )
    {

        return
                ops.connectionFactoryForUrlChecked(constructor, url).biMap(
                        checked ->
                                new JMSRuntimeException(checked.getMessage()),
                        identity()
                ).flatMap(factory ->
                        ops.createContext(factory, userName, password).flatMap(context -> {
                                    Either<JMSRuntimeException, Object> sent =
                                            ops.createQueue(context, queueName).flatMap(queue ->
                                                    ops.createProducer(context).flatMap(producer -> {
                                                                Optional<JMSRuntimeException> error =
                                                                        ids.mapToObj((LongFunction<Optional<JMSRuntimeException>>) id -> {
                                                                                            try
                                                                                            {
                                                                                                try
                                                                                                {
                                                                                                    Message message = createMessage.apply(context);
                                                                                                    updateMessage.accept(message, id);
                                                                                                    producer.send(queue, message);
                                                                                                    return empty();
                                                                                                }
                                                                                                catch (JMSException e)
                                                                                                {
                                                                                                    return of(new JMSRuntimeException(e.getMessage()));
                                                                                                }
                                                                                            }
                                                                                            catch (JMSRuntimeException e)
                                                                                            {
                                                                                                return of(e);
                                                                                            }
                                                                                        }
                                                                                ).
                                                                                filter(Optional::isPresent).findFirst().orElse(empty());
                                                                return error.map(Either::left).orElse(right(NO_ERROR));
                                                            }
                                                    )
                                            );
                                    Optional<JMSRuntimeException> closed = ops.closeContext(context);
                                    return sent.isLeft()
                                            ? sent
                                            : closed.map(Either::left).orElse(right(NO_ERROR));
                                }
                        )
                ).flip().toOptional();
    }
}
