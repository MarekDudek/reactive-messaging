package md.reactive_messaging.jms;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.jms.ConnectionFactory;
import javax.jms.JMSRuntimeException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
public final class JmsSimplifiedApiManager
{
    private static final Consumer<JMSRuntimeException> IGNORE_ERROR = e -> {
    };
    private static final Consumer<JMSRuntimeException> LOG_ERROR = e -> log.error("", e);
    private static final Consumer<JMSRuntimeException> DEFAULT = LOG_ERROR;

    @NonNull
    private final JmsSimplifiedApiOps ops;

    public void sendTextMessage
            (
                    Function<String, ConnectionFactory> constructor,
                    String url,
                    String userName,
                    String password,
                    String queueName,
                    String text
            )
    {
        ops.instantiateConnectionFactory(constructor, url).consume(DEFAULT, factory ->
                ops.createContext(factory, userName, password).consume(DEFAULT, context -> {
                            ops.createQueue(context, queueName).consume(DEFAULT, queue ->
                                    ops.createProducer(context).consume(DEFAULT, producer ->
                                            ops.sendTextMessage(producer, queue, text).ifPresent(DEFAULT)
                                    )
                            );
                            ops.closeContext(context).ifPresent(DEFAULT);
                        }
                )
        );
    }

    public void sendTextMessages
            (
                    Function<String, ConnectionFactory> constructor,
                    String url,
                    String userName,
                    String password,
                    String queueName,
                    Stream<String> texts
            )
    {
        ops.instantiateConnectionFactory(constructor, url).consume(DEFAULT, factory ->
                ops.createContext(factory, userName, password).consume(DEFAULT, context -> {
                            ops.createQueue(context, queueName).consume(DEFAULT, queue ->
                                    ops.createProducer(context).consume(DEFAULT, producer ->
                                            texts.forEach(text ->
                                                    ops.sendTextMessage(producer, queue, text).ifPresent(DEFAULT)
                                            )
                                    )
                            );
                            ops.closeContext(context).ifPresent(DEFAULT);
                        }
                )
        );
    }
}
