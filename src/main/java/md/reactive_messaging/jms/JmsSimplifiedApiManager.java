package md.reactive_messaging.jms;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.jms.ConnectionFactory;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public final class JmsSimplifiedApiManager
{
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
        ops.instantiateConnectionFactory(constructor, url).consume(
                error -> log.error("", error),
                factory ->
                        ops.createContext(factory, userName, password).consume(
                                error ->
                                        log.error("", error),
                                context -> {
                                    ops.createQueue(context, queueName).consume(
                                            error ->
                                                    log.error("", error),
                                            queue ->
                                                    ops.createProducer(context).consume(
                                                            error ->
                                                                    log.error("", error),
                                                            producer ->
                                                                    ops.sendTextMessage(producer, queue, text).ifPresent(
                                                                            error ->
                                                                                    log.error("", error)
                                                                    )
                                                    )
                                    );
                                    ops.closeContext(context).ifPresent(
                                            error ->
                                                    log.error("", error)
                                    );
                                }
                        )
        );
    }
}
