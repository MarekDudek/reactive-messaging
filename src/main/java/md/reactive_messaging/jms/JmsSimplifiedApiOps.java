package md.reactive_messaging.jms;

import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.Either;
import md.reactive_messaging.functional.throwing.ThrowingBiConsumer;
import md.reactive_messaging.functional.throwing.ThrowingFunction;

import javax.jms.*;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static md.reactive_messaging.jms.JmsSimplifiedApiMetaOps.*;

@Slf4j
public class JmsSimplifiedApiOps
{
    private static final BiConsumer<String, Object[]> ERROR = log::error;

    public Either<JMSRuntimeException, ConnectionFactory> connectionFactoryForUrl(Function<String, ConnectionFactory> constructor, String url)
    {
        return function(constructor, url, "connection-factory-for-url", log::info, ERROR);
    }

    public Either<JMSException, ConnectionFactory> connectionFactoryForUrlChecked(ThrowingFunction<String, ConnectionFactory, JMSException> constructor, String url)
    {
        return throwingFunction(constructor, url, "connection-factory-for-url-checked", log::info, ERROR);
    }

    public Either<JMSRuntimeException, JMSContext> createContext(ConnectionFactory factory, String userName, String password)
    {
        return biFunction(factory::createContext, userName, password, "create-context", log::info, ERROR);
    }

    public Optional<JMSRuntimeException> closeContext(JMSContext context)
    {
        return runnable(context::close, "close-context", log::info, ERROR);
    }

    public Optional<JMSRuntimeException> setExceptionListener(JMSContext context, ExceptionListener listener)
    {
        return consumer(context::setExceptionListener, listener, "set-exception-listener", log::debug, ERROR);
    }

    public Either<JMSRuntimeException, Queue> createQueue(JMSContext context, String queueName)
    {
        return function(context::createQueue, queueName, "create-queue", log::debug, ERROR);
    }

    public Either<JMSRuntimeException, JMSProducer> createProducer(JMSContext context)
    {
        return supplier(context::createProducer, "create-producer", log::debug, log::debug);
    }

    public Optional<JMSRuntimeException> setAsync(JMSProducer producer, CompletionListener completionListener)
    {
        return consumer(producer::setAsync, completionListener, "set-async", log::debug, ERROR);
    }

    public Optional<JMSRuntimeException> sendTextMessage(JMSProducer producer, Destination destination, String text)
    {
        return biConsumer(producer::send, destination, text, "send-text-message", log::trace, ERROR);
    }

    public Optional<JMSRuntimeException> sendTextMessage(JMSProducer producer, Destination destination, TextMessage message)
    {
        return biConsumer(producer::send, destination, message, "send-text-message", log::trace, ERROR);
    }

    public Optional<JMSRuntimeException> sendMessage(JMSProducer producer, Destination destination, Message message)
    {
        return biConsumer(producer::send, destination, message, "send-message", log::trace, ERROR);
    }

    public Either<JMSRuntimeException, JMSConsumer> createConsumer(JMSContext context, Queue queue)
    {
        return function(context::createConsumer, queue, "create-consumer", log::debug, ERROR);
    }

    public Optional<JMSRuntimeException> setMessageListener(JMSConsumer consumer, MessageListener listener)
    {
        return consumer(consumer::setMessageListener, listener, "set-message-listener", log::debug, ERROR);
    }

    public <BODY> Either<JMSRuntimeException, BODY> receiveBody(JMSConsumer consumer, Class<BODY> klass)
    {
        return function(consumer::receiveBody, klass, "receive-body", log::trace, ERROR);
    }

    public <R> Either<JMSException, R> applyToMessage(Message message, ThrowingFunction<Message, R, JMSException> function)
    {
        return throwingFunction(function, message, "apply-to-message", log::trace, log::error);
    }

    public static <T> Optional<JMSException> acceptWithMessage(ThrowingBiConsumer<String, T, JMSException> consumer, String name, T value)
    {
        return throwingBiConsumer(consumer, name, value, "accept-with-message", log::trace, log::error);
    }
}
