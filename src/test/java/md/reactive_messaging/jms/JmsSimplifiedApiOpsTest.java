package md.reactive_messaging.jms;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.Either;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import javax.jms.*;
import java.util.Optional;

import static md.reactive_messaging.TestTibcoEmsConfig.*;
import static md.reactive_messaging.jms.utils.CheckIdMessageConsumer.CheckIdMessageConsumer;
import static md.reactive_messaging.jms.utils.FailErrorConsumer.FailErrorConsumer;
import static md.reactive_messaging.jms.utils.FailExceptionListener.FailExceptionListener;
import static md.reactive_messaging.jms.utils.FailJmsErrorConsumer.FailJmsErrorConsumer;
import static md.reactive_messaging.jms.utils.LogMessageListener.LogMessageListener;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@TestMethodOrder(OrderAnnotation.class)
@Disabled
final class JmsSimplifiedApiOpsTest
{
    private static final JmsSimplifiedApiOps OPS = new JmsSimplifiedApiOps();

    @Order(1)
    @Test
    void synchronous_send_text_message()
    {
        final Either<JMSRuntimeException, ConnectionFactory> factory =
                OPS.connectionFactoryForUrl(TibjmsConnectionFactory::new, URL);
        final Either<JMSRuntimeException, JMSContext> context =
                factory.flatMap(f -> OPS.createContext(f, USER_NAME, PASSWORD));
        assertThat(context.isRight()).isTrue();
        context.consume(
                FailJmsErrorConsumer,
                c -> OPS.setExceptionListener(c, FailExceptionListener).ifPresent(FailJmsErrorConsumer)
        );
        final Either<JMSRuntimeException, Queue> queue =
                context.flatMap(c -> OPS.createQueue(c, QUEUE_NAME));
        assertThat(queue.isRight()).isTrue();
        final Either<JMSRuntimeException, JMSProducer> producer =
                context.flatMap(OPS::createProducer);
        assertThat(producer.isRight()).isTrue();
        final Either<JMSRuntimeException, Optional<JMSRuntimeException>> status =
                producer.flatMap(p -> queue.map(q -> OPS.sendTextMessage(p, q, "synchronous text message")));
        assertThat(status.isRight()).isTrue();
        assertThat(status.rightOr(null)).isEmpty();
        final Optional<JMSRuntimeException> error =
                OPS.closeContext(context.rightOr(null));
        assertThat(error).isEmpty();
    }

    @Order(2)
    @Test
    void synchronous_receive_text_message()
    {
        final Either<JMSRuntimeException, ConnectionFactory> factory =
                OPS.connectionFactoryForUrl(TibjmsConnectionFactory::new, URL);
        final Either<JMSRuntimeException, JMSContext> context =
                factory.flatMap(f -> OPS.createContext(f, USER_NAME, PASSWORD));
        assertThat(context.isRight()).isTrue();
        context.consume(
                FailJmsErrorConsumer,
                c -> OPS.setExceptionListener(c, FailExceptionListener).ifPresent(FailJmsErrorConsumer)
        );
        final Either<JMSRuntimeException, Queue> queue =
                context.flatMap(c -> OPS.createQueue(c, QUEUE_NAME));
        assertThat(queue.isRight()).isTrue();
        final Either<JMSRuntimeException, JMSConsumer> consumer =
                context.flatMap(c -> queue.flatMap(q -> OPS.createConsumer(c, q)));
        assertThat(consumer.isRight()).isTrue();
        final Either<JMSRuntimeException, String> body =
                consumer.flatMap(c -> OPS.receiveBody(c, String.class));
        assertThat(body.isRight()).isTrue();
        final Optional<JMSRuntimeException> error =
                OPS.closeContext(context.rightOr(null));
        assertThat(error).isEmpty();
    }

    @Order(3)
    @Test
    void asynchronous_send_text_message()
    {
        final Either<JMSRuntimeException, ConnectionFactory> factory =
                OPS.connectionFactoryForUrl(TibjmsConnectionFactory::new, URL);
        final Either<JMSRuntimeException, JMSContext> context =
                factory.flatMap(f -> OPS.createContext(f, USER_NAME, PASSWORD));
        assertThat(context.isRight()).isTrue();
        context.consume(
                FailJmsErrorConsumer,
                c -> OPS.setExceptionListener(c, FailExceptionListener).ifPresent(FailJmsErrorConsumer)
        );
        final Either<JMSRuntimeException, Queue> queue =
                context.flatMap(c -> OPS.createQueue(c, QUEUE_NAME));
        assertThat(queue.isRight()).isTrue();
        final Either<JMSRuntimeException, JMSProducer> producer =
                context.flatMap(OPS::createProducer);
        assertThat(producer.isRight()).isTrue();
        producer.consume(
                FailJmsErrorConsumer,
                p -> OPS.setAsync(p, new CompletionListenerImpl(CheckIdMessageConsumer, FailErrorConsumer)).ifPresent(FailJmsErrorConsumer)
        );
        final Either<JMSRuntimeException, Optional<JMSRuntimeException>> status =
                producer.flatMap(p -> queue.map(q -> OPS.sendTextMessage(p, q, "asynchronous text message")));
        assertThat(status.isRight()).isTrue();
        assertThat(status.rightOr(null)).isEmpty();
        final Optional<JMSRuntimeException> error =
                OPS.closeContext(context.rightOr(null));
        assertThat(error).isEmpty();
    }

    @Order(4)
    @Test
    void asynchronous_receive_text_message() throws InterruptedException
    {
        final Either<JMSRuntimeException, ConnectionFactory> factory =
                OPS.connectionFactoryForUrl(TibjmsConnectionFactory::new, URL);
        final Either<JMSRuntimeException, JMSContext> context =
                factory.flatMap(f -> OPS.createContext(f, USER_NAME, PASSWORD));
        assertThat(context.isRight()).isTrue();
        context.consume(
                FailJmsErrorConsumer,
                c -> OPS.setExceptionListener(c, FailExceptionListener).ifPresent(FailJmsErrorConsumer)
        );
        final Either<JMSRuntimeException, Queue> queue =
                context.flatMap(c -> OPS.createQueue(c, QUEUE_NAME));
        assertThat(queue.isRight()).isTrue();
        final Either<JMSRuntimeException, JMSConsumer> consumer =
                context.flatMap(c -> queue.flatMap(q -> OPS.createConsumer(c, q)));
        assertThat(consumer.isRight()).isTrue();
        final Either<JMSRuntimeException, Optional<JMSRuntimeException>> status =
                consumer.map(c -> OPS.setMessageListener(c, LogMessageListener));
        assertThat(status.isRight()).isTrue();
        assertThat(status.rightOr(null)).isEmpty();
        Thread.sleep(1);
        final Optional<JMSRuntimeException> error =
                OPS.closeContext(context.rightOr(null));
        assertThat(error).isEmpty();
    }
}
