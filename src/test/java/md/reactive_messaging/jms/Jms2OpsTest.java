package md.reactive_messaging.jms;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.Either;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import javax.jms.*;
import java.util.Optional;

import static md.reactive_messaging.jms.TestTibcoEmsConfig.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
@TestMethodOrder(OrderAnnotation.class)
final class Jms2OpsTest
{
    private static final Jms2Ops OPS = new Jms2Ops();

    @Order(1)
    @Test
    void synchronous_send_text_message()
    {
        final Either<JMSRuntimeException, ConnectionFactory> factory =
                OPS.instantiateConnectionFactory(TibjmsConnectionFactory::new, URL);
        final Either<JMSRuntimeException, JMSContext> context =
                factory.flatMap(f -> OPS.createContext(f, USER_NAME, PASSWORD));
        assertThat(context.isRight()).isTrue();
        context.consume(
                e -> fail(),
                c -> OPS.setExceptionListener(c, error -> fail()).ifPresent(exception -> fail())
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
                OPS.instantiateConnectionFactory(TibjmsConnectionFactory::new, URL);
        final Either<JMSRuntimeException, JMSContext> context =
                factory.flatMap(f -> OPS.createContext(f, USER_NAME, PASSWORD));
        assertThat(context.isRight()).isTrue();
        context.consume(
                e -> fail(),
                c -> OPS.setExceptionListener(c, error -> fail()).ifPresent(exception -> fail())
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
                OPS.instantiateConnectionFactory(TibjmsConnectionFactory::new, URL);
        final Either<JMSRuntimeException, JMSContext> context =
                factory.flatMap(f -> OPS.createContext(f, USER_NAME, PASSWORD));
        assertThat(context.isRight()).isTrue();
        context.consume(
                e -> fail(),
                c -> OPS.setExceptionListener(c, error -> fail()).ifPresent(exception -> fail())
        );
        final Either<JMSRuntimeException, Queue> queue =
                context.flatMap(c -> OPS.createQueue(c, QUEUE_NAME));
        assertThat(queue.isRight()).isTrue();
        final Either<JMSRuntimeException, JMSProducer> producer =
                context.flatMap(OPS::createProducer);
        assertThat(producer.isRight()).isTrue();
        producer.consume(
                e -> fail(),
                p -> OPS.setAsynch(p, new CompletionListenerImpl(
                                message -> log.info("Message sending completed {}", message),
                                (message, error) -> log.error("Error sending message {}", message, error)
                        )
                )
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
                OPS.instantiateConnectionFactory(TibjmsConnectionFactory::new, URL);
        final Either<JMSRuntimeException, JMSContext> context =
                factory.flatMap(f -> OPS.createContext(f, USER_NAME, PASSWORD));
        assertThat(context.isRight()).isTrue();
        context.consume(
                e -> fail(),
                c -> OPS.setExceptionListener(c, error -> fail()).ifPresent(exception -> fail())
        );
        final Either<JMSRuntimeException, Queue> queue =
                context.flatMap(c -> OPS.createQueue(c, QUEUE_NAME));
        assertThat(queue.isRight()).isTrue();
        final Either<JMSRuntimeException, JMSConsumer> consumer =
                context.flatMap(c -> queue.flatMap(q -> OPS.createConsumer(c, q)));
        assertThat(consumer.isRight()).isTrue();
        final Either<JMSRuntimeException, Optional<JMSRuntimeException>> status =
                consumer.map(c -> OPS.setMessageListener(c, message -> log.info("Message heard '{}'", message)));
        assertThat(status.isRight()).isTrue();
        assertThat(status.rightOr(null)).isEmpty();
        Thread.sleep(1);
        final Optional<JMSRuntimeException> error =
                OPS.closeContext(context.rightOr(null));
        assertThat(error).isEmpty();
    }
}
