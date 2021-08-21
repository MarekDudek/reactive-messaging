package md.reactive_messaging.jms;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.Either;
import org.junit.jupiter.api.Test;

import javax.jms.*;
import java.util.Optional;

import static md.reactive_messaging.jms.TestTibcoEmsConfig.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
final class Jms2OpsTest
{
    private static final Jms2Ops OPS = new Jms2Ops();

    @Test
    void synchronous_send_text_message()
    {
        final ConnectionFactory factory =
                new TibjmsConnectionFactory(URL);
        final Either<JMSRuntimeException, JMSContext> context =
                OPS.createContext(factory, USER_NAME, PASSWORD);
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

    @Test
    void synchronous_receive_text_message()
    {
        final ConnectionFactory factory =
                new TibjmsConnectionFactory(URL);
        final Either<JMSRuntimeException, JMSContext> context =
                OPS.createContext(factory, USER_NAME, PASSWORD);
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
}
