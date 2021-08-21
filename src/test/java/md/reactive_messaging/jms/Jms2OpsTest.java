package md.reactive_messaging.jms;

import com.tibco.tibjms.TibjmsConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.Either;
import org.junit.jupiter.api.Test;

import javax.jms.*;
import java.util.Optional;

import static md.reactive_messaging.jms.TestTibcoEmsConfig.*;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
final class Jms2OpsTest
{
    private static final Jms2Ops OPS = new Jms2Ops();

    @Test
    void synchronous_send_text_message()
    {
        final ConnectionFactory factory = new TibjmsConnectionFactory(URL);
        final Either<JMSRuntimeException, JMSContext> context = OPS.createContext(factory, USER_NAME, PASSWORD);
        assertThat(context.isRight()).isTrue();
        context.consume(null, c -> {
            final Optional<JMSRuntimeException> settingError = OPS.setExceptionListener(c, error -> log.error("Exception heard in context", error));
            settingError.ifPresent(exception -> log.error("Error while setting exception listener"));
        });
        final Either<JMSRuntimeException, Queue> queue = context.flatMap(c -> OPS.createQueue(c, QUEUE_NAME));
        final Either<JMSRuntimeException, JMSProducer> producer = context.flatMap(OPS::createProducer);
        producer.flatMap(p -> queue.map(q -> OPS.sendTextMessage(p, q, "synchronous text message")));

        final Optional<JMSRuntimeException> error = OPS.closeContext(context.rightOr(null));
        assertThat(error).isEmpty();
    }
}
