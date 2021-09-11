package md.reactive_messaging.apps;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import md.reactive_messaging.functional.throwing.ThrowingFunction;
import md.reactive_messaging.reactive.ReactiveOps;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static md.reactive_messaging.reactive.GenericSubscribers.FluxSubscribers.subscribeWithAndLatch;

@Builder
@Slf4j
public final class JmsSyncReceiver<T> implements Runnable
{
    @NonNull
    private final ReactiveOps ops;

    @NonNull
    private final ThrowingFunction<String, ConnectionFactory, JMSException> connectionFactory;
    @NonNull
    private final String url;

    @NonNull
    private final String userName;
    @NonNull
    private final String password;

    @NonNull
    private final String queueName;
    @NonNull
    private final ThrowingFunction<Message, T, JMSException> converter;

    private final long maxAttempts;
    @NonNull
    private final Duration minBackoff;
    @NonNull
    private final Duration maxBackoff;

    @Override
    public void run()
    {
        try
        {
            log.info("Start");

            final Flux<T> messages =
                    ops.messages(
                            connectionFactory, url,
                            userName, password,
                            queueName, converter,
                            maxAttempts, minBackoff, maxBackoff
                    );

            Subscriber<T> subscriber =
                    new BaseSubscriber<T>()
                    {
                        private long last;

                        @Override
                        protected void hookOnSubscribe(Subscription subscription)
                        {
                            super.hookOnSubscribe(subscription);
                            last = 0;
                        }

                        @Override
                        protected void hookOnNext(T value)
                        {
                            super.hookOnNext(value);

                        }
                    };

            CountDownLatch latch = subscribeWithAndLatch(messages, subscriber);
            latch.await();
        }
        catch (InterruptedException e)
        {
            log.warn("Requested to interrupt");
        }
        log.info("Finish");
    }
}
