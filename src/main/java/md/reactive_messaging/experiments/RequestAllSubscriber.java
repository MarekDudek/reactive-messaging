package md.reactive_messaging.experiments;


import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@Slf4j
public final class RequestAllSubscriber<T> implements Subscriber<T>
{
    @Override
    public void onSubscribe(Subscription subscription)
    {
        log.trace("Subscription '{}'", subscription);
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(T next)
    {
        log.trace("Next is '{}'", next);
    }

    @Override
    public void onError(Throwable throwable)
    {
        log.error("Ended with error '{}'", throwable.getMessage());
    }

    @Override
    public void onComplete()
    {
        log.trace("Ended successfully");
    }
}
