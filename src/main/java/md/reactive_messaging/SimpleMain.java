package md.reactive_messaging;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.CountDownLatch;

@Slf4j
public final class SimpleMain
{
    public static void main(String[] args) throws InterruptedException
    {
        new SimpleMain().run();
    }

    public void run()
    {
        final Flux<Integer> numbers =
                Flux.generate(
                        () -> 1,
                        (i, sink) -> {
                            sink.next(i);
                            return i + 1;
                        },
                        i -> {
                            log.info("Finished with {}", i);
                        }
                );
        final Flux<Integer> dropped = numbers.onBackpressureDrop(item -> log.error("Publisher dropped {}", item));
        final Flux<Integer> published = dropped.publishOn(Schedulers.single());
        final Flux<Integer> logged = published.log();

        logged.subscribe(
                item -> log.info("Subscriber got {}", item),
                error -> log.error("Subscriber error", error),
                () -> log.info("Subscriber finished")
        );
        //waitForCtrlC();
        log.info("Done");
    }

    private static void waitForCtrlC()
    {
        final CountDownLatch done = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(
                new Thread(
                        () -> {
                            log.info("Shutting down");
                            done.countDown();
                        }, "shutdown-hook-ctrl-c"
                )
        );
        try
        {
            log.info("Awaiting");
            done.await();
        }
        catch (InterruptedException e)
        {
            log.info("Interrupted");
        }
    }
}
