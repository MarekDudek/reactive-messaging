package md.reactive_messaging;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.time.Duration.ofSeconds;

@Slf4j
public final class SimpleDebug
{
    public static void main(String[] args) throws InterruptedException
    {
        Thread thread =
                new Thread(() -> {
                    while (true)
                    {
                        try
                        {
                            log.info("sleeping");
                            sleep(ofSeconds(1).toMillis());
                        }
                        catch (InterruptedException e)
                        {
                            log.warn("Requested to interrupt");
                            currentThread().interrupt();
                            break;
                        }
                    }
                }, "loop");
        thread.start();
        waitForCtrlC(thread);
    }

    static void waitForCtrlC(Thread thread) throws InterruptedException
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
        finally
        {
            thread.interrupt();
            thread.join();
        }
    }
}
