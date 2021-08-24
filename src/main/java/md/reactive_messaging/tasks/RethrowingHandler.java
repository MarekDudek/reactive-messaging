package md.reactive_messaging.tasks;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class RethrowingHandler
{
    public static final RethrowingHandler RETHROWING_HANDLER = new RethrowingHandler();

    public void handle(Runnable runnable, String task)
    {
        try
        {
            log.info("Trying task {}", task);
            runnable.run();
            log.info("Success of task {}", task);
        }
        catch (Throwable e)
        {
            log.error("Exception in task {}", task);
            throw e;
        }
    }
}
