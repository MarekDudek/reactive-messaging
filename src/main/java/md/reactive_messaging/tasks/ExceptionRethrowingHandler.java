package md.reactive_messaging.tasks;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ExceptionRethrowingHandler
{
    public static final ExceptionRethrowingHandler EXCEPTION_RETHROWING_HANDLER = new ExceptionRethrowingHandler();

    public void handle(Runnable runnable, String task)
    {
        try
        {
            log.info("Trying task {}", task);
            runnable.run();
            log.info("Success of task {}", task);
        }
        catch (Exception e)
        {
            log.error("Exception in task {}", task);
            throw e;
        }
        log.error("Critical error in task {}", task);
    }
}
