package md.reactive_messaging.jms.utils;

import lombok.extern.slf4j.Slf4j;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public enum FailExceptionListener implements ExceptionListener
{
    FailExceptionListener;

    @Override
    public void onException(JMSException exception)
    {
        log.error("Error ", exception);
        fail();
    }
}
