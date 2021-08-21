package md.reactive_messaging.jms.utils;

import lombok.extern.slf4j.Slf4j;

import javax.jms.Message;
import javax.jms.MessageListener;

@Slf4j
public enum LogMessageListener implements MessageListener
{
    LogMessageListener;

    @Override
    public void onMessage(Message message)
    {
        log.info("Message received {}", message);
    }
}
