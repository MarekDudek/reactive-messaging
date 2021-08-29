package md.reactive_messaging.jms;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.time.Duration;
import java.time.Instant;

import static java.lang.String.format;
import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.time.Instant.ofEpochMilli;

public enum MessageConverters
{
    ;

    public static String getText(Message message) throws JMSException
    {
        return ((TextMessage) message).getText();
    }

    public static String getStringBody(Message message) throws JMSException
    {
        return message.getBody(String.class);
    }

    public static String formatStringBodyWithDeliveryDelay(Message message) throws JMSException
    {
        final String body = message.getBody(String.class);
        final Instant delivery = ofEpochMilli(message.getJMSDeliveryTime());
        final Duration delay = between(delivery, now());
        return format("RECEIVED %s (after %s)", body, delay);
    }
}
