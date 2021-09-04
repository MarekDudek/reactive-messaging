package md.reactive_messaging.jms;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.time.Duration;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public static int extractTextNumber(String string) {
        final Matcher matcher = Pattern.compile("RECEIVED text-(\\d+) \\(after ([^\\)]*)\\)").matcher(string);
        if (matcher.matches()) {
            final String id = matcher.group(1);
            return Integer.parseInt(id);
        } else {
            throw new RuntimeException("Text not matched");
        }
    }
}
