package md.reactive_messaging.jms;

import md.reactive_messaging.jms.MessageExtract.MessageExtractBuilder;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.time.Duration;
import java.time.Instant;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.time.Instant.ofEpochMilli;
import static md.reactive_messaging.configs.TibcoConfig.SEQUENTIAL_ID;

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

    @Deprecated
    public static String formatStringBodyWithDeliveryDelay(Message message) throws JMSException
    {
        final String body = message.getBody(String.class);
        final Instant delivery = ofEpochMilli(message.getJMSDeliveryTime());
        final Duration delay = between(delivery, now());
        return format("RECEIVED %s (after %s)", body, delay);
    }

    @Deprecated
    public static int extractTextNumber(String string)
    {
        final Matcher matcher = Pattern.compile("RECEIVED text-(\\d+) \\(after ([^\\)]*)\\)").matcher(string);
        if (matcher.matches())
        {
            final String id = matcher.group(1);
            return Integer.parseInt(id);
        }
        else
        {
            throw new RuntimeException("Text not matched");
        }
    }


    public static void setSequentialId(Message message, long id) throws JMSException
    {
        message.setLongProperty(SEQUENTIAL_ID, id);
    }

    public static MessageExtract extract(Message message)
    {
        MessageExtractBuilder extract = MessageExtract.builder();

        try
        {
            long id = message.getLongProperty(SEQUENTIAL_ID);
            extract.sequentialId(OptionalLong.of(id));
        }
        catch (JMSException e)
        {
            extract.sequentialId(OptionalLong.empty());
        }

        return extract.build();
    }
}
