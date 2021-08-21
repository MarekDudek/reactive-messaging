package md.reactive_messaging.jms;

import lombok.NonNull;
import lombok.Value;

import javax.jms.CompletionListener;
import javax.jms.Message;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Value
public class CompletionListenerImpl implements CompletionListener
{
    @NonNull
    Consumer<Message> onCompletion;
    @NonNull
    BiConsumer<Message, Exception> onException;

    @Override
    public void onCompletion(Message message)
    {
        onCompletion.accept(message);
    }

    @Override
    public void onException(Message message, Exception exception)
    {
        onException.accept(message, exception);
    }
}
