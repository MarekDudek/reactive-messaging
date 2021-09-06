package md.reactive_messaging.jms;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.FieldDefaults;

import java.time.Duration;
import java.util.OptionalLong;

import static lombok.AccessLevel.PUBLIC;

@Value
@Builder
@FieldDefaults(level = PUBLIC)
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class MessageExtract
{
    @NonNull
    OptionalLong sequentialId;
    @NonNull
    Duration delay;
}
