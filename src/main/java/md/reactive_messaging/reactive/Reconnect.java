package md.reactive_messaging.reactive;

public enum Reconnect
{
    @Deprecated
    RECONNECT,
    CREATING_ASYNC_LISTENER_FAILED,
    CREATING_CONTEXT_FAILED,
    EXCEPTION_IN_CONTEXT_HEARD,
    ERROR_RECEIVING_MESSAGE
}
