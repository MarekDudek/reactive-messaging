package md.reactive_messaging.reactive;

public enum Reconnect
{
    @Deprecated
    RECONNECT,
    CREATING_SYNC_RECEIVER_FAILED,
    CREATING_CONTEXT_FAILED,
    EXCEPTION_IN_CONTEXT_HEARD,
    EXCEPTION_IN_CONNECTION_HEARD,
    ERROR_RECEIVING_MESSAGE
}
