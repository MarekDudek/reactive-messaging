package md.reactive_messaging;

import java.time.Duration;

public enum TestTibcoEmsConfig
{
    ;

    public static final String URL = "tcp://localhost:7222";
    public static final String USER_NAME = "some-user";
    public static final String PASSWORD = "some-password";
    public static final String QUEUE_NAME = "some-queue";

    public static final long MAX_ATTEMPTS = Long.MAX_VALUE;
    public static final Duration MIN_BACKOFF = Duration.ofMillis(100);
    public static final Duration MAX_BACKOFF = Duration.ofSeconds(3);
}