package md.reactive_messaging.functional.consumer;

@FunctionalInterface
public interface TriConsumer<A, B, C>
{
    void accept(A a, B b, C c);
}
