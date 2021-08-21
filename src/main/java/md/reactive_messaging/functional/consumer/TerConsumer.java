package md.reactive_messaging.functional.consumer;

@FunctionalInterface
public interface TerConsumer<A, B, C, D>
{
    void accept(A a, B b, C c, D d);
}
