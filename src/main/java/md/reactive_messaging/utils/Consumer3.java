package md.reactive_messaging.utils;

@FunctionalInterface
public interface Consumer3<A, B, C>
{
    void accept(A a, B b, C c);
}
