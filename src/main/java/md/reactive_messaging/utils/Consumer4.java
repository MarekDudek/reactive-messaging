package md.reactive_messaging.utils;

@FunctionalInterface
public interface Consumer4<A, B, C, D>
{
    void accept(A a, B b, C c, D d);
}
