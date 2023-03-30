package gr.ntua.ivml.mint.f;

public interface Predicate<T> {
	boolean test(T t) throws Exception;
}
