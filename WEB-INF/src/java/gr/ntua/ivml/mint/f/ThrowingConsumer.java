package gr.ntua.ivml.mint.f;

// functional interface for massaging the publication last second
public interface ThrowingConsumer<T> {
	public void accept(T t ) throws Exception;
}