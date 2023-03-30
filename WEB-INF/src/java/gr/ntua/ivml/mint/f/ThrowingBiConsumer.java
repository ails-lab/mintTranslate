package gr.ntua.ivml.mint.f;

// functional interface for massaging the publication last second
public interface ThrowingBiConsumer<S,T> {
	public void accept(S s, T t ) throws Exception;
}