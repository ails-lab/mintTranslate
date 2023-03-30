package gr.ntua.ivml.mint.f;

public interface ThrowingFunction<A,B> {
	public B apply( A a ) throws Exception;
}
