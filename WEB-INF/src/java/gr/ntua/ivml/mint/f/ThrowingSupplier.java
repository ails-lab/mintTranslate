package gr.ntua.ivml.mint.f;

public interface ThrowingSupplier<T>  {
	public T get() throws Exception; 
}