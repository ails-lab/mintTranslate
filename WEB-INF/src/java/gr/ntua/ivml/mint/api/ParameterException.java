package gr.ntua.ivml.mint.api;

/*
 * After a lot of internet research and soul searching, Http parameter check can be done reasonably with
 * Exceptions. see https://shipilev.net/blog/2014/exceptional-performance/ and other places.
 */
public class ParameterException extends RuntimeException {
	public ParameterException( String message ) {
		super( message );
	}

	public ParameterException( String message, Throwable cause  ) {
		super( message, cause );
	}

	// not that performance is critical, but this saves some work
	@Override
    public synchronized Throwable fillInStackTrace() {
        // do nothing
        return this;
    }
	
	// convenience thrower
	public static void error( String message ) {
		throw new ParameterException( message );
	}
	

}