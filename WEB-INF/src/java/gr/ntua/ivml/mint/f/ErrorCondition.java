package gr.ntua.ivml.mint.f;

import gr.ntua.ivml.mint.api.RequestHandler;

// when this is set, don't continue doing stuff
public class ErrorCondition {
	String err=null;
	public void set( String s ) {
		this.err = s;
	}
	public String get() {
		return err;
	}
	
	public boolean isSet() {
		return err!= null;
	}
	
	/**
	 * Only execute Supplier if the error condition is still clear.
	 * If the supply returns null, set given error message.
	 */
	public <T> T grab( ThrowingSupplier<T> supply, String failureMsg ) throws Exception {
		if( ! isSet() ) {
			T tmp = supply.get();
			if( tmp != null ) 
				return tmp;
			set( failureMsg );
		}
		return null;		
	}
	
	
	public boolean grabBoolean( ThrowingSupplier<Boolean> supply, String failureMsg ) throws Exception {
		if( ! isSet() ) {
			Boolean tmp = supply.get();
			if( tmp != null ) 
				return tmp;
			set( failureMsg );
		}
		return false;				
	}
	
	/**
	 *  if your check returns true, all is well, else set the errorCondition to given message.
	 */
	public ErrorCondition check( Check p, String errMsg ) throws Exception {
		
		if( !isSet()) {
			if(!p.test()) set( errMsg );
		}
		return this;
	}
	
	/**
	 * Execute some side effect code when errorCondition is clear.
	 * @param p
	 * @return
	 */
	public ErrorCondition onValid( Procedure p ) throws Exception {
		if( !isSet() ) p.run();
		return this;	
	}
	
	/**
	 * What to do when the errCondition is in failed state.
	 * @param p
	 */
	public void onFailed( Procedure p ) {
		if( isSet() ) {
			try {
				p.run();
			} catch( Exception e ) {
				RequestHandler.log.error( "", e );
			}
		}
	}
}