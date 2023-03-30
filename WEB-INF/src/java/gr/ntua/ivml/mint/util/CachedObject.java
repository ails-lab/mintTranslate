package gr.ntua.ivml.mint.util;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Idiots cache for expensive to create Object. You need a method that makes the object and a timer
 * and expiry time. get() will first call the supplier and then cache the object for some seconds.
 * Every get() will extend the lifetime of the object.
 * 
 * @author arne
 *
 * @param <T>
 */
public class CachedObject<T> {
	private T cache;
	private Supplier<T> makeObject;
	private int expireInSeconds;
	private Timer t;
	private TimerTask expireTask;
	
	public synchronized T get() {
		if( cache != null ) {
			if( expireTask != null ) expireTask.cancel();
		}
		else {
			cache = makeObject.get();
		}
		expireTask = new TimerTask() { 
			public void run() {
				expire();
			}
		};
		t.schedule( expireTask, TimeUnit.SECONDS.toMillis( expireInSeconds ));
		return cache;
	}
	
	public CachedObject( Timer t, Supplier<T> supplier, int expireInSeconds ) {
		this.expireInSeconds = expireInSeconds;
		this.makeObject = supplier;
		this.t = t;
	}
	
	private synchronized void expire() {
		 cache = null; 
		 expireTask = null;
	}
}
