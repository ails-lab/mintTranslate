package gr.ntua.ivml.mint.util;

import java.util.HashMap;

public class SetCounter {
	private HashMap<String, Integer> counters = new HashMap<>();
	
	public synchronized void add( String val ) {
		int count = counters.computeIfAbsent(val, k->0);
		counters.put( val, count+1);
	}
	
	public HashMap<String,Integer> getResult() {
		return (HashMap<String, Integer>)counters.clone();
	}
	
	public int get( String val ) {
		return counters.getOrDefault( val, 0 );
	}
	
	public synchronized void reset( String val ) {
		counters.remove( val );
	}
	
	public synchronized void clear() {
		counters.clear();
	}
	
	public synchronized void add( String val, int increment ) {
		int count = counters.computeIfAbsent(val, k->0);
		counters.put( val, count+increment);		
	}
	
	// return this(val) - other(val)
	// if val not key, assume 0
	public int diff( SetCounter other, String val ) {
		int otherInt = other.counters.getOrDefault(val, 0);
		int thisInt = this.counters.getOrDefault(val, 0);
		return thisInt - otherInt;
	}
}
