package gr.ntua.ivml.mint.f;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Poor mans transducers. Make a pipeline of interceptors that can change the number of items.
 * Like Stream, but supplier side driven. End is, when you stop pushing stuff.
 * Exceptions stop the process.
 * 
 * I guess not very useful, but I needed the exercise.
 * @author stabenau
 *
 * @param <A>
 * @param <B>
 */

public interface Interceptor<A,B> {
	public ThrowingConsumer<A> intercept( ThrowingConsumer<B> nestedConsumer );
	
	// does nothing, not sure is useful, pass if there is one expected I guess
	public static <A> Interceptor<A,A> emptyInterceptor() {
		return (consumer) -> consumer;
	};
	
	/**
	 * Combine into next one.
	 * I1.into( I2 ).into( I3 ) ... make one Interceptor.
	 * @param <C>
	 * @param next
	 * @return
	 */
	public default <C> Interceptor<A,C> into( Interceptor<B,C> next ) {
		return (innerConsumer)->intercept( next.intercept( innerConsumer ));
	}
	
	/**
	 * Make a D from C, push on.
	 * @param <C>
	 * @param <D>
	 * @param modifier
	 * @return
	 */
	public static <C,D> Interceptor<C,D> mapInterceptor( ThrowingFunction<C,D> modifier ) {
		return (consumer)->{
			return (val) -> consumer.accept( modifier.apply(val));
		};
	}
	
	/**
	 * Want to change number of items? Push as many or no D into target as C demands.
	 * @param <C>
	 * @param <D>
	 * @param modifier
	 * @return
	 */
	public static <C,D> Interceptor<C,D> mapcatInterceptor( ThrowingBiConsumer<C,ThrowingConsumer<D>> modifier ) {
		return (consumer)->{
			return (val) -> modifier.accept( val, consumer);
		};
	}

	/**
	 * Push only if Test is true for C.
	 * @param <C>
	 * @param test
	 * @return
	 */
	public static <C> Interceptor<C,C> filterInterceptor( Predicate<C> test ) {
		return mapcatInterceptor( (item, next) -> { if( test.test(item)) next.accept( item );});
	}

	
	/**
	 * Need to log something or mess with C, we are not "read only" here :-)
	 * @param <C>
	 * @param sideEffect
	 * @return
	 */
	public static <C> Interceptor<C,C> modifyInterceptor( ThrowingConsumer<C> sideEffect ) {		
		return mapInterceptor( (val)-> { sideEffect.accept(val); return val;}); 
	}


	// easier chaining
	public default <C> Interceptor<A,C> map( ThrowingFunction<B,C> modifier ) {
		return into( mapInterceptor( modifier ));
	}
	
	public default Interceptor<A,B> filter( Predicate<B> test ) {
		return into( filterInterceptor( test ));
	}
	
	public default Interceptor<A,B> modifiy( ThrowingConsumer<B> sideEffect ) {
		return into( modifyInterceptor( sideEffect ));
	}
	
	public default <C> Interceptor<A,C> mapcat( ThrowingBiConsumer<B, ThrowingConsumer<C>> modifier ) {
		return into( mapcatInterceptor( modifier ));
	}
	
	
	// Some convenience methods...
	
	// convenience for testing I guess,
	// end is null
	public default List<B> intoList(ThrowingSupplier<A> supplier ) throws Exception {
		final ArrayList<B> res = new ArrayList<>();
		ThrowingConsumer<B> collector = (val)->res.add(val);
		runUntilNull( supplier, collector );
		return res;
	}
	
	public default void runUntilNull( ThrowingSupplier<A> source , ThrowingConsumer<B> sink ) throws Exception {
		ThrowingConsumer<A> sourceConsumer = intercept(sink);
		A elem;
		while((elem = source.get()) != null) {
			sourceConsumer.accept(elem);
		}
	}
	
	public default void iterate( Iterator<A> source , ThrowingConsumer<B> sink ) throws Exception {
		ThrowingConsumer<A> sourceConsumer = intercept(sink);
		while( source.hasNext())
			sourceConsumer.accept(source.next());
	}
	
	public default void iterate( Iterable<A> source , ThrowingConsumer<B> sink ) throws Exception {
		iterate( source.iterator(), sink);
	}
	
	public default List<B> toList( Iterable<A> source  ) throws Exception {
		final ArrayList<B> res = new ArrayList<>();
		ThrowingConsumer<B> collector = (val)->res.add(val);
		iterate( source.iterator(), collector );
		return res;
	}	
	
	
	// use this interceptor to convert a demand into a different demand, buffer the intermediate stuff
	public default ThrowingSupplier<B> demandDriven( ThrowingSupplier<A> source, int bufferSize ) {
		final ArrayBlockingQueue<B> buffer = new ArrayBlockingQueue<B>(bufferSize);
		final ThrowingConsumer<B> bufferConsumer = elem -> {
			buffer.add( elem );
		};
		final ThrowingConsumer<A> sourceConsumer = intercept( bufferConsumer );
		return ()-> {
			while( buffer.isEmpty()) {
				A elem = source.get();
				// we stop at null feed
				if( elem == null ) break;
				sourceConsumer.accept(elem);
			}
			return buffer.poll();
		};
	}
	
}

