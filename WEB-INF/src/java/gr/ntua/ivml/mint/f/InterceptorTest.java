package gr.ntua.ivml.mint.f;

import java.util.Arrays;

public class InterceptorTest {
	public static void main(String[] args ) throws Exception {
		Interceptor<Integer, String> myInter = Interceptor.<Integer>emptyInterceptor()
				.map( (Integer i) ->i+1 )
				.filter(  i ->(i%2)==0 )
				.mapcat( ( val,  sink ) -> {
						for( int j=0; j<=val; j++ ) {
							sink.accept( j );
						}
					})
				.map( val ->  "Num: " + String.valueOf(val));
		System.out.println( myInter.toList( Arrays.asList( 0,1,2,3,4,5 )));
	}
	
}
