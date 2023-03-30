package gr.ntua.ivml.mint.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;

public class Trie<T> {
/**
 * A simple trie to help with finding prefixes, with a reverse index to match from the back ???
 * 
 */
	private static final int MINIMUM_MATCH_LEN = 3;

	
	public static class TrieLookup<V> {
		public TrieLookup( float confidence, V result ) {
			this.result = result;
			this.confidence = confidence;
		}
		
		public V result;
		public float confidence;
	}
	
	public static class TrieNode<S> {
		Map<Character, TrieNode<S>> children;
		int entries;
		S payload;
		
		// return where it ended up
		TrieNode<S> insert( String elem, S payload ) {
			entries++;
			
			if( elem.length() == 0 ) {
				this.payload = payload;
				return this;
			}

			char first = elem.charAt(0);
			String rest = elem.substring(1);
			if( children == null ) children = new HashMap<>();
			TrieNode<S> child = children.computeIfAbsent(first, (ignore) -> new TrieNode<S>());
			return child.insert(rest, payload);
		}
		
		Optional<TrieNode<S>> get( String elem ) {
			if( elem.length() == 0 ) return Optional.of( this );

			char first = elem.charAt(0);
			String rest = elem.substring(1);
			if( children == null ) return Optional.empty();
			TrieNode<S> child = children.get( first );
			if( child == null ) return Optional.empty();
			return child.get( rest );
		}
	}

	TreeMap<String,TrieNode<T>> reverseIndex = new TreeMap<>();
	TrieNode<T> root = new TrieNode<T>( );
	
	public void insertValue( String s, T value ) {
		TrieNode<T> leaf = root.insert( s, value );
		String reverse = reverse( s );
		reverseIndex.put(reverse, leaf);
	}
	
	// how significant is the match in the set 0.0 - 1.0
	public Optional<TrieLookup<T>> lookup( String s ) {
		String reverse = reverse( s );
		Entry<String,TrieNode<T>> afterEntry = reverseIndex.ceilingEntry(reverse);
		Entry<String,TrieNode<T>> beforeEntry = reverseIndex.floorEntry(reverse);
		
		int betterLen = 0;
		// which one is the better match?? length of common prefix
		Entry<String, TrieNode<T>> betterEntry = afterEntry;
		if( betterEntry == null ) {
			betterEntry = beforeEntry;
			if( betterEntry == null ) return Optional.empty();
			betterLen = commonPrefixLength(reverse, beforeEntry.getKey());
		} else {
			betterLen = commonPrefixLength(reverse, afterEntry.getKey());
			if( beforeEntry != null ) {
				int otherLen = commonPrefixLength(reverse, beforeEntry.getKey());
				if( otherLen > betterLen ) {
					betterEntry = beforeEntry;
					betterLen = otherLen;
				}
			}
		}
				
		if( betterLen < MINIMUM_MATCH_LEN ) return Optional.empty();

		// how significant is the non-matching prefix ..
		// if it contains all the nodes, its a prefix thats added to all the stuff
		// if not, its probably not a match
		String closestMatchingKey = reverse( betterEntry.getKey());
		Optional<TrieNode<T>> parentOpt = root.get( closestMatchingKey.substring( 0, closestMatchingKey.length()-betterLen));
		
		return Optional.of(
				new TrieLookup<T>( 
						((float)parentOpt.get().entries)/root.entries, 
						betterEntry.getValue().payload));
	}
	
	static int commonPrefixLength( String s1, String s2 ) {
		int i=0;
		while( i< s1.length() && i<s2.length()) {
			if( s1.charAt(i) != s2.charAt(i)) break;
			i++;
		}
		return i;
	}
	
	static String reverse(String s ) {
		char[] content = s.toCharArray();
		for( int i=0; i<(content.length /2); i++ ) {
			char swap=content[i];
			content[i]=content[ content.length-i-1 ];
			content[ content.length-i-1 ] = swap;
		}
		return new String( content );
	}
	
}
