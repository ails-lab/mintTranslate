package gr.ntua.ivml.mint.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonProcessor {
	
	public static final Logger log = Logger.getLogger( JsonProcessor.class );
	
	// keep context info around
	// eg jsonPath
	public Map<String, Object> context  = new HashMap<String, Object>();
	public Stack<String> jsonPath = new Stack<String>();
	
	// abort on false;
	public boolean enterObject( ObjectNode obj ) {
		return true;
	}
	
	// called after key is processed
	public void exitObject( ObjectNode obj ) {
		
	}
	
	public boolean enterArray( ArrayNode arr ) {
		return true;
	}
	
	public void exitArray( ArrayNode arr ) {
		
	}
	
	public boolean enterNode( JsonNode node ) {
		return true;
	}
	
	public void exitNode( JsonNode node ) {
		
	}
	
	
	public void recurse( JsonNode node  ) {
		try {
			enterNode( node );
		if( node.isObject()) 
			recurseObject( (ObjectNode) node );
		else if( node.isArray())
			recurseArray( (ArrayNode) node );
		else {
			// value ? should be dealt with in checkJson
		}
		} finally {
			exitNode( node );
		}
	}
	
	public void recurseObject( ObjectNode obj ) {
		try {
			if( enterObject( obj )) {
				Iterator<Entry<String,JsonNode>> it = obj.fields();
				while( it.hasNext() ) {
					Entry<String, JsonNode> e = it.next();
					try {
						jsonPath.push(e.getKey());
						JsonNode val = e.getValue();
						recurse( val);
					} finally {
						jsonPath.pop();
					}			
				}
			}
		} finally {
			exitObject( obj );
		}
	}
	
	public void recurseArray(ArrayNode arr) {
		try {
			if( enterArray( arr )) {

				Iterator<JsonNode> it = arr.elements();
				int counter = 0;
				while( it.hasNext()) {
					JsonNode val = it.next();
					try {
						jsonPath.push( "[" + counter + "]");
							recurse( val );
						counter++;
					} finally {
						jsonPath.pop();
					}
				}
			}
		} finally {
			exitArray( arr );
		}
	}
	
	public Object getContext( String key ) {
		return context.get( key );
	}
	
	public String getJsonPath( ) {
		return jsonPath.stream().collect( Collectors.joining("/"));
	}
}
