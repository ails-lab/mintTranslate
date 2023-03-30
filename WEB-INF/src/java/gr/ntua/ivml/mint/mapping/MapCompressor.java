package gr.ntua.ivml.mint.mapping;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import gr.ntua.ivml.mint.util.JsonProcessor;

public class MapCompressor extends JsonProcessor {
	
	// here is the compressed mapping
	public List<Entry<String, JsonNode>> mappingsByTarget = new ArrayList<Entry<String, JsonNode>>();

	
	public boolean enterObject( ObjectNode obj ) {
		if( obj.has("name") && obj.has("mapping-cases")) {
			String path = obj.get("name").asText();
			if( obj.has( "prefix" )) {
				String prefix = obj.get( "prefix").asText();
				if( path.startsWith("@"))
					path = "@"+ prefix + ":" + path.substring( 1 );
				else
					path = prefix + ":" + path;
			}
			String xpath = (String) context.get( "xpath" );
			if( xpath == null ) xpath = "/"+path;
			else xpath = xpath + "/" + path;
			// have we seen it
			Set<String> seenPaths = (Set<String>) context.get("seenXpaths" );
			if( seenPaths == null ) {
				seenPaths = new HashSet<String>();
				context.put( "seenXpaths", seenPaths );
			}
			int idx = 1;
			String newPath = xpath;
			while( seenPaths.contains(newPath)) {
				newPath = xpath+"["+idx+"]";
				idx++;
			}
			context.put( "xpath", newPath );
			seenPaths.add( newPath );
		}
		return true;
	}
	
	public void exitObject( ObjectNode obj ) {
		if( obj.has("name") && obj.has( "mapping-cases")) {
			// remove xpath component
			String oldXpath = (String) context.get( "xpath");
			if( StringUtils.isNotEmpty(oldXpath)) {
				String newPath = oldXpath.substring(0, oldXpath.lastIndexOf("/"));
				context.put( "xpath", newPath);
			}
		}
	}
	
	public boolean enterNode( JsonNode node ) {
		// flag if this is value
		if( node.isValueNode()) context.put( "hasValue", "true" );
		if( !jsonPath.isEmpty() && "mapping-cases".equals( jsonPath.peek())) {
			context.remove( "hasValue" );
		}

		return true;
	}
	
	public void exitNode( JsonNode node ) {
		try {
			if( !jsonPath.isEmpty() &&
					"mapping-cases".equals( jsonPath.peek()) &&
					context.containsKey("hasValue")) {
				mappingsByTarget.add( new AbstractMap.SimpleEntry<String, JsonNode>(
						(String) context.get("xpath"), 
						node));
				context.remove( "hasValue" );
			}
		} catch( Exception e ) {
			log.error( "", e);
		}
	}
	
}