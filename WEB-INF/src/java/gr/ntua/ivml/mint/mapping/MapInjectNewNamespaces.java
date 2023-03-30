package gr.ntua.ivml.mint.mapping;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import gr.ntua.ivml.mint.util.JsonProcessor;
import gr.ntua.ivml.mint.xml.util.XPathUtils;

public class MapInjectNewNamespaces extends JsonProcessor {
	public static final Logger log = Logger.getLogger(MapInjectNewNamespaces.class);
	
	public Map<String, String> oldToNewPrefixes;
	
	public void setNewPrefixes( Map<String, String> oldToNewPrefixes ) {
		this.oldToNewPrefixes = oldToNewPrefixes;
	}

	// change prefixes everywhere in the target path
	public boolean enterObject( ObjectNode obj ) {
		if( obj.has("name") && obj.has("mapping-cases") &&
				obj.has( "prefix" )) {
			String oldPrefix = obj.get("prefix").asText();
			if( oldPrefix != null ) {
				String newPrefix = oldToNewPrefixes.get( oldPrefix );
				if( newPrefix != null ) {
					obj.put( "prefix", newPrefix );
					log.debug( "Injected " + oldPrefix + "->" + newPrefix + " into " + getJsonPath());
				}
			}
		}
		return true;
	}
	
	// fix paths in xpath mappings and in conditions
	public boolean enterNode( JsonNode node ) {
		
		ObjectNode obj = null;
		if( node.isObject()) obj = (ObjectNode) node;
		
		// xpath mappings
		if( !jsonPath.isEmpty() && "mappings".equals( jsonPath.peek())) {
			if( obj != null ) {
				if( "xpath".equals( obj.get( "type").asText())) {
					String oldPath = obj.get( "value").asText();
					if(! StringUtils.isEmpty(oldPath)) {
						String newPath = XPathUtils.changePrefixes(oldPath, oldToNewPrefixes);
						obj.put( "value", newPath );
						log.debug( "Fixing mapping " + oldPath + "->" + newPath + " in " + getJsonPath());
					}
				}
			}
		}
		
		// conditions
		if( !jsonPath.isEmpty() && jsonPath.contains( "condition")) {
			if( obj != null) {
				if( obj.has( "xpath")) {
					String xpath = obj.get( "xpath").asText();
					if( !StringUtils.isEmpty(xpath)) {
						String newPath = XPathUtils.changePrefixes(xpath, oldToNewPrefixes);
						obj.put( "xpath", newPath);
						log.debug( "Fixing condition " + xpath + "->" + newPath + " in " + getJsonPath());
					}
				}
			}
		}
		return true;
	}
	
}