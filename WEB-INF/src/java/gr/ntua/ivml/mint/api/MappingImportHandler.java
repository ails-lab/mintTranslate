package gr.ntua.ivml.mint.api;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;

import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.db.GlobalPrefixStore;
import gr.ntua.ivml.mint.mapping.MapCompressor;
import gr.ntua.ivml.mint.mapping.MapInjectNewNamespaces;
import gr.ntua.ivml.mint.persistent.Organization;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.xml.util.XPathUtils;

public class MappingImportHandler implements Runnable {
	
	// organizationId ... for which org to upload
	// project .. which project tag this has
	// schemaId .. the target schema for this mapping .. this is slightly dangerous,
	// can be wrong but difficult to check
	// namespaces .. all namespace prefixes of the original 
	// mapping ... the payload of the original mapping
	
	public ObjectNode meta;
	public Long userId;
	
	public Map<String, String> prefixTranslator;
	
	Logger log = Logger.getLogger( MappingImportHandler.class );

	public MappingImportHandler( ObjectNode meta, Long userId ) {
		this.meta = meta;
		this.userId = userId;
	}

	@Override
	public void run() {
		DB.getSession().beginTransaction();

		try {
			runInTransaction();
		} catch( Exception e ) {
			log.error( "", e );
		} finally {
			DB.closeSession();
			DB.closeStatelessSession();
		}
	}
	
	public void runInTransaction() throws Exception {
		User u = DB.getUserDAO().getById(userId, false );
		Organization org = u.getOrganization();
		if( meta.has("organizationId")) {
			org = DB.getOrganizationDAO().getById(meta.get( "organizationId").asLong(), false);
		}
		
		if( !u.can( "change data", org )) { 
			log.error("User #" + u.getDbID() + " attempted illegal data upload.");
			throw new Exception("User cannot upload");
		}

		// build namespace adjust structure
		Map<String,String> oldPrefixes = getPrefixes( (ObjectNode) meta.get( "namespaces" ));

		// upload them into current mint
		for( Entry<String,String> e: oldPrefixes.entrySet()) 
			GlobalPrefixStore.createPrefix(e.getValue(), e.getKey());

		Map<String,String> newPrefixes = GlobalPrefixStore.allPrefixMap();
		
		prefixTranslator = newPrefixes( oldPrefixes, newPrefixes );
		if( prefixTranslator.size() == 0 ) {
			log.debug( "There is nothing to fix with the prefixes ");
		} else {
			log.debug( "Prefix changes \n " + 
					prefixTranslator.entrySet()
					.stream()
					.map(e -> e.getKey() + "->" + e.getValue() )
					.collect(Collectors.joining("\n")));
			// MapCompressor mp = new MapCompressor();
			MapInjectNewNamespaces injector = new MapInjectNewNamespaces();
			injector.oldToNewPrefixes = prefixTranslator;
			
			injector.recurse( meta );

			fixNamespaceTable();
			// now create a mapping and store it :-(
			
		}
		log.debug( meta.toString());
	}
	
	/**
	 * Find which URLs have new prefixes and return a map old to new prefix. Throw if some prefix is not 
	 * in the new list (it should be in there)
	 * @param oldPreToUrl
	 * @param newPreToUrl
	 * @return The map is empty if nothing needs changing
	 */
	public Map<String, String> newPrefixes( Map<String,String> oldPreToUrl, Map<String, String> newPreToUrl ) throws Exception {
		HashMap<String, String> result = new HashMap<String,String>();
		HashMap<String, String> reverseNew = new HashMap<String, String>();
		newPreToUrl.forEach((a,b) -> {
			reverseNew.put( b, a);
		});
		for( Entry<String,String> e: oldPreToUrl.entrySet() ) {
			String oldPre = e.getKey();
			String url = e.getValue();
			String newPre = reverseNew.get( url );
			if( newPre == null ) {
				// this shouldnt happen, but hey
				throw new Exception( "Unknown namespace " + url ); 
			}
			if( ! oldPre.equals( newPre )) result.put(oldPre, newPre );
		}
		return result;
	}
	
		
	private Map<String,String> getPrefixes( ObjectNode prefixes ) {
		if( prefixes == null ) return null;
		
		Map<String, String> result = new ObjectMapper().convertValue(
				 prefixes, 
				 TypeFactory.defaultInstance().constructMapType( HashMap.class, String.class, String.class));
		return result;
	}
	
	// not in the recursive part
	// Fix the namespace table in the mapping (inside mappingJson, not the global namespace table outside)
	public void fixNamespaceTable(  ) {
		ObjectNode mapping = (ObjectNode) meta.get( "mappingJSON" );
		if( mapping == null || mapping.isMissingNode()) {
			log.info( "Not a JSON mapping");
			return;
		}
		ObjectNode internalNamespaces = (ObjectNode) mapping.get( "namespaces");
		if( internalNamespaces == null || internalNamespaces.isMissingNode()) {
			log.error( "Malformed Json mapping");
			return;
		}
		
		ObjectNode newNamespaces = JsonNodeFactory.instance.objectNode();
		Iterator<Entry<String,JsonNode>> it = internalNamespaces.fields();
		Entry<String,JsonNode> e;
		while( it.hasNext()) {
			e = it.next();
			String prefix = e.getKey();
			String newPrefix = prefix;
			if( prefixTranslator.containsKey(prefix)) {
				newPrefix = prefixTranslator.get( prefix );
				log.debug( "Fixed namespace prefix " + prefix + "->" + newPrefix );
			}
			newNamespaces.set( newPrefix, e.getValue());
		}
		mapping.set( "namespaces", newNamespaces );
	}


}
