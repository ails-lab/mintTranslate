package gr.ntua.ivml.mint.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Stuff we need to work with Jackson
 * @author stabenau
 *
 */
public class Jackson {
	public static ObjectMapper om;
	private static Logger log = LogManager.getLogger();
	
	public static ObjectMapper om() {
		if( om == null ) {
			om = new ObjectMapper();
			om.enable(JsonParser.Feature.ALLOW_COMMENTS);
			// better longer strings (you can have newlines inem)
			om.enable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS);
			// deserialize everything, ignore stuff you dont know about
			om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		}
		return om;
	}
	
	public static String prettyPrint( Object obj ) {
		om.enable(SerializationFeature.INDENT_OUTPUT);
		try {
			return om.writeValueAsString(obj);
		} catch( Exception e ) {
			log.error( "Cant write json string");
			return "\"Error while creating json.\"";
		}
	}
}
