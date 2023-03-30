package gr.ntua.ivml.mint.annotation;

/*
 * Helper class to serialize / deserialize the Annotation Json LD that come from Sage and is used as exchange format
 * in the NTUA tool zoo (Sage/Mint/CrowdHeritage)
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class Annotation {
	
	public static class Literal {
		public String type ="Literal";
		public String value, language;  
		public Literal() {}
		public Literal( String value, String language ) {
			this.value = value;
			this.language = language;
		}
	}
	
	public static class TextPosition {
		public String type = "TextPositionSelector";
		public Integer start, end;
	}
	
	public static class RefinedBy {
		public String type="TextPositionSelector";
		public int start, end;
	}
	
	public static class Selector {		
		public String type;
		public String property, rdfPath; // one of those
		public Literal destination;
		public RefinedBy refinedBy;
	}
		
	public static class Target {
		public String source;
		public Selector selector;
	}
	
	
	// single uri serialize as TextNode
	// multiple as ArrayNode of TextNode
	// literal as ObjectNode with type, value, language
	// and vice versa
	
	public static class Body {
		
		public List<String> uris;
		public String value, language;

		public Body() {
			
		}
		
		// convenience constructors , deserialize
		public Body( ArrayNode manyUris ) {
			uris = new ArrayList<>();
			for( JsonNode uri:manyUris ) {
				if( uri instanceof TextNode ) {
					uris.add( uri.textValue());
				}
			}
		}

		public Body( TextNode text ) {
			uris = Arrays.asList(text.textValue());
		}
		
		public Body( ObjectNode obj ) {
			value = obj.get( "value").textValue();
			language = obj.get( "language").textValue();
		}
		
		// serialize into tree
		public JsonNode toJson() {
			if( value != null ) {
				ObjectNode res = new ObjectMapper().createObjectNode();
				res.put( "value", value );
				if( language != null ) 
					res.put( "language", language);
				res.put( "type", "TextualBody");
				return res;
			}
			
			if( uris != null ) {
				if( uris.size() == 1 ) {
					return new TextNode( uris.get(0));
				} else {
					ArrayNode res = new ObjectMapper().createArrayNode();
					for( String uri: uris)
						res.add( new TextNode( uri ));
					return res;
				}
			}
			return null;
		}
		
	}
	
	public static class BodyDeserializer extends StdDeserializer<Body> { 

	    public BodyDeserializer() { 
	        this(null); 
	    } 

	    public BodyDeserializer(Class<?> vc) { 
	        super(vc); 
	    }

	    @Override
	    public Body deserialize(JsonParser jp, DeserializationContext ctxt) 
	      throws IOException, JsonProcessingException {
	        JsonNode node = jp.getCodec().readTree(jp);
	        if( node instanceof ArrayNode ) return new Body( (ArrayNode) node );
	        if( node instanceof TextNode ) return new Body( (TextNode) node );
	        if( node instanceof ObjectNode ) return new Body( (ObjectNode) node );
	        return null;
	    }
	}
	
	public static class BodySerializer extends StdSerializer<Body> {
	   public BodySerializer() { 
	      this(null); 
	   } 
	   public BodySerializer(Class<Body> t) { 
	      super(t); 
	   } 
	   @Override 
	   public void serialize(Body value, 
	      JsonGenerator generator, SerializerProvider serProv) throws IOException { 
	      generator.writeObject( value.toJson()); 
	   } 
	}
	
	public static class Creator {
		public String id, name, type;
	}

	public static class Review {
		public String type, recommendation;
	}

	/*
	 * High level description of an Annotation in json.
	 * There is an array if those in the "@graph" section of an AnnotationSet
	 */
	
	
	public String id;
	public String type="Annotation";
	public String created;
	public String confidence;
	public String scope;
	
	
	public Creator creator;
	public Target target;

	public Review review;
	
	@JsonDeserialize(using = BodyDeserializer.class) 
	@JsonSerialize( using = BodySerializer.class )
	public Body body;
	
	
	
	public static void main( String[] args ) {
		ObjectMapper om = new ObjectMapper();
		om.enable(SerializationFeature.INDENT_OUTPUT);

		try {
			JsonNode node = om.readTree( Annotation.class.getResourceAsStream("annotationExample.json"));
			System.out.println( om.writeValueAsString( node.get("@graph")));

			for( JsonNode n: (ArrayNode) (node.get("@graph"))) {
				Annotation a = (Annotation) om.treeToValue(n, Annotation.class );
				JsonNode node2 = om.valueToTree(a);
				System.out.println( "Roundtrip:\n" + om.writeValueAsString( node2 ));
				
			}
			// om.writerWithDefaultPrettyPrinter().writeValue(System.out, node.get("@graph").get(0));
		} catch( Exception e ) {
			e.printStackTrace();
		}
	}
}
