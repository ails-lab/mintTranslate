package gr.ntua.ivml.mint.projects.with.model;

import java.io.File;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.XMLReader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Cookies;
import com.jayway.restassured.response.Response;

import gr.ntua.ivml.mint.xml.util.XmlValueUtils;
import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.Node;

public class With {

		
	public static final Logger log = LogManager.getLogger();

	public ArrayNode provenance;
	public ArrayNode media;
	public DescriptiveData descriptiveData;

	public With(Document doc ) {
		this.descriptiveData = new DescriptiveData(doc);
		this.provenance = createProvenance(doc);
		this.media =createMedia(doc);
	}
	
	public static class WithConnection {
		public String baseUrl;
		public Cookies cookies = null;
		
		public WithConnection( String baseUrl ) {
			this.baseUrl = baseUrl;
		}
		
		public boolean login( String username, String password) {
		    this.cookies = 
		        RestAssured.given()
		        	.baseUri( baseUrl )
		            .contentType(ContentType.JSON)
		            .body(String.format( "{\"email\":\"%s\",\"password\":\"%s\"}", username, password )) 
		            .post("/user/login")
		            .then()
		              .statusCode( 200 )
		              .extract()
		              .detailedCookies();
		     // not sure how to do failed login
		    return ( cookies != null);
		    
		}
		
		public boolean loginCheck( ) {
			if( cookies == null ) return false;
			
	        String response = RestAssured.given()
	        	.baseUri( baseUrl )
	            .contentType(ContentType.JSON)
	            .cookies( cookies )
	            .get("/user/me")
	            .then()
	              .statusCode( 200 )
	            .extract()
	            .asString();

	        log.info( response );
	        return true;
		}
		
		public boolean addRecord( String jsonObject, String collectionId ) {
            
			Response response = RestAssured.given()
        	.baseUri( baseUrl )
            .contentType(ContentType.JSON)
            .cookies( cookies )
            .body(jsonObject)
            .post("/collection/" + collectionId + "/addRecord");

            if( response.statusCode() != 200 ) return false;
            return true;
		}
	}

	
	private ArrayNode createProvenance( Document doc) {
		try {
			return JacksonBuilder.arr().appendObj()
				.optional()
				.optionalAddText("provider", getEdmField( "dataProvider", doc))
				.optionalAddText( "resourceId", getEdmField( "dcidentifier", doc))
				.up()
			.appendObj().optional()
				.optionalAddText("provider", getEdmField( "provider", doc))
				.optionalAddText( "resourceId", getEdmField( "isShownAt", doc))
				.up()
			.appendObj()
				.add("provider","Mint")
				.optionalAddText( "resourceId", XmlValueUtils.getUniqueValue(doc, "//*[ local-name()='ProvidedCHO']/@*[local-name()='about']"))
				.up()
//			.appendObj()
//				.add("provider", "Mint")
//				.add( "resourceId", "urn://mint-projects.image.ntua.gr/mint4all?organization="
//						+ URLEncoder.encode(item.getDataset().getOrganization().getEnglishName(), "UTF8" )
//						+ "&itemNativeId=" + URLEncoder.encode(item.getPersistentId(), "UTF8" ))
//				.up()
			.getArray();
		} catch( Exception e ) {
			log.error( "", e );
			return JacksonBuilder.arr().getArray();
		}
	}
	
	private boolean mediaObject( JacksonBuilder obj, String url, Optional<String> edmRights,
			Optional<String> dcrights, Optional<String> edmtype ) {
		
		obj
			.add( "url", url )
			.optionalAddText("type", edmtype)
			.withObj( "originalRights" )
				.optional()
				.optionalAddText("uri", dcrights)
				.up()
			.add( "withRights", MediaRights.fromString(edmRights.orElse(null)).toString());
		return true;
	}
	
	private ArrayNode createMedia( Document doc ) {
		final JacksonBuilder result = JacksonBuilder.arr();
		Optional<String> edmRights = XmlValueUtils.getNonEmpty( doc, Edm.getProvenancepaths().get( "edmrights"));
		Optional<String> dcrights = XmlValueUtils.getNonEmpty( doc, Edm.getXpaths().get("dcrights"));
		Optional<String> edmtype = XmlValueUtils.getNonEmpty( doc, Edm.getProvenancepaths().get("edmtype"));
		
		Optional<String> original = XmlValueUtils.getUniqueValue(doc, Edm.getMediapaths().get("isShownBy"));
		Optional<String> thumb =  XmlValueUtils.getUniqueValue(doc, Edm.getMediapaths().get("object"));
		
		
		if( original.isPresent() ) {
			JacksonBuilder media = JacksonBuilder.obj()
					.put( "Original", child->
						mediaObject( child, original.get(), edmRights, dcrights, edmtype ))
					.put( "Thumbnail", child -> {
						if( !thumb.isPresent()) return false;
						mediaObject(child, thumb.get(), edmRights, dcrights, Optional.of( "IMAGE"));
						child.add("mediaVersion", "Thumbnail" );
						return true;
					} );
			result.optionalAppend( Optional.of(media));
		}
				
		for(  String hasView: XmlValueUtils.valueIterable(doc, Edm.getMediapaths().get("hasView"))) {
			JacksonBuilder media = JacksonBuilder.obj()
				.put( "Original", child->
						mediaObject( child, hasView, edmRights, dcrights, edmtype ));
			result.optionalAppend( Optional.of(media));			
		}
		
		return result.getArray();
	}
	
	
	public Optional<String> getEdmField( String fieldname, Document doc ) {
		String path = Edm.getProvenancepaths().get(fieldname);
		for( Node n:XmlValueUtils.nodeIterable(doc,path))
			return XmlValueUtils.getNonEmptyValFromElement(n);
		return Optional.empty();		
	}
		

	
	public static void main( String[] args ) {
		try {
			// String collectionId = args[0];
			File xmlFile = new File( args[0] );
			// XMLReader parser = j
			XMLReader parser = javax.xml.parsers.SAXParserFactory.newInstance().newSAXParser().getXMLReader();
			parser.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
				
			Builder builder = new Builder(parser);
			Document doc = builder.build( xmlFile );
			With withObj = new With( doc );

			ObjectMapper om = new ObjectMapper();
			om.enable(SerializationFeature.INDENT_OUTPUT);
			
			System.out.println( om.writeValueAsString(withObj));
	
//			String collectionId = "636a0d2da7b11b0007bac306"; //euscreen demo
//			String collectionId = "636a0f12a7b11b0007bac320"; //museu demo
//			String collectionId = "636a0f21a7b11b0007bac321"; //fashion demo
			
			//WithConnection withConnection = new WithConnection( "dev-api.crowdheritage.eu");
			//withConnection.login( "mariaral", "123456");
			//withConnection.addRecord(om.writeValueAsString(withObj), collectionId );
		} catch( Exception e ) {
			log.error( "", e );
		}
	}
}
