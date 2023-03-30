package gr.ntua.ivml.mint.projects.fashion;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import nu.xom.Attribute;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Node;
import nu.xom.Nodes;


/**
 * The "standard" fashion enrichment separated out for xx.
 * @author arne
 *
 */
public class ManualEnrich {

	private static final Logger log = Logger.getLogger(ManualEnrich.class);

	final static String dcUriNamespace = "http://purl.org/dc/elements/1.1/";
	final static String dctermsUriNamespace = "http://purl.org/dc/terms/" ;
	final static String rdfUriNamespace = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	final static String edmNamespace = "http://www.europeana.eu/schemas/edm/";
	
	private static Map<String, String> creatorEnrich = new HashMap<>();
	private static Map<String, String> spatialEnrich = new HashMap<>();
	private static long lastAccess = 0;

	
	/**
	 * Read two column tsv file into given Map. TSV should be literal\turi\n
	 * @param resourceName
	 * @param targetMap
	 * @throws Exception
	 */
	private static void tsvToMap( String resourceName, Map<String,String> targetMap ) throws Exception  {
		try( InputStream tsvStream = ManualEnrich.class.getClassLoader().getResourceAsStream( resourceName )) {
			synchronized( targetMap ) {
				List<String> lines = IOUtils.readLines(tsvStream, "UTF8");
				targetMap.clear();
				for( String line : lines ) {
					String columns[] = line.split("\\t",2);
					if( !StringUtils.isEmpty(columns[0]) && !StringUtils.isEmpty( columns[1]) ) {
						targetMap.put( columns[0],  columns[1]);
					}
				}
			}
		} catch( Exception e ) {
			log.error("",e );
			throw e;
		}
	}
	
	private static void readManualEnrichments() {
		try {
			tsvToMap( "/gr/ntua/ivml/mint/projects/fashion/creator_enrich.tsv", creatorEnrich );
			tsvToMap( "/gr/ntua/ivml/mint/projects/fashion/spatial_enrich.tsv", spatialEnrich );
			lastAccess = System.currentTimeMillis();
		} catch( Exception e ) {
			log.error("Enrich IO problem", e );
		} 
	}
	
	private static String getEnrichUri( String literal, Map<String, String> enrichMap ) {
		// outdated, older than 1h?
		if( System.currentTimeMillis() - lastAccess > 3600l*1000 ) {
			readManualEnrichments();
		} else {
			lastAccess = System.currentTimeMillis();
		}
		synchronized( enrichMap ) {
			return enrichMap.get( literal ); 
		} 			
	}
		
	private static void swapLiteralForResource( Node element, Map<String,String> map, Function<String, String> modifyLiteral, boolean keepLiteral, boolean markAsSoftwareAgent ) {
		Element elem = (Element) element;
		String literal = elem.getValue();
		// log.info( "Literal: '" + literal + "'");
		literal = modifyLiteral.apply( literal );
		String resource = getEnrichUri( literal, map );
		if( resource != null) {
		
			if( keepLiteral ) {
				Element newElem = (Element) elem.copy();
				int index = elem.getParent().indexOf(elem);
				elem.getParent().insertChild(newElem, index );
			}
			
			// remove literal, children and attributes
			elem.removeChildren();
			for( int i=elem.getAttributeCount(); i-->0;) {
				elem.removeAttribute(elem.getAttribute(i));
			}
			
			// create rdf:resource attribute 
			elem.addAttribute( new Attribute( "rdf:resource", rdfUriNamespace, resource ));
			if ( markAsSoftwareAgent ) {
				elem.addAttribute( new Attribute( "edm:wasGeneratedBy", edmNamespace, "SoftwareAgent" ));
				elem.addAttribute( new Attribute( "edm:confidenceLevel", edmNamespace, "1.0" ));
			}
		}
	}
	
	
	/**
	 * Applies changes to dc:creator and dc:spatial according to the tsv files
	 * creator_enrich.tsv and spatial_enrich.tsv
	 * @param xml
	 * @return xml with creator and spatial replaced with rdf:references
	 */
	public static void manualEnrich( Document doc, boolean markAsSoftwareAgent ) {
		try {
			// modify dc:spatials
			// modify dc:creator
			Nodes nodes = doc.query( "//*[local-name()='spatial' and namespace-uri()='"+ dctermsUriNamespace+"']" );
			for( int i=0; i<nodes.size(); i++ ) {
				nu.xom.Node n = nodes.get( i );
				swapLiteralForResource(n,  spatialEnrich, str->str , false, markAsSoftwareAgent );
			}
			
			// if found replace 
			nodes = doc.query( "//*[local-name()='creator' and namespace-uri()='"+ dcUriNamespace+"']" );
			for( int i=0; i<nodes.size(); i++ ) {
				nu.xom.Node n = nodes.get( i );
				swapLiteralForResource(n,  creatorEnrich, 
						(String str) -> str.replaceAll(" +(\\(Photographer\\))$", ""), false, markAsSoftwareAgent
				);
				swapLiteralForResource(n,  creatorEnrich, 
						(String str) -> str.replaceAll(" +(\\(Designer\\))$", ""), true, markAsSoftwareAgent
				);
			}
			
			nodes = doc.query( "//*[local-name()='contributor' and namespace-uri()='"+ dcUriNamespace+"']" );
			for( int i=0; i<nodes.size(); i++ ) {
				nu.xom.Node n = nodes.get( i );
				swapLiteralForResource(n,  creatorEnrich, 
						(String str) -> str.replaceAll(" +(\\(Stylist\\)|\\(Model\\)|\\(Curator\\)|" 
								+ "\\(Author\\)|\\(Illustrator\\)|\\(Set designer\\)|"
								+ "\\(Hair stylist\\)|\\(Make up artist\\)|\\(Editor\\))$", ""), false, markAsSoftwareAgent
				);
			}
		} catch( Exception e ) {
			log.error( "Manual enrich failed", e);
		}
	}
}
