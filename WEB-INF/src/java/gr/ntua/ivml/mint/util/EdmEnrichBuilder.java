package gr.ntua.ivml.mint.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.log4j.Logger;

import gr.ntua.ivml.mint.concurrent.GroovyTransform;
import gr.ntua.ivml.mint.concurrent.Queues;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.db.GlobalPrefixStore;
import gr.ntua.ivml.mint.f.Interceptor;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Item;
import gr.ntua.ivml.mint.persistent.Transformation;
import gr.ntua.ivml.mint.persistent.XmlSchema;
import nu.xom.Attribute;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Nodes;

// base class to make edm enrichments with a script
public class EdmEnrichBuilder {
	private static final Logger log = Logger.getLogger( EdmEnrichBuilder.class );
	final static String dctermsUriNamespace = "http://purl.org/dc/terms/" ;

	public XmlSchema schema = null;
	public String name = null;

	private Interceptor<Document, Document> documentInterceptor;
	private Optional<Consumer<Transformation>> postProcessor = Optional.empty();
			
	static List<String> providedChoIdx = Arrays.asList(
			// basetype in ProvidedCHO is unordered
			// the dc and dcterms elements can come in any order
			"{http://purl.org/dc/elements/1.1/}:description",
			"{http://purl.org/dc/elements/1.1/}:title",
			"{http://purl.org/dc/elements/1.1/}:creator",
			"{http://purl.org/dc/elements/1.1/}:contributor",
			"{http://purl.org/dc/elements/1.1/}:subject",
			"{http://purl.org/dc/elements/1.1/}:type",
			"{http://purl.org/dc/elements/1.1/}:coverage",
			"{http://purl.org/dc/elements/1.1/}:date",
			"{http://purl.org/dc/elements/1.1/}:format",
			"{http://purl.org/dc/elements/1.1/}:source",
			"{http://purl.org/dc/elements/1.1/}:rights",
			"{http://purl.org/dc/elements/1.1/}:relation",
            

			"{http://purl.org/dc/terms/}:hasPart",
			"{http://purl.org/dc/terms/}:spatial",
			"{http://purl.org/dc/terms/}:isPartOf",
			"{http://purl.org/dc/terms/}:medium",

			// extension is ordered but has to come after base type anyway
			"{http://www.europeana.eu/schemas/edm/}:currentLocation",
			"{http://www.europeana.eu/schemas/edm/}:hasMet" ,
			"{http://www.europeana.eu/schemas/edm/}:hasType" ,
			"{http://www.europeana.eu/schemas/edm/}:incorporates",
			"{http://www.europeana.eu/schemas/edm/}:isDerivativeOf",
			"{http://www.europeana.eu/schemas/edm/}:isNextInSequence" ,
			"{http://www.europeana.eu/schemas/edm/}:isRelatedTo",
			"{http://www.europeana.eu/schemas/edm/}:isRepresentationOf",
			"{http://www.europeana.eu/schemas/edm/}:isSimilarTo",
			"{http://www.europeana.eu/schemas/edm/}:isSuccessorOf",
			"{http://www.europeana.eu/schemas/edm/}:realizes",

			
			"{http://www.europeana.eu/schemas/edm/}:type"			
		);

	public  static class FieldInfo {
		public String namespace, elementName;
		public FieldInfo( String namespace, String elementName ) {
			this.elementName = elementName;
			this.namespace = namespace;
		}
	}
	
	
	// find for a fieldname the correct namespace, ignore prefix, mostly not needed.
	public static Optional<FieldInfo> findFieldInfoByName( String name ) {
		// seems to be the only ambiguous field in providedcho
		if( name.equals( "edm:type"))
			return Optional.of( new FieldInfo("http://www.europeana.eu/schemas/edm/", "type" ));
		if( name.contains(":")) {
			name = name.substring( name.indexOf(":"));
		}
		for( String field: providedChoIdx ) {
			if( field.endsWith(":"+name)) {
				int splitPos = field.lastIndexOf(':');
				String namespace = field.substring( 1, splitPos-1);
				return Optional.of( new FieldInfo( namespace, name ));
			}
		}
		return Optional.empty();
	}
	
	public static void enrichRdfResource( Document doc, String fieldLookupName, String uri, Optional<Boolean> humanGenerated, Optional<String> confidenceOpt ) {
		Optional<FieldInfo> fieldInfoOpt = findFieldInfoByName( fieldLookupName );
		
		if( !fieldInfoOpt.isPresent()  ) {
			log.error( "Unknown field name "+fieldLookupName+" for enrich");
			return;
		}
		
		Element elem = Item.createElement(doc, fieldInfoOpt.get().elementName, fieldInfoOpt.get().namespace, 
				providedChoIdx, "//*[local-name()='ProvidedCHO']" );
		elem.addAttribute( new Attribute( "rdf:resource", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", uri ));

		// not sure what is better, check the opt here or in the  subroutine??
		humanGenerated.ifPresent( b-> addAgent( elem, b ));
		addConfidence( elem, confidenceOpt );
	}
	
	
	public static void enrichLiteral( Document doc, String fieldLookupName, String lang, String val, Optional<Boolean> humanGenerated, Optional<String> confidenceOpt  ) {
		Optional<FieldInfo> fieldInfoOpt = findFieldInfoByName( fieldLookupName );
		
		if( !fieldInfoOpt.isPresent()  ) {
			log.error( "Unknown field name "+fieldLookupName+" for enrich");
			return;
		}
		Element elem = Item.createElement(doc,  fieldInfoOpt.get().elementName, fieldInfoOpt.get().namespace,
				providedChoIdx, "//*[local-name()='ProvidedCHO']" );
		addLang( elem, lang );
		humanGenerated.ifPresent( b-> addAgent( elem, b ));
		addConfidence( elem, confidenceOpt );
		elem.appendChild(val);
	}
	

	public EdmEnrichBuilder setDocumentInterceptor( Interceptor<Document, Document> modifyDocument ) {
		this.documentInterceptor = modifyDocument;
		return this;
	}
	
	public Interceptor<Item, Item> getInterceptor() {		
		return new Interceptors.ItemDocWrapInterceptor(documentInterceptor, true);
	}
	
	// create desccription in the document
	public static void enrichDescription( Document doc, String lang, String desc, String confidence) {
		Element descElem = Item.createElement(doc, "description", "http://purl.org/dc/elements/1.1/", 
				providedChoIdx, "//*[local-name()='ProvidedCHO']" );
		addLang( descElem, lang );
		addAgent( descElem );
		addConfidence( descElem,  Optional.ofNullable(confidence) );
		descElem.appendChild(desc);
	}
	
	public static void enrichTitle( Document doc, String lang, String title, String confidence ) {
		Element titleElem = Item.createElement(doc, "title", "http://purl.org/dc/elements/1.1/", 
				providedChoIdx, "//*[local-name()='ProvidedCHO']" );
		addLang( titleElem, lang );
		addAgent( titleElem );
		addConfidence( titleElem, Optional.ofNullable(confidence) );
		titleElem.appendChild(title);
	}
	
	// if there is exactly one extent, replace the text content with given
	public static void fixExtent( Document doc, String newExtent ) {
		Nodes nodes = doc.query( "//*[local-name()='extent' and namespace-uri()='"+ dctermsUriNamespace+"']" );
		if( nodes.size() != 1 ) return;
		Element elem  = (Element) nodes.get(0);
		elem.removeChildren();
		elem.insertChild(newExtent, 0);
	}
	
	
	public static void enrichCreator( Document doc, String uri,  String confidence ) {
		Element creatorElem = Item.createElement(doc, "creator", "http://purl.org/dc/elements/1.1/", 
				providedChoIdx, "//*[local-name()='ProvidedCHO']" );
		addAgent( creatorElem );
		addConfidence( creatorElem,  Optional.ofNullable(confidence));

		creatorElem.addAttribute( new Attribute( "rdf:resource", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", uri ));
	}

	public static void enrichSpatial( Document doc, String uri, String confidence ) {
		Element spatialElem = Item.createElement(doc, "spatial", "http://purl.org/dc/terms/", 
				providedChoIdx, "//*[local-name()='ProvidedCHO']" );
		addAgent( spatialElem );
		addConfidence( spatialElem,  Optional.ofNullable(confidence) );

		spatialElem.addAttribute( new Attribute( "rdf:resource", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", uri ));
	}

	public static void enrichRelatedTo( Document doc, String uri, String confidence ) {
		Element spatialElem = Item.createElement(doc, "isRelatedTo", "http://www.europeana.eu/schemas/edm/", 
				providedChoIdx, "//*[local-name()='ProvidedCHO']" );
		addAgent( spatialElem );
		addConfidence( spatialElem,  Optional.ofNullable(confidence) );

		spatialElem.addAttribute( new Attribute( "rdf:resource", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", uri ));
	}

	public static void enrichSubject( Document doc, String uri, String confidence ) {
		Element spatialElem = Item.createElement(doc, "subject", "http://purl.org/dc/elements/1.1/", 
				providedChoIdx, "//*[local-name()='ProvidedCHO']" );
		addAgent( spatialElem );
		addConfidence( spatialElem,  Optional.ofNullable(confidence) );

		spatialElem.addAttribute( new Attribute( "rdf:resource", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", uri ));
	}

	public static void enrichMedium( Document doc, String uri, String confidence ) {
		Element spatialElem = Item.createElement(doc, "medium", "http://purl.org/dc/terms/", 
				providedChoIdx, "//*[local-name()='ProvidedCHO']" );
		addAgent( spatialElem );
		addConfidence( spatialElem,  Optional.ofNullable(confidence) );

		spatialElem.addAttribute( new Attribute( "rdf:resource", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", uri ));
	}
	
	public static void enrichDcType( Document doc, String uri, String confidence ) {
		Element typeElem = Item.createElement(doc, "type", "http://purl.org/dc/elements/1.1/", 
				providedChoIdx, "//*[local-name()='ProvidedCHO']" );
		addAgent( typeElem );
		addConfidence( typeElem,  Optional.ofNullable(confidence) );

		typeElem.addAttribute( new Attribute( "rdf:resource", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", uri ));
	}

	public static void enrichContributor( Document doc, String uri, String confidence ) {
		Element spatialElem = Item.createElement(doc, "contributor", "http://purl.org/dc/elements/1.1/", 
				providedChoIdx, "//*[local-name()='ProvidedCHO']" );
		addAgent( spatialElem );
		addConfidence( spatialElem,  Optional.ofNullable(confidence) );

		spatialElem.addAttribute( new Attribute( "rdf:resource", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", uri ));
	}

	public static void removeDuplicateRdfResource( Document doc ) {
		
		// elements with rdf:resource
		Nodes nodes = doc.query("//*[local-name()='ProvidedCHO']/*[@*[local-name()='resource']]");
		Map<String, List<Element>> values = new HashMap<>();
		for (int i = 0; i < nodes.size(); i++) {
			Element elem = (Element) nodes.get(i);
			String resourceValue = elem.getAttributeValue("resource", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");

			List<Element> vec = values.get(resourceValue);
			if (vec == null) {
				vec = new ArrayList<Element>();
				values.put(resourceValue, vec);
			}
			vec.add(elem);
		}

		// now deal with the duplicates
		for (String k : values.keySet()) {
			List<Element> vec = values.get(k);
			// not duplicated
			if (vec.size() < 2)
				continue;

			// group by element name and see if we can remove some there
			Map<String, List<Element>> byElementName = new HashMap<>();
			
			Iterator<Element> it = vec.iterator();
			while (it.hasNext()) {
				Element e = it.next();

				String elementName = e.getLocalName();
				
				List<Element> vec2 = byElementName.get(elementName);
				if (vec2 == null) {
					vec2 = new ArrayList<Element>();
					byElementName.put(elementName, vec2);
				}
				vec2.add(e);
			}
			
			boolean specificAnnotationExists = false;
			for( String elementName: byElementName.keySet()) {
				if( elementName.equals("isRelatedTo" )) continue;
				specificAnnotationExists = true;
				
				// if we have the same element we should keep the computer annotation?
				// If that sounds odd, you are not alone :-(
				List<Element> vec2 = byElementName.get(elementName);
				if( vec2.size() < 2) continue;

				vec2.sort((Element e1, Element e2) -> {
					// if we have a wasGeneratedBy put it at the end of the list
					int e1b = (e1.getAttribute("wasGeneratedBy", "http://www.europeana.eu/schemas/edm/" ) == null)?0:1;
					int e2b = (e2.getAttribute("wasGeneratedBy", "http://www.europeana.eu/schemas/edm/" ) == null)?0:1;
					return Integer.compare(e1b, e2b);
				} );
				it = vec2.iterator();
				// we need to leave at least one
				int limit = vec2.size();
				while (it.hasNext()) {
					Element e = it.next();
					if( limit > 1 ) {
						e.getParent().removeChild(e);
						log.info( "Removed duplicated " + elementName + "'" + k + "'");
						limit--;
					}
				}
			}
			// remove isRelatedTo if the same URL is elsewhere as well, surely thats redundant
			List<Element> vec2 = byElementName.get( "isRelatedTo" );
			// nothing to do here
			if( vec2 == null ) continue;
			
			if( !specificAnnotationExists ) {
				// need to keep one
				vec2.remove(0);
			} // else all can go
			for( Element e: vec2 ) {
				e.getParent().removeChild( e );
				log.info( "Removed duplicated edm:isRelatedTo '" + k + "'");
			}			
		} // end of loop over equal rdf:resources
	}

	// simple duplicate rdf:resource strategy, first appearance wins.
	// the returned SetCounter counts how many removals per element name happened
	public static SetCounter simpleDuplicateResourceRemoval( Document doc ) {
		SetCounter result = new SetCounter();
		Nodes nodes = doc.query("//*[local-name()='ProvidedCHO']/*[@*[local-name()='resource']]");
		Set<String> seenResources = new HashSet<>();
		for (int i = 0; i < nodes.size(); i++) {
			Element elem = (Element) nodes.get(i);
			String resourceValue = elem.getAttributeValue("resource", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
			if( seenResources.contains(resourceValue)) {
				elem.getParent().removeChild(elem);
				result.add( elem.getLocalName());
			} else {
				seenResources.add( resourceValue );
			}
		}
		return result;
	}
	
	public static void tagXX( Document doc, boolean conditional ) {
		if( conditional ) {
			Nodes annotated = doc.query("//@*[local-name()='wasGeneratedBy' and .='SoftwareAgent']");
			if ((annotated.size() == 0)) return;
		}
		
		Nodes nodes = doc.query("//*[local-name()='isPartOf']");

		for (int i = 0; i < nodes.size(); i++) {
			Element e = (Element) nodes.get(i);
			String val = e.getValue();
			// no duplicated XX tags
			if (val.contains("Century of Change"))
				return;
		}

		// ok to insert Century of Change
		Element elem = new Element("dcterms:isPartOf", "http://purl.org/dc/terms/");
		elem.appendChild("Europeana XX: Century of Change");

		Attribute att = new Attribute("xml:lang", "http://www.w3.org/XML/1998/namespace", "en");
		elem.addAttribute(att);

		nodes = doc.query("//*[local-name()='ProvidedCHO']");
		if (nodes.size() > 0) {
			Element parent = (Element) nodes.get(0);
			parent.insertChild(elem, 0);
		}
	}
	
	
	public static void addLang( Element elem, String lang  ) {
		Attribute attLang = new Attribute("xml:lang", "http://www.w3.org/XML/1998/namespace", lang );
		elem.addAttribute(attLang);
	}

	
	public static void addAgent( Element elem) {
		addAgent( elem, false);
	}
	
	public static void addAgent( Element elem, boolean humanGenerated) {
		String agent = humanGenerated?"Person": "SoftwareAgent";
		Attribute att = new Attribute("edm:wasGeneratedBy", "http://www.europeana.eu/schemas/edm/", agent);
		elem.addAttribute( att );
	}
	
	
	public static void ensureNamespace( Document doc, String uri ) {
		HashMap<String, String> namespaces = Item.effektiveNamespaces( doc.getRootElement());
		if( namespaces.containsKey(uri)) return;
		// missing, need to add it
		String prefix = GlobalPrefixStore.getPrefix(uri);
		if( prefix == null ) { 
			log.warn( "No prefix for '" + uri + "' in store!");
			return;
		}
		doc.getRootElement().addNamespaceDeclaration( prefix, uri );
	}
	
	public static void addConfidence( Element elem, Optional<String> confidenceOpt ) {
		if( !confidenceOpt.isPresent()) return;
		String confidence = confidenceOpt.get();
		if( StringUtils.empty(confidence)) return;
		
		String conf = "0.0";
		try {
			float val = Float.parseFloat(confidence);
			if( val >= 0.0 && val <= 100.0 )
				conf = String.format("%.2g", val );
		} catch( Exception e ) {
			log.debug("'" + confidence + "' not a float");
			return;
		}
		Attribute att = new Attribute("edm:confidenceLevel", "http://www.europeana.eu/schemas/edm/", conf);
		elem.addAttribute( att );
	}
	
	public static Transformation prepareTransformation( Dataset ds ) {
		Transformation transformation = new Transformation();
		transformation.init(ds.getCreator());
		transformation.setName("Process: "+ds.getName());
		transformation.setParentDataset(ds);
		transformation.setCreated(new Date());
		transformation.setOrganization(ds.getOrganization());
		transformation.setSchema( ds.getSchema());
		
		return transformation;
	}
	
	public EdmEnrichBuilder setTargetSchema( XmlSchema schema ) {
		this.schema = schema;
		return this;
	}
	
	public EdmEnrichBuilder setPostProcessor( Consumer<Transformation> postProcessor ) {
		this.postProcessor = Optional.of(postProcessor);
		return this;
	}
	
	public EdmEnrichBuilder setName( String name ) {
		this.name = name;
		return this;
	}
	
	public void submit( Dataset ds ) throws Exception {
		if( documentInterceptor == null ) {
			throw new Exception( "No document Interceptor set, no operation");
		}
		Transformation tr= prepareTransformation(ds);
		if( this.schema != null ) 
			tr.setSchema(schema);
		if( this.name != null )
			tr.setName(name);
		
		DB.getTransformationDAO().makePersistent(tr);
		DB.commit();
		GroovyTransform gt = new GroovyTransform(tr, getInterceptor());
		gt.postProcessor = postProcessor;
		
		Queues.queue( gt, "db");		
	}
	
	public void submit( Dataset ...datasets  ) throws Exception {
		// the executable part 
		for( Dataset ds: datasets ) {
			submit( ds );
		}		
	}

	public void submit( long ...datasetIds  ) throws Exception {		
		for( long dsId: datasetIds ) {
			Dataset ds = DB.getDatasetDAO().getById(dsId, false);
			submit( ds );
		}
	}
}

/*
 * a) make an document modifier
 * 
 * def makeModifier() {
 * 
 *  Enrichment.EnrichBuilder titleEnrich = DB.enrichmentDAO.getById( xxx, ).getEnrichBuilder( 0 )
 *  Enrichment.EnrichBuilder creatorEnrich = DB.enrichmentDAO.getById( xxx, ).getEnrichBuilder( 0 ) 
 * 
 *  ThrowingConsumer<Document> mod = doc -> {
 *   String key = doc.query...
 *   .. all the magic
 *   titleEnrich.enrich( row -> EdmEnrichBuilder.enrichTitle( key, row[1], row[2] ))
 *   descEnrich.enrich( row -> ... )
 *   
 *  }
 *  return mod
 *  }
 *  
 *  def interceptor = Interceptor.modifyInterceptor( makeModifier() ).into(  euscreen.RequestHandler.addCenturyOfChange())
 *  
 *  new EdmEnirchBuilder().setName( "Enrich and tag" )
 *   .setSchema( theEnrichSchema )
 *   .setInterceptor( interceptor )
 *   .submit( num, num, num ....)
 *  
 */
