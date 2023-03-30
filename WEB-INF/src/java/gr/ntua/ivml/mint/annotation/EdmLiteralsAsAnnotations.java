package gr.ntua.ivml.mint.annotation;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.XMLReader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import gr.ntua.ivml.mint.annotation.Annotation.Creator;
import gr.ntua.ivml.mint.annotation.PangeanicApi.TranslationDetails;
import gr.ntua.ivml.mint.persistent.AnnotationSet;
import gr.ntua.ivml.mint.xml.util.XmlValueUtils;
import nu.xom.Attribute;
import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Node;

public class EdmLiteralsAsAnnotations {
	
	private static final Logger log = LogManager.getLogger();
	
	private static ObjectMapper om = new ObjectMapper();
	static {
		om.enable(SerializationFeature.INDENT_OUTPUT);
	}
	
	public static List<String> validFieldnames = Arrays.asList(new String[] {
		"dc:title", "dc:description","dc:format" 
		// and more
	});
	
	public static Map<String,String> edmNamespaces = new HashMap<>();
	static { 
		String[] vals = new String[] {
				// prefix, namespace
				"dc","http://purl.org/dc/elements/1.1/",
				"dcterms","http://purl.org/dc/terms/",
				"edm" ,"http://www.europeana.eu/schemas/edm/",
				"enrichment","http://www.europeana.eu/schemas/edm/enrichment/",
				"foaf","http://xmlns.com/foaf/0.1/",
				"ore" ,"http://www.openarchives.org/ore/terms/",
				"owl", "http://www.w3.org/2002/07/owl#",
				"rdaGr2","http://rdvocab.info/ElementsGr2/",
				"rdf" ,"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
				"rdfs" ,"http://www.w3.org/2000/01/rdf-schema#",
				"sch","http://purl.oclc.org/dsdl/schematron",
				"skos" ,"http://www.w3.org/2004/02/skos/core#",
				// "wgs84_pos","http://www.w3.org/2003/01/geo/wgs84_pos#",
				"wgs84","http://www.w3.org/2003/01/geo/wgs84_pos#",
				"xml", "http://www.w3.org/XML/1998/namespace"
		};
		for( int i=0; i<vals.length; i+=2 )
			edmNamespaces.put( vals[i], vals[i+1]);
	}
	
	public static class Literal {
		
		// language can be null
		public String path,field, value, language, recordId;		
		
		public HashMap<String, String> asMap() {
			HashMap<String, String> result = new HashMap<>();
			result.put( "path", path);
			result.put( "field", field);
			result.put( "value", value.trim());
			result.put( "recordId", recordId );
			if( language != null ) result.put( "language", language);
			return result;
		}
		
		public Literal copy() {
			Literal res = new Literal();
			res.path = path;
			res.field = field;
			res.value = value;
			res.language = language;
			res.recordId = recordId;
			return res;
		}
	}
	
	
	
	private static void buildLiteralList( Element elem, String path, Map<String, String> namespaceToPrefix,
			List<Literal> result, Predicate<Node> filter ) {
		Optional<String> lang = XmlValueUtils.getLanguage( elem );
		
		for( int i=0; i<elem.getAttributeCount(); i++ ) {
			Attribute att = elem.getAttribute(i);
			if( att.getNamespaceURI().equals( "http://www.w3.org/XML/1998/namespace" ) 
				&& att.getLocalName().equals( "lang")) {
				continue;
			}
			String content = att.getValue();
			// replace funny spaces with real ones
			content = content.replaceAll("[\u00A0\u1680\u180E\u2000-\u200B\u202F\u205F\u3000\uFEFF]", " ");
			if( StringUtils.isNotBlank(content) && filter.test(att)) {
				Literal lit = new Literal();
				lit.language = lang.orElse(null);
				lit.field = "@"+getNodeName( att, namespaceToPrefix );
				lit.path = path+"/"+getNodeName( elem, namespaceToPrefix );
				lit.value = content;
				result.add( lit );
			}
		}
		
		// if elem has element child, we dont want a literal, its mixed content in best case
		if( !XmlValueUtils.hasElementChild( elem )) {
			String textContent = elem.getValue();
			textContent = textContent.replaceAll("[\u00A0\u1680\u180E\u2000-\u200B\u202F\u205F\u3000\uFEFF]", " ");
			if( StringUtils.isNotBlank( textContent ) && filter.test( elem )) {
				Literal lit = new Literal();
				lit.language = lang.orElse(null);
				lit.field = getNodeName( elem, namespaceToPrefix );
				lit.path = path;
				lit.value = textContent;
				result.add( lit );			
			}
			return;
		}
		
		for( int i=0; i<elem.getChildCount(); i++ ) {
			Node node = elem.getChild(i);
			if( node instanceof Element ) {
				buildLiteralList( (Element ) node, 
						path + "/" + getNodeName( elem, namespaceToPrefix), 
						namespaceToPrefix, result, filter );
			}
		}
	}
	
	public static boolean isElementGenerated( Element elem ) {
		for( int i=0; i<elem.getAttributeCount(); i++ ) {
			Attribute att = elem.getAttribute(i);
			if( att.getNamespaceURI().equals( "http://www.europeana.eu/schemas/edm/" ) 
				&& att.getLocalName().equals( "wasGeneratedBy")) {
				return true;
			}
		}
		return false;
	}
	
	public static List<Literal> buildLiteralList( Document doc, Map<String, String> prefixToNamespace, Predicate<Node> filter) {
		List<Literal> result = new ArrayList<Literal>();
		
		String recordId = XmlValueUtils.getUniqueValue(doc, "//*[local-name()='ProvidedCHO']/@*[local-name()='about']").orElse(null);
		if( recordId == null ) {
			log.warn( "Record has no rdf:about" );
			return result;
		}
		
		Element root = doc.getRootElement();
		Map<String, String> namespaceToPrefix = prefixToNamespace.entrySet().stream()
				.collect(Collectors.toMap(Entry::getValue, Entry::getKey));
		buildLiteralList(root, "", namespaceToPrefix, result, filter );
		for( Literal l:result) l.recordId = recordId;
		return result;
	}
	
	private static String getNodeName( Node node, Map<String, String> namespaceToPrefix) {
		
		String name, namespace;
		if( node instanceof Attribute ) {
			name = ((Attribute) node).getLocalName();
			namespace = ((Attribute) node).getNamespaceURI();
		} else if( node instanceof Element ) {
			name = ((Element) node).getLocalName();
			namespace = ((Element) node).getNamespaceURI();
		} else return null;
		
		if( namespace != null ) {
			String prefix = namespaceToPrefix.get( namespace );
			if( prefix != null ) 
				name = prefix+":"+name;
			else 
				name = "{"+namespace+"}:"+name;
		}
		return name;
	}
	
	/*
	 * 
	 * 
	 */
	// traverse attributes first then children, then self 
	public static void xmlTreeCheck( Node node, Consumer<Node> nodeModifier ) {
		if( node instanceof Element ) {
			Element elem = (Element) node;
			for( int i=0; i<elem.getAttributeCount(); i++ ) {
				Attribute att = elem.getAttribute(i);
				nodeModifier.accept(att);
			}

			for( int i=0; i<elem.getChildCount(); i++ ) {
				Node child = elem.getChild(i);
				xmlTreeCheck( child, nodeModifier );
			}

		} 
		
		nodeModifier.accept(node);
	}
	
	public static Set<String> unknownNamespaces( Document doc, Map<String, String> namespaceToPrefix ) {
		Set<String> result = new HashSet<String>();
		
		Consumer<Node> testNamespace = node -> { 
			String uri = null;
			if( node instanceof Element ) {
				Element elem = (Element ) node;
				uri = elem.getNamespaceURI();
			}
			if( node instanceof Attribute ) {
				Attribute att = (Attribute) node;
				uri = att.getNamespaceURI();
			}
			if(( uri != null) && (!namespaceToPrefix.containsKey(uri)))
				result.add( uri );
		};
		
		xmlTreeCheck( doc.getRootElement(), testNamespace );
		return result;
	}
	
	
	// move doc to given prefix map, on false, the process failed.
	public static boolean namespaceLift(Document doc, Map<String, String> namespaceToPrefix ) {
		Set<String> usedNamespaces = new HashSet<>();
		if( !unknownNamespaces(doc, namespaceToPrefix).isEmpty()) return false;
		
		Consumer<Node> modifyNamespace = node -> { 
			String uri = null;
			if( node instanceof Element ) {
				Element elem = (Element ) node;
				uri = elem.getNamespaceURI();
				String prefix = namespaceToPrefix.get( uri );
				usedNamespaces.add( uri );
				elem.setNamespacePrefix(prefix);
				// clear internal namespace nodes
				Set<String> prefixesDeclared = new HashSet<>();
				for( int i=0; i<elem.getNamespaceDeclarationCount(); i++ ) 
					prefixesDeclared.add( elem.getNamespacePrefix(i));
				for( String prefixDeclared: prefixesDeclared ) 
					elem.removeNamespaceDeclaration(prefixDeclared);
				
			}
			if( node instanceof Attribute ) {
				Attribute att = (Attribute) node;
				uri = att.getNamespaceURI();
				String prefix = namespaceToPrefix.get( uri );
				usedNamespaces.add( uri );
				att.setNamespace(prefix, uri);
			}
		};

		// adjust the tree 
		xmlTreeCheck(doc.getRootElement(), modifyNamespace);
		
		// add the missing namespace declarations 
		for( String uri: usedNamespaces ) 
			doc.getRootElement().addNamespaceDeclaration(namespaceToPrefix.get( uri ), uri);

		return true;
	}
	
	
	private static JsonNode getStandardEdmContext() {
		try {
			return om.readTree( AnnotationSet.class.getClassLoader().getResourceAsStream("edmStandardContext.json" ));
		} catch( Exception e ) {
			log.error( "Standard Context not found", e );
			return null;
		}
	}

	public static JsonNode createAnnotationSetFromLiterals( List<Literal> literals ) {
		ObjectNode result = om.createObjectNode();
		result.set( "@context", getStandardEdmContext() );		
		result.set( "@graph",om.valueToTree( translate(literals)));
		pruneNull( result );
		return result;
	}
	
	public static JsonNode createAnnotationSetFromLiterals( List<Literal> literals, int maxCharCount ) {
		ObjectNode result = om.createObjectNode();
		result.set( "@context", getStandardEdmContext() );		
		result.set( "@graph",om.valueToTree( translateBatched(literals, maxCharCount )));
		pruneNull( result );
		return result;
	}
	
	
	// split into batches according to length of elements
	// collect elements that are too long in batch 0
	public static <T> List<List<T>> batchList( List<T> elements, Function<T,Integer> sizeingFunction, int maxSize ) {
		
		List<List<T>> result = new ArrayList<>();
		// big ones go here and will be 0 elem
		List<T> badBatch = new ArrayList<>();
		result.add( badBatch );
		
		Iterator<T> it = elements.iterator();
		int currentBatchSize = 0;
		List<T> currentBatch = null;

		while( it.hasNext()) {
			T nextElement = it.next();
			int elemSize = sizeingFunction.apply( nextElement );
			if( elemSize <= maxSize ) {
				if(( elemSize + currentBatchSize > maxSize ) ||
						(currentBatch == null)) {
					currentBatch = new ArrayList<>();
					currentBatch.add( nextElement );
					result.add( currentBatch );
					currentBatchSize = elemSize;
				} else {
					currentBatch.add( nextElement );
					currentBatchSize += elemSize;
				}
			} else {
				// ignore 
				badBatch.add( nextElement );
			}
		}
		return result;
	}
	
	public static void pruneNull( JsonNode tree ) {
		if( tree instanceof ObjectNode ) {
			ObjectNode obj = (ObjectNode) tree;
			Iterator<Entry<String,JsonNode>> it = obj.fields();
			while( it.hasNext() ) {
				Entry<String,JsonNode> e = it.next();
				if(( e.getValue() == null ) || ( e.getValue().isNull()))
					it.remove();
				else pruneNull( e.getValue());
			}
		}
		if( tree instanceof ArrayNode ) {
			ArrayNode arr = (ArrayNode) tree;
			for( JsonNode elem :arr ) {
				pruneNull( elem );
			}
		}
	}

	public static List<Literal> edmExtract( Document doc, String... fields ) {
		List<Literal> res = buildLiteralList(doc, edmNamespaces, 
				n -> {
					if( ! (n instanceof Element )) return false;
					Element elem = (Element) n;
					for( int i=0; i<elem.getAttributeCount();i++)
						if( elem.getAttribute(i).getValue().equals("SoftwareAgent"))
							return false;
					return Arrays.asList(fields ).contains( ((Element) n).getLocalName()); 
				} );
		return res;
	}
	
	public static List<Literal> filterByLang( List<Literal> literals, String lang ) {
		return literals.stream().filter( l -> l.language.equals(lang)).collect(Collectors.toList());
	}
	
	// zip the sources (fieldnames) and translations together into Annotations
	public static List<Annotation> fromTranslation( List<PangeanicApi.TranslationDetails> translations,
			List<Literal> sources ) {

		ArrayList<Annotation> res = new ArrayList<>();
		Iterator<TranslationDetails> translationIterator = translations.iterator();
		Iterator<Literal> sourceIterator = sources.iterator();
		
		while( translationIterator.hasNext() && sourceIterator.hasNext()) {
			TranslationDetails tl = translationIterator.next();
			Literal literal = sourceIterator.next();
			
			Annotation annotation = new Annotation();
			
			// make body
			annotation.body = new Annotation.Body();
			annotation.body.language = "en";
			annotation.body.value = tl.getTranslatedValue();
			
			// make target
			annotation.target = new Annotation.Target();
			annotation.target.source = literal.recordId;
			annotation.target.selector = new Annotation.Selector();
			annotation.target.selector.type = "RDFPropertySelector";
			annotation.target.selector.property = literal.field;
			annotation.target.selector.destination = new Annotation.Literal(tl.getOriginalValue(), literal.language );
			annotation.target.selector.refinedBy = new Annotation.RefinedBy();
			
			
			annotation.scope = literal.field;
			
			annotation.creator = new Creator();
			annotation.creator.type = "Software";
			annotation.creator.name = "Pangeanic Translation API"; 
			annotation.confidence = String.format( "%.2f", tl.getTranslationScore());
			
			OffsetDateTime now = OffsetDateTime.now();
			DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
			annotation.created = formatter.format(now);

			
			res.add( annotation );
		}
		return res;
	}
	
	
	public static List<Annotation> translate( List<Literal> literals ) {
		ArrayNode n = om.createArrayNode();

		if( literals.isEmpty() ) return Collections.emptyList();
		
		List<PangeanicApi.TranslationDetails> tls = new ArrayList<>();
		for( Literal l: literals ) {
			TranslationDetails tl = new TranslationDetails(l.value);
			tl.setDetectedLanguage(l.language);
			tls.add( tl );
		}
		try {
			PangeanicApi.translate(tls);
		} catch( Exception e ) {
			log.error( "Pangeanic problem", e );
			return Collections.emptyList();
		}
		return fromTranslation( tls, literals );
	}

	public static List<Annotation> translateBatched( List<Literal> literals, int batchSizeCharCount ) {
		List<List<Literal>> batchedInput = batchList( literals, lit -> lit.value.length(), batchSizeCharCount );
		List<Annotation> result = new ArrayList<>();

		if( batchedInput.size() > 2 ) {
			log.warn( "Batch into " + (batchedInput.size()-1) + " batches. ");
		}
		
		boolean first = true;
		for( List<Literal> listLit: batchedInput ) {
			if( first ) {
				if( listLit.size() > 0 ) {
					log.warn( "Discarded " + listLit.size() + " literals during batching for translate.");
				}
				first = false;
			} else 
				result.addAll( translate( listLit  ));
		}
		
		return result;
	}

	
	public static void main( String[] args ) {
		try {
			XMLReader parser = org.xml.sax.helpers.XMLReaderFactory.createXMLReader(); 
			parser.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
	
			
			Builder builder = new Builder(parser);
			Document doc = builder.build( EdmLiteralsAsAnnotations.class.getResourceAsStream("/exampleXml/Item_25735142.xml"));
			List<Literal> res = buildLiteralList(doc, edmNamespaces, 
					// n -> true );
					 //n -> !( n instanceof Attribute)  );
					n -> {
						if( ! (n instanceof Element )) return false;
						return Arrays.asList( "title", "description").contains( ((Element) n).getLocalName()); 
					} );
			
			
			res.stream().forEach( l -> {
				System.out.println( "Field: " + l.field + " Path: " + l.path + " Value: " + l.value 
						+ (l.language != null ? 
								" Lang: " + l.language :
								"" ));
			});
		} catch( Exception e ) {
			e.printStackTrace();
		}
	}
}

