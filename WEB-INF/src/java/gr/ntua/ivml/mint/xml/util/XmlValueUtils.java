package gr.ntua.ivml.mint.xml.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import nu.xom.Attribute;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Node;
import nu.xom.Nodes;
import nu.xom.Text;

public class XmlValueUtils {
	
	public static class LangString {
		String lang, literal;
	}

	// return value if there is exactly one non-empty value on path. (trim it first)
	// otherwise return empty
	public static Optional<String> getUniqueValue( Document doc, String path) {
		Optional<String> result = Optional.empty();
		for( Node n: nodeIterable(doc,path)) {
			String val = n.getValue();
			if( !StringUtils.isEmpty(val)) {
				if( result.isPresent()) return Optional.empty();
				else result = Optional.of( val.trim());
			}
		}
		return result;
	}

	// return success or not
	public static boolean setUniqueValue( Document doc, String path, String newValue) {

		int count = 0;
		for( Node n: nodeIterable(doc,path)) {
			count++;
		}
		if( count != 1 ) return false;
		
		for( Node n: nodeIterable(doc,path)) {
			if( n instanceof Attribute ) { 
				((Attribute)n ).setValue(newValue);
				return true;
			}
						
			if( n instanceof Element ) { 
				((Element) n).removeChildren();
				((Element) n).appendChild(newValue);
				return true;
			}
		}
		return false;
	}

	// how many replacements were made, matches that actually changed nodes.
	public static int replaceValues( Document doc, String path, String matchExpression, String replaceString ) {
		int count = 0;
		for( Node n: nodeIterable(doc,path)) {
			if( n instanceof Attribute ) {
				String oldValue= ((Attribute)n ).getValue();
				String newValue = oldValue.replaceAll(matchExpression, replaceString);
				if( newValue.equals( oldValue )) continue;
				
				((Attribute)n ).setValue(newValue);
				count++;
			}
						
			if( n instanceof Element ) { 
				String oldValue= ((Element)n ).getValue();
				String newValue = oldValue.replaceAll(matchExpression, replaceString);
				if( newValue.equals( oldValue )) continue;
				((Element) n).removeChildren();				
				((Element) n).appendChild(newValue);
				count++;
			}
		}
		return count;
		
	}
	
		
	public static List<Node> queryDoc( Document doc, String query ) {
		List<Node> result = new ArrayList<>();
		Nodes nodes = doc.query( query );
		for( int i=0; i<nodes.size(); i++ ) 
			result.add( nodes.get( i ));
		return result;
	}
	
	
	public static Iterable<Node> nodeIterable( Document doc , String query ) {
		return queryDoc( doc, query );
	}
	

	public static Iterable<String> valueIterable( Document doc, String query ) {
		
		return queryDoc( doc, query ).stream()
			.map( n -> n.getValue())
			.collect( Collectors.toList());
	}

	public static Optional<String> getLanguage( Element elem ) {
		for( int i=0; i<elem.getAttributeCount(); i++ ) {
			Attribute att = elem.getAttribute(i);
			if( att.getNamespaceURI().equals( "http://www.w3.org/XML/1998/namespace" ) 
				&& att.getLocalName().equals( "lang")) {
				return Optional.of( att.getValue());
			}			
		}
		return Optional.empty();
	}


	public static Optional<String> getNonEmpty( Document doc, String path ) {
		for( Node n: nodeIterable(doc,path)) {
			String val = n.getValue();
			if( !StringUtils.isEmpty(val)) return Optional.of( val.trim());
		}
		return Optional.empty();				
	}
	
	public static Optional<String> getAttValIgnoreNamespace( Node node, String attributeName ) {
		Element elem = (Element) node;

		for (int i = 0; i < elem.getAttributeCount(); i++) {
			Attribute att = elem.getAttribute(i);
			if( att.getLocalName().equals(attributeName))
				return Optional.of( att.getValue());
		}
		return Optional.empty();
	}


	public static Optional<String> getNonEmptyValFromElement( Node node ) {
		if( ! (node instanceof Element )) return Optional.empty();
		String val = node.getValue();
		if( StringUtils.isEmpty(val)) return Optional.empty();
		return Optional.of(val);
	}
	
	
	public static Optional<LangString> getLangStringFromNode( Node node ) {
		return getNonEmptyValFromElement(node).map( text -> {
			LangString ls = new LangString();
			ls.lang =  getAttValIgnoreNamespace(node, "lang").orElse( "unknown");
			ls.literal = text;
			return ls;
		});
	}
	
	public static boolean hasTextChild( Element elem ) {
		for( int i=0; i<elem.getChildCount(); i++ ) {
			Node node = elem.getChild(i);
			if( node instanceof Text ) return true;
		}
		return false;
	}
	
	public static boolean hasElementChild( Element elem ) {
		for( int i=0; i<elem.getChildCount(); i++ ) {
			Node node = elem.getChild(i);
			if( node instanceof Element ) return true;
		}
		return false;
	}
	


}
