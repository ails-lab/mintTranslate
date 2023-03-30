package gr.ntua.ivml.mint.projects.with.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

import gr.ntua.ivml.mint.xml.util.XmlValueUtils;
import nu.xom.Attribute;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Node;

public class MultiLiteralOrResource extends HashMap<String, ArrayList<String>> {

	public static Optional<MultiLiteralOrResource> parseItem(String path, Document doc ) {

		MultiLiteralOrResource result = new MultiLiteralOrResource();
		for( Node n: XmlValueUtils.nodeIterable(doc, path)) 
			result.addNode(n);
		
		if (result.entrySet().size() == 0)
			return Optional.empty();
		
		result.updateDef();
		return Optional.of(result);
	}

	private void updateDef() {
		// TODO Auto-generated method stub
		if( containsKey( "def")) return;
		// people tend to not mark their own language, so own makes good default
		if( containsKey( "unknown")) {
			put( "def", get( "unknown"));
			return;
		}
		
		// longest length entry becomes def
		Optional<Entry<String, ArrayList<String>>> max = entrySet().stream()
				.filter(a -> !a.getKey().equals( "uri" ))
				.reduce( (a,b) -> 
					(a.getValue().size() > b.getValue().size())?a:b);
		max.ifPresent( entry-> put("def",entry.getValue()));
	}

	public void setLiteral(String label) {
		computeIfAbsent("unknown", (l) -> new ArrayList<>()).add(label);
	}

	public void setLiteral(String lang, String label) {
		computeIfAbsent(lang, (l) -> new ArrayList<>()).add(label);

		if (lang.equals("en"))
			computeIfAbsent("def", (l) -> new ArrayList<>()).add(label);
	}

	public void addNode(Node temp) {
		Element node = (Element) temp;

		for (int i = 0; i < node.getAttributeCount(); i++) {

			String type;
			Attribute atr = node.getAttribute(i);
			type = atr.getQualifiedName();
			if (type.contains("rdf:resource")) {
				addResource(atr.getValue());
				break;
			} else if (type.contains("xml:lang")) {
				if( StringUtils.isNotBlank( node.getValue()))
					setLiteral(atr.getValue(), node.getValue());
				break;
			}

		}
		if( node.getAttributeCount() == 0 ) { // nothing was added from aboves loop
			if( StringUtils.isNotBlank( node.getValue()))
				setLiteral(node.getValue());
		}
	}

	public ArrayList<String> getResources() {
		return computeIfAbsent("uri", (key) -> new ArrayList<>());
	}

	public void addResource(String resource) {
		getResources().add(resource);
	}
}
