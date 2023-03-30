package gr.ntua.ivml.mint.projects.with.model;

import java.util.HashMap;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gr.ntua.ivml.mint.xml.util.XmlValueUtils;
import nu.xom.Document;
import nu.xom.Node;

public class DescriptiveData extends HashMap<String,Object> {

	protected final Logger log = LogManager.getLogger();

	public DescriptiveData(Document doc) {
		// multiliterals
		for (String label : Edm.getXpaths().keySet()) {
			String path = Edm.getXpaths().get(label);
			
			MultiLiteralOrResource.parseItem(path, doc).ifPresent(
					mlor->put(label, mlor));
		}

		// just uris
		for (String label : Edm.getURIpaths().keySet()) {
			String path = Edm.getURIpaths().get(label);
			for( Node n: XmlValueUtils.nodeIterable(doc, path)) {
				XmlValueUtils.getAttValIgnoreNamespace(n, "resource")
					.ifPresent( uri -> put( label, uri ));
			}
		}
		
		
		JacksonBuilder dates = JacksonBuilder.arr();
		for( String label: Edm.getDatepaths().keySet()) {
			String path = Edm.getDatepaths().get(label);
			for( Node n:XmlValueUtils.nodeIterable(doc, path)) {
				Optional<String> date = XmlValueUtils.getAttValIgnoreNamespace(n, "resource");
				if( !date.isPresent())
					date = XmlValueUtils.getNonEmptyValFromElement(n);
				date.ifPresent( d -> dates.appendObj().add( "free", d));
			}
		}
		
		// sameAs
		JacksonBuilder sameAsUris = JacksonBuilder.arr();
		String sameAsPattern = "//*[local-name()='sameAs' and namespace-uri()='http://www.w3.org/2002/07/owl#']";
		for( String uri: XmlValueUtils.valueIterable(doc, sameAsPattern ))
			sameAsUris.append(uri);
		
		if( sameAsUris.getArray().size() > 0 )
			put( "sameAs", sameAsUris.getArray());
		
		put("rdfType", "http://www.europeana.eu/schemas/edm/ProvidedCHO");
		put("metadataRights", "http://creativecommons.org/publicdomain/zero/1.0/");
		put( "dates", dates.getArray());
	}

}
