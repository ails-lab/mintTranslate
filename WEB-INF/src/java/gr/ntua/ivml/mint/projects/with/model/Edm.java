package gr.ntua.ivml.mint.projects.with.model;

import java.util.HashMap;
import java.util.Map;


public class Edm {

	private static final Map<String, String> xpaths;
	// private static final Map<String, String[]> xpaths;

	static {
		xpaths = new HashMap<String, String>();

		xpaths.put("dccontributor",
				"//*[local-name()='contributor' and namespace-uri()='http://purl.org/dc/elements/1.1/']");
		xpaths.put("dccreator", "//*[local-name()='creator' and namespace-uri()='http://purl.org/dc/elements/1.1/']");
		xpaths.put("label", "//*[local-name()='title' and namespace-uri()='http://purl.org/dc/elements/1.1/']");
		xpaths.put("description",
				"//*[local-name()='description' and namespace-uri()='http://purl.org/dc/elements/1.1/']");
		xpaths.put("keywords", "//*[local-name()='subject' and namespace-uri()='http://purl.org/dc/elements/1.1/']");
		xpaths.put("dcformat", "//*[local-name()='format' and namespace-uri()='http://purl.org/dc/elements/1.1/']");
		xpaths.put("dcidentifier",
				"//*[local-name()='identifier' and namespace-uri()='http://purl.org/dc/elements/1.1/']");
		xpaths.put("dclanguage", "//*[local-name()='language' and namespace-uri()='http://purl.org/dc/elements/1.1/']");
		xpaths.put("dcrights", "//*[local-name()='rights' and namespace-uri()='http://purl.org/dc/elements/1.1/']");
		xpaths.put("dctype", "//*[local-name()='type' and namespace-uri()='http://purl.org/dc/elements/1.1/']");
		xpaths.put("dccoverage", "//*[local-name()='coverage' and namespace-uri()='http://purl.org/dc/elements/1.1/']");

		xpaths.put("isRelatedTo",
				"//*[local-name()='isRelatedTo' and namespace-uri()='http://www.europeana.eu/schemas/edm/']");
		xpaths.put("dctermsspatial", "//*[local-name()='spatial' and namespace-uri()='http://purl.org/dc/terms/']");

		xpaths.put("altLabels", "//*[local-name()='alternative' and namespace-uri()='http://purl.org/dc/terms/']");
		xpaths.put("dcspatial", "//*[local-name()='spatial' and namespace-uri()='http://purl.org/dc/terms/']");
		xpaths.put("dctermsmedium", "//*[local-name()='medium' and namespace-uri()='http://purl.org/dc/terms/']");
	}

	private static final Map<String, String> provenancepaths;

	static {
		provenancepaths = new HashMap<String, String>();
		provenancepaths.put("dataProvider",
				"//*[local-name()='dataProvider' and namespace-uri()='http://www.europeana.eu/schemas/edm/']");
		provenancepaths.put("provider",
				"//*[local-name()='provider' and namespace-uri()='http://www.europeana.eu/schemas/edm/']");
		provenancepaths.put("edmtype",
				"//*[local-name()='type' and namespace-uri()='http://www.europeana.eu/schemas/edm/']");
		provenancepaths.put("edmrights",
				"//*[local-name()='rights' and namespace-uri()='http://www.europeana.eu/schemas/edm/']/@*[local-name()='resource']");
		provenancepaths.put("isShownAt",
				"//*[local-name()='isShownAt' and namespace-uri()='http://www.europeana.eu/schemas/edm/']/@*[local-name()='resource']");
		provenancepaths.put("ProvidedCHO",
				"//*[local-name()='ProvidedCHO']/@*[local-name()='resource']");
		provenancepaths.put("dcidentifier",
				"//*[local-name()='identifier' and namespace-uri()='http://purl.org/dc/elements/1.1/']");
	}

	private static final Map<String, String> mediapaths;

	static {
		mediapaths = new HashMap<String, String>();
		mediapaths.put("isShownBy",
				"//*[local-name()='isShownBy' and namespace-uri()='http://www.europeana.eu/schemas/edm/']/@*[local-name()='resource']");
		mediapaths.put("hasView",
				"//*[local-name()='hasView' and namespace-uri()='http://www.europeana.eu/schemas/edm/']/@*[local-name()='resource']");
		mediapaths.put("object",
				"//*[local-name()='object' and namespace-uri()='http://www.europeana.eu/schemas/edm/']/@*[local-name()='resource']");
	}

	public static Map<String, String> getXpaths() {
		return xpaths;
	}

	public static Map<String, String> getProvenancepaths() {
		return provenancepaths;
	}

	public static Map<String, String> getMediapaths() {
		return mediapaths;
	}

	public static Map<String, String> getDatepaths() {
		return datepaths;
	}

	private static final Map<String, String> datepaths;

	static {
		datepaths = new HashMap<String, String>();
		datepaths.put("dcdate", "//*[local-name()='date' and namespace-uri()='http://purl.org/dc/elements/1.1/']");
		datepaths.put("dccreated", "//*[local-name()='created' and namespace-uri()='http://purl.org/dc/terms/']");
		datepaths.put("dcissued", "//*[local-name()='issued' and namespace-uri()='http://purl.org/dc/terms/']");
		// datepaths.put("dctemporal", "//*[local-name()='temporal' and
		// namespace-uri()='http://purl.org/dc/elements/1.1/']");

	}

	public static Map<String, String> getURIpaths() {
		return uripaths;
	}

	private static final Map<String, String> uripaths;

	static {
		uripaths = new HashMap<String, String>();
		uripaths.put("isShownBy",
				"//*[local-name()='isShownBy' and namespace-uri()='http://www.europeana.eu/schemas/edm/']");
		uripaths.put("isShownAt",
				"//*[local-name()='isShownAt' and namespace-uri()='http://www.europeana.eu/schemas/edm/']");
	}

	
}
