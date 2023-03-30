package gr.ntua.ivml.mint;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Item;
import gr.ntua.ivml.mint.persistent.Organization;
import gr.ntua.ivml.mint.persistent.User;
import nu.xom.Attribute;
import nu.xom.Element;
import nu.xom.Node;
import nu.xom.Nodes;

public class OperationDirectPublication extends BasePublication {

	public OperationDirectPublication(Organization org) {
		super(org);
	}

	public class XpathInfo {

		public String localName;
		public String namespaceUri;
		public boolean multipleValues;
		public boolean languageAware;

		public XpathInfo(String localName, String namespaceUri, boolean multiple, boolean languageAware) {
			this.localName = localName;
			this.namespaceUri = namespaceUri;
			this.multipleValues = multiple;
			this.languageAware = languageAware;
		}
	}

	public class XpathValue {

		public String field;
		public String value;
		public boolean isArray;
		public String language;

		public XpathValue(String field, String value, boolean isArray, String language) {
			if (field.equals("language"))
				this.field = "objectLanguage";
			else
				this.field = field;
			this.value = value;
			this.isArray = isArray;
			this.language = language;
		}
	}

	List<XpathInfo> xpaths = Arrays.asList(new XpathInfo("title", "http://purl.org/dc/elements/1.1/", false, true),
			new XpathInfo("description", "http://purl.org/dc/elements/1.1/", false, true),
			new XpathInfo("publisher", "http://purl.org/dc/elements/1.1/", true, true),
			new XpathInfo("source", "http://purl.org/dc/elements/1.1/", true, true),
			new XpathInfo("alternative", "http://purl.org/dc/terms/", true, true),
			new XpathInfo("created", "http://purl.org/dc/terms/", false, true),
			new XpathInfo("issued", "http://purl.org/dc/terms/", false, true),
			new XpathInfo("format", "http://purl.org/dc/elements/1.1/", true, true),
			new XpathInfo("extent", "http://purl.org/dc/terms/", true, true),
			new XpathInfo("medium", "http://purl.org/dc/terms/", true, true),
			new XpathInfo("provenance", "http://purl.org/dc/terms/", true, true),
			new XpathInfo("type", "http://www.europeana.eu/schemas/edm/", false, false),
			// new Xpath("dataOwner", "http://purl.org/dc/elements/1.1/", true),
			new XpathInfo("language", "http://purl.org/dc/elements/1.1/", false, false),
			new XpathInfo("identifier", "http://purl.org/dc/terms/", false, false),
			new XpathInfo("relation", "http://purl.org/dc/terms/", false, false));

	@Override
	public boolean publish(Dataset ds, User publisher) {
		if (!isDirectlyPublishable(ds))
			return false;
		List<Item> items = ds.getItems(0, 10);
		for (Item item : items) {
			List<XpathValue> xpathValues = parseXpaths(item);
			System.out.println(xpathValues);
			OpDirectObject opDirectObject = new OpDirectObject();
			for (XpathValue xpathValue : xpathValues) {
				opDirectObject.addField(xpathValue.field, xpathValue.language, xpathValue.value, xpathValue.isArray);
			}
			String mapAsJson;
			try {
				mapAsJson = new ObjectMapper().setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
						.writeValueAsString(opDirectObject);
				sendToOperationDirect(mapAsJson);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return true;
	}

	public void sendToOperationDirect(String json) throws ClientProtocolException, IOException {
		String url = "https://europeana-direct.semantika.eu/EuropeanaDirect/api/object";
		HttpClient client = HttpClientBuilder.create().build();
		HttpPost post = new HttpPost(url);
		post.addHeader("Content-Type", "application/json");
		post.addHeader("Accept", "application/json");
		post.setEntity(new StringEntity(json, "UTF-8"));
		HttpResponse response;
		response = client.execute(post);
//		System.out.println("Response Code : " + response.getStatusLine().getStatusCode());
//		BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
//		String line = "";
//		while ((line = rd.readLine()) != null) {
//			result.append(line);
//		}
	}

	public List<XpathValue> parseXpaths(Item item) {
		List<XpathValue> res = new ArrayList<XpathValue>();
		for (XpathInfo xpath : xpaths) {
			String query = "//*[local-name()='" + xpath.localName + "' and namespace-uri()='" + xpath.namespaceUri
					+ "']";
			Nodes nodes = item.getDocument().query(query);
			log.debug("Nodes founds after getValue are " + nodes.size());
			if (nodes.size() == 0)
				continue;
			for (int i = 0; i < nodes.size(); i++) {
				Node node = nodes.get(i);
				String value = node.getValue();
				Attribute lang = null;
				if (node instanceof Element) {
					lang = ((Element) node).getAttribute("lang", "http://www.w3.org/XML/1998/namespace");
				}
				if (value == null || value.isEmpty())
					continue;
				if (!xpath.languageAware) {
					res.add(new XpathValue(xpath.localName, value, xpath.multipleValues, null));
					continue;
				}
				if (lang == null)
					// Undetermined language in ISO 639-3 language codes
					res.add(new XpathValue(xpath.localName, value, xpath.multipleValues, "und"));
				else
					res.add(new XpathValue(xpath.localName, value, xpath.multipleValues, lang.getValue()));
			}
		}
		return res;
	}

}
