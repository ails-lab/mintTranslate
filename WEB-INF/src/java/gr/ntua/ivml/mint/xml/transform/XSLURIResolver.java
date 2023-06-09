package gr.ntua.ivml.mint.xml.transform;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;
import javax.xml.transform.URIResolver;
import javax.xml.transform.stream.StreamSource;

import gr.ntua.ivml.mint.util.Config;

public class XSLURIResolver implements URIResolver {

	@Override
	public Source resolve(String href, String base) throws TransformerException {
		String path = Config.getXSLPath(href);
		StreamSource source = null;
		try {
			source = new StreamSource(new FileInputStream(path));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		return source;
	}

}
