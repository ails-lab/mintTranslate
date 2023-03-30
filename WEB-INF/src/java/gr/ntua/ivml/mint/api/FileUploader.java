package gr.ntua.ivml.mint.api;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import gr.ntua.ivml.mint.util.ApplyI;

public class FileUploader {
	
	static Map idMap = new HashMap();

	public static void installStreamHandler( UUID id, ApplyI<InputStream> handler ) {
		idMap.put( id, handler);
		}
	
	public static void executeStreamHandler( UUID id, InputStream s ) throws Exception {
		if (!idMap.containsKey(id))
			throw new Exception();
		((ApplyI<InputStream>) idMap.get(id)).apply(s);
	}

}
