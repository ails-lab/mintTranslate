package gr.ntua.ivml.mint.api.handlers;

import java.util.HashMap;
import java.util.Map;

import gr.ntua.ivml.mint.api.RouterServlet.Handler;

public class Setup {
	public static Map<String, Handler> handlers() {
		Map<String, Handler> result = new HashMap<>();
		result.putAll(AnnotationSetHandlers.handlers());
		// and all the other handler that will come up
		// maybe there will be a chance to be generic somewhere
		return result;
	}
}
