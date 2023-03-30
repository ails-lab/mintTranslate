package gr.ntua.ivml.mint.api;

import static gr.ntua.ivml.mint.api.RequestHandler.errJson;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import gr.ntua.ivml.mint.api.handlers.Setup;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.User;

import javax.servlet.annotation.MultipartConfig;

/**
 * Router class. Depending on the request type and URL of the resource,
 * it will call the correspondent method to deal with the request.
 *
 * There are 3 RequestHandler classes that may be called:
 * Request at /api/*        -> ./RequestHandler
 * Request at /api/fashion  -> /projects/fashion/RequestHandler
 * Request at /api/modemuze -> /projects/modemuze/RequestHandler
 *
 * This class uses method reflection to invoke methods. Depending
 * on the name of the method and the request type (GET, POST etc)
 * we reflect the correspondent method and execute it from this class.
 */
@MultipartConfig(
		fileSizeThreshold   = 1024 * 1024 * 1,  // 1 MB
		maxFileSize         = 1024 * 1024 * 10, // 10 MB
		maxRequestSize      = 1024 * 1024 * 15 // 15 MB
//		location            = "D:/Uploads"
)
public class RouterServlet extends HttpServlet {
	
	public interface Handler {
		public void handle( HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException;
		
	}
	
	public static Logger log = Logger.getLogger(RouterServlet.class);
	public static Map<String, Class<?>> projectHandlers = new HashMap<>();
	public static Map<String, Handler> overrideHandlers = new HashMap<>();
	
	static {
		// add the request handlers from projects here
		projectHandlers.put( "fashion", gr.ntua.ivml.mint.projects.fashion.RequestHandler.class );
		projectHandlers.put( "modemuze", gr.ntua.ivml.mint.projects.modemuze.RequestHandler.class );
		projectHandlers.put( "photo", gr.ntua.ivml.mint.projects.photo.RequestHandler.class );
		projectHandlers.put( "photoxx", gr.ntua.ivml.mint.projects.photoxx.RequestHandler.class );
		projectHandlers.put( "efgxx", gr.ntua.ivml.mint.projects.efgxx.RequestHandler.class );
		projectHandlers.put( "euscreen", gr.ntua.ivml.mint.projects.euscreen.RequestHandler.class );
		projectHandlers.put( "fashionxx", gr.ntua.ivml.mint.projects.fashionxx.RequestHandler.class );
		projectHandlers.put( "direct", gr.ntua.ivml.mint.projects.direct.RequestHandler.class );
		
		// install a hashmap of handlers from api.handlers directory
		overrideHandlers.putAll( Setup.handlers());
	}
	
	public enum RequestMethod {
		GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS, TRACE;

		@Override
		public String toString() {
			return StringUtils.capitalize(super.toString().toLowerCase());
		}
	}

	public void init() throws ServletException {
		// Do required initialization

	}
	
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doRequest(RequestMethod.POST, request, response);
	}
	
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doRequest(RequestMethod.GET, request, response);
	}

	public void doRequest(RequestMethod requestMethod, HttpServletRequest request, HttpServletResponse response) {
		String path = request.getPathInfo();
		String[] parts = path.split("/");
		String part;
		Class<?> handler = RequestHandler.class;
		
		// TODO: Check the domain against whitelist
		response.addHeader("Access-Control-Allow-Origin", request.getHeader( "Origin"));
		response.addHeader("Access-Control-Allow-Credentials", "true" );
		response.addHeader("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, OPTIONS" );
		
		User u = (User) request.getSession().getAttribute("user");
		if( u!=null )
			request.setAttribute("user", DB.getUserDAO().getById( u.getDbID(), false ));
		
		String token = request.getHeader("Authorization");
		if( !StringUtils.isEmpty(token)) {
			try {
				User user2 = TokenManager.getUserFromToken(token);

				if (user2 != null) 
					request.setAttribute("user", user2 );

			} catch( Exception e2 ) {
				log.error("", e2);
			}
		}

		// check for installed handlers, longer prefix matches override shorter ones. 
		for( int pathPrefixLength = parts.length; pathPrefixLength>0; pathPrefixLength-- ) {
			String prefix = String.join( "/", Arrays.copyOfRange(parts, 0, pathPrefixLength));
			
			// something should be there
			if( ! prefix.startsWith("/")) continue;	
			
			Handler override =  overrideHandlers.get( requestMethod.name() + " " + prefix );
			if( override != null ) {
				try {
					log.info( "Override called " + requestMethod.name() + " " + path );
					override.handle( request, response);
				} catch( Exception e ) {
					log.error( "Override method error", e );
				} 
				return;
			}
		}
		
		if( requestMethod.equals( RequestMethod.OPTIONS)) {
			response.setStatus(HttpServletResponse.SC_OK);
			return;
		}
			
		try {
			if (parts.length >= 2) {
				part = parts[1];
				if( projectHandlers.containsKey(parts[1])) {
					handler = projectHandlers.get( parts[1]);
					part = parts[2];
				}
			} else
				return;

			Method method = handler.getMethod(part + requestMethod,
					new Class[] { HttpServletRequest.class, HttpServletResponse.class });
			
			method.invoke(null, request, response);
		} catch (Exception e) {
			log.error( "Could not dispatch '" + String.join(", ", parts ) +"'", e );
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
		}
	}

	public void destroy() {
		// do nothing.
	}

}
