package gr.ntua.ivml.mint.util;

import java.io.IOException;
import java.util.Optional;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

public class OriginProjectFilter implements Filter {


	public void destroy() {

	}

	static interface ProjectGetter {
		public Optional<String> getProject( HttpServletRequest request);
	}
	
	public static ProjectGetter projectGetter = null;
	
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) 
		throws IOException, ServletException {

		if( projectGetter != null ) 
			projectGetter.getProject( (HttpServletRequest) request ).ifPresent( project -> request.setAttribute("projectOrigin",  project ));
		filterChain.doFilter(request, response); 
	}

	public void init(FilterConfig filterConfig) throws ServletException { 
		// TODO Auto-generated method stub String encodingParam = filterConfig.getInitParameter("encoding"); if (encodingParam != null) { encoding = encodingParam; }
	} 

	public static String getAppname( HttpServletRequest request ) {
		return request.getContextPath().substring(1);
	}
	
} 