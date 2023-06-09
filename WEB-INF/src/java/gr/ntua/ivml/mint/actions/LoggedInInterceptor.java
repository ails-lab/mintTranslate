package gr.ntua.ivml.mint.actions;

import gr.ntua.ivml.mint.Custom;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.util.Config;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.log4j.Logger;
import org.apache.struts2.StrutsStatics;

import com.opensymphony.xwork2.ActionInvocation;
import com.opensymphony.xwork2.interceptor.AbstractInterceptor;

/**
 * The LoggedInInterceptor should be part of the stack for pages
 * that are only accessible for users that are logged in!
 * It redirects to the "logon" result which should allow
 * you to log in. It should not however use this Interceptor (loop!)
 * 
 * @author arne
 *
 */
public class LoggedInInterceptor extends AbstractInterceptor {
	public static final Logger log = Logger.getLogger( LoggedInInterceptor.class );
	@Override
	public String intercept(ActionInvocation invocation ) throws Exception {
		GeneralAction ga = (GeneralAction) invocation.getAction();
		log.debug( "Name " + invocation.getInvocationContext().getName());
		HttpServletRequest request = (HttpServletRequest) invocation.getInvocationContext().get(StrutsStatics.HTTP_REQUEST);
		HttpSession httpSession = request.getSession();
		// allow for custom login schema
		Custom.login( request );
		ga.setSessionId(httpSession.getId());
		Map<String, Object> s = invocation.getInvocationContext().getSession();
		User u = (User) s.get( "user" );
		if( u == null ){
			 if( Config.get( "autoAdminLogin") != null ) {
				 u = DB.getUserDAO().getByLogin("admin");
			 } else {
				 return "logon";				 
			 }
		} else {
		
			// not good enough, not deep: DB.getSession().update(u);
			u = DB.getUserDAO().getById(u.getDbID(), false);
		}
		ga.setUser( u );
		s.put( "user", u);
		String userData = "User: " + u.toString() + "->";
		
		String result =  invocation.invoke();
		// make sure the user is loaded and displayed in tomcat session list
		log.info( userData + result );
		
		return result;
	}
}
