package gr.ntua.ivml.mint.util;

import java.sql.Connection;
import java.sql.ResultSet;

import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.ConnectionTester;

public class ConnectionCheckoutLog implements ConnectionTester {
	public static final Logger log = Logger.getLogger( ConnectionCheckoutLog.class);
	
	@Override
	public int activeCheckConnection(Connection conn ) {
		log.info( "Checkout occured");
		if( log.isDebugEnabled()) {
			Exception e= new Exception();
			e.fillInStackTrace();
			log.debug( "Trace\n" + StringUtils.filteredStackTrace(e, "gr.ntua.ivml.mint"));
		}
		ResultSet rs = null;
		try {
			rs = conn.createStatement().executeQuery("select 1");
			if( rs.next()) 
				return CONNECTION_IS_OKAY;
			else
				return CONNECTION_IS_INVALID;
		} catch( Exception e ) {
			return CONNECTION_IS_INVALID;
		} finally {
			try {
				if( rs != null ) rs.close();
			} catch( Exception e2 ) {}
		}
	}

	@Override
	public int statusOnException(Connection arg0, Throwable arg1) {
		return CONNECTION_IS_INVALID;
	}

}
