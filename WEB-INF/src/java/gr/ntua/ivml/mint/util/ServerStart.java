package gr.ntua.ivml.mint.util;


import java.io.File;
import java.util.Set;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.log4j.Logger;

import com.mchange.v2.c3p0.C3P0Registry;
import com.mchange.v2.c3p0.PooledDataSource;

import gr.ntua.ivml.mint.actions.ScriptTester;
import gr.ntua.ivml.mint.concurrent.TransformHotdir;
import gr.ntua.ivml.mint.db.DB;

public class ServerStart implements ServletContextListener {

	Logger log = Logger.getLogger(ServerStart.class);
	
	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		// TODO Auto-generated method stub
		for( PooledDataSource pds: (Set<PooledDataSource>) C3P0Registry.allPooledDataSources()) {
			try {
				pds.close();
			} catch( Exception e ) {
				log.debug( "Error closing PooledDataSource.", e );
			}
		}
	}

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		Config.updateFromMeta();
		Config.readProps();

		DB.getSession().beginTransaction();
		try {
			DB.cleanup();
			// TODO Auto-generated method stub
			Config.setContext( arg0.getServletContext());
			// read live properties from database
			// load this class
			new ScriptTester();

			// maybe there is a solr lock file
			String solrDir = Config.get( "solr.directory");
			if( !StringUtils.empty( solrDir )) {
				File dir = new File( solrDir );
				File lock = new File( dir, "index/write.lock");
				if( lock.exists() && lock.canWrite()) {
					log.info( "Solr lock removed!!");
					lock.delete();
				}
			}
			// create the hotdir and sync it
			TransformHotdir hot = DB.getHotdir();
			if( hot != null ) hot.sync();
			
		} finally {
			DB.closeSession();
			DB.closeStatelessSession();
		}
	}

}
