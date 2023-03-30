package gr.ntua.ivml.mint.projects.fashion;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Date;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import gr.ntua.ivml.mint.Publication;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Item;
import gr.ntua.ivml.mint.persistent.PublicationRecord;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.persistent.XpathHolder;
import gr.ntua.ivml.mint.util.ApplyI;
import gr.ntua.ivml.mint.util.Config;
import gr.ntua.ivml.mint.util.Counter;
import gr.ntua.ivml.mint.util.Jackson;
import gr.ntua.ivml.mint.util.StringUtils;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

public class FashionPortalPublication  {
	
	Logger log = Publication.log;
	String portalUrl = "";
	private DefaultHttpClient httpclient;

	public FashionPortalPublication() {
		portalUrl = Config.get( "fashion.portal.url");
		PoolingClientConnectionManager cm = new PoolingClientConnectionManager();
		cm.setDefaultMaxPerRoute(8);
		httpclient = new DefaultHttpClient(cm);
	}

	
	/**
	 * Send given dataset to the staging portal.
	 * @param ds
	 * @return
	 */
	public void portalPublish( final Dataset ds, User publisher ) {
		final Dataset originalDataset = ds.getOrigin();
		
		log.info( "Portal publish " + originalDataset.getName() );

		originalDataset.logEvent("Sending to Portal.", "Use schema " + ds.getSchema().getName() + " actual Dataset #" + ds.getDbID() );
		
		// get or make a publication record
		
		PublicationRecord pr;
		pr = DB.getPublicationRecordDAO().getByPublishedDatasetTarget(ds, "fashion.portal").orElse(null);
		if( pr == null ) {
			log.error( "Publication Record should exist already...");
			return;
		}
		
		final Counter itemCounter = new Counter(0);
		final Counter problemCounter = new Counter(0);
		HttpContext httpContext = new BasicHttpContext();
		
		String report = null;
		try {
		    report = createReport( originalDataset, httpContext );
			final ObjectNode template = portalTemplate( ds );
			template.put( "reportId",  report );
			
			
			ApplyI<Item> itemSender = new ApplyI<Item>() {
				@Override
				public void apply(Item item) throws Exception {
						String json = itemPortalJson(item, template);
						if( insertRecord( json )) {
							itemCounter.inc();
						} else {
							problemCounter.inc();
						}
				}
			};
			
			ds.processAllValidItems(itemSender, false);
			
			// put the report in the log message!
			ds.logEvent( "Finished publishing. " + itemCounter.get() + " items send.", 
					problemCounter.get()>0
						? ( "There were problems with " + problemCounter.get() + " items.")
					    : "" );
			
			pr.setPublishedItemCount(itemCounter.get());
			pr.setEndDate(new Date());
			pr.setStatus(Dataset.PUBLICATION_OK);
			DB.commit();
		} catch( Exception e ) {
			log.warn( "Item publication went wrong", e );
			ds.logEvent( "Publication went wrong. ", e.getMessage());
			pr.setStatus(Dataset.PUBLICATION_FAILED);
			pr.setEndDate(new Date());
			pr.setReport(e.getMessage());
			pr.setPublishedItemCount(itemCounter.get());
			DB.commit();
		} finally {
			if( report != null ) {
				try {
					closeReport( report, httpContext );
				} catch( Exception e ) {
					log.error( "Report closing problem", e );
				}
			}
		}
		
	}

	public boolean portalUnpublish( Dataset ds  ) {
		HttpContext httpContext = new BasicHttpContext();

		try {
			// not sure do we need the original or the published dataset id here
			simpleGet( httpContext, "api/deleteByDatasetId", "datasetId", ds.getOrigin().getDbID().toString());
			ds.logEvent("Unpublished from Portal.", "Dataset #"+ds.getDbID().toString() + " removed from Portal.");
			return true;
		} catch( Exception e ) {
			log.error( "Error during unpublish", e );
			ds.logEvent("Error during unpublish", e.toString() );
			return false;
		} finally {
			DB.commit();
		}
	}
	
	private String itemPortalJson( Item item, ObjectNode template ) {
		
		
		
		ObjectNode on = Jackson.om().createObjectNode();
		on.setAll( template );
		on
			.put( "record", item.getXml())
			.put( "hash", StringUtils.sha1Utf8( item.getXml()) )
			.put( "recordId",  item.getDbID())
			.put( "sourceItemId", item.getSourceItem().getDbID())
			.put( "datestamp", System.currentTimeMillis());
				
			
		String res = on.toString();
		
		return res;
	}
	
	/**
	 * Accumulate dataset dependend attributes in a JSONObject
	 * @param ds
	 * @return
	 */
	private ObjectNode portalTemplate( Dataset ds) {
		XpathHolder xmlRoot = ds.getRootHolder().getChildren().get(0);
		ObjectNode on = Jackson.om().createObjectNode();
		on
			.put( "datasetId", ds.getDbID())
			.put( "mintOrgId", ds.getOrganization().getDbID())
			.put( "userId", ds.getCreator().getDbID())
			.put( "sourceDatasetId", ds.getOrigin().getDbID());
		on.set( "namespace", Jackson.om().createObjectNode()
				.put( "prefix", xmlRoot.getUriPrefix())
				.put( "uri", xmlRoot.getUri())
				);
		on.set( "sets", Jackson.om().createArrayNode()
				.add( ds.getDbID() )
				.add( ds.getOrganization().getDbID())
				.add( ds.getCreator().getDbID())
				);

		return on;
	}
	
	
	/**
	 * Sends the given String to the portal as one record.
	 * @param item
	 * @return
	 */
	public boolean insertRecord( String item ) {
		HttpPost post = null;
	
		try {
			post = new HttpPost( portalUrl + "api/insertBlocking" );
			StringEntity ent = new StringEntity( item, "UTF-8");
			
			ent.setContentType("application/json");
			post.setEntity(ent);
			httpclient.execute(post);
			return true;
		} catch( Exception e ) {
			log.error( "Exception during insert of Record", e );
			return false;
		} finally {
			if( post != null )
				post.releaseConnection();
		}
	}
	
	public String createReport(Dataset ds, HttpContext httpContext) throws Exception {
		try {
			String resp = simpleGet( httpContext, "api/createReport", "datasetId", ds.getDbID().toString(), "orgId", ds.getOrganization().getDbID()+"" );
			JSONObject res = (JSONObject) JSONValue.parse( resp );
			return  res.get( "reportId" ).toString();
		} catch( Exception e ) {
			log.error( "Error during create report!", e );
			throw e;
		}
	}
 	
	public void closeReport( String reportId, HttpContext httpContext  ) throws Exception {
		try {
			String resp = simpleGet( httpContext, "api/closeReport", "reportId", reportId );
		} catch( Exception e ) {
			log.error( "Error during close report!", e );
			throw e;
		}
	}
	
	private String simpleGet( HttpContext httpContext, String verb, String... params ) throws Exception {
		HttpGet get = new HttpGet(portalUrl+"/"+verb );
		try {
			URIBuilder builder = new URIBuilder( get.getURI());
			if( params != null ) {
				for( int i=0; i<params.length; ) {
					String key = params[i];
					i++;
					if( i<params.length ) {
						String val = params[i];
						builder.addParameter(key, val);
					}
					i++;
				}
			}
			URI uri = builder.build();
			get.setURI(uri);
			HttpResponse response = httpclient.execute(get, httpContext );
			BufferedReader rd = new BufferedReader
					(new InputStreamReader(response.getEntity().getContent()));
			String resp = org.apache.commons.io.IOUtils.toString(rd);	
			return resp;
		} finally {
			get.releaseConnection();
		}
	}
}
