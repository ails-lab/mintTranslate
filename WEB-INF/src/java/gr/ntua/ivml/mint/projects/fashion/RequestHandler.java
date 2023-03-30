package gr.ntua.ivml.mint.projects.fashion;

import static gr.ntua.ivml.mint.api.RequestHandler.errJson;
import static gr.ntua.ivml.mint.api.RequestHandler.getPathInt;
import static gr.ntua.ivml.mint.api.RequestHandler.okJson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import gr.ntua.ivml.mint.concurrent.Queues;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.PublicationRecord;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.persistent.XmlSchema;
import gr.ntua.ivml.mint.projects.fashionxx.XxEnrich;
import gr.ntua.ivml.mint.util.Config;
import gr.ntua.ivml.mint.util.PublicationHelper;

/**
 * Static messages routed here by the router servlet, starting with /api/fashion/...
 * @author stabenau
 *
 */
public class RequestHandler {
	
	public static final Logger log=Logger.getLogger(RequestHandler.class);
	
	// get the dataset and check if the user is allowed to do anything 
	// and if the dataset belongs to fashion.
	// if dataset is null, response already contains errcode and mesg
	private static Optional<Dataset> accessAllowed( HttpServletRequest request, HttpServletResponse response ) throws Exception {
		int datasetId = getPathInt( request, 3 );
		Dataset ds = DB.getDatasetDAO().getById((long) datasetId, false ); 
		User u = (User) request.getAttribute("user");
		if (u == null) {
			errJson("No User logged in", response);
			return Optional.empty();
		}
		if( ! Arrays.asList( ds.getOrigin().getProjectNames()).contains("fashion")) {
			errJson("Dataset not in fashion", response);
			return Optional.empty();			
		}
		
		
		// this is to simplify the rights system, just read,write,own and none is the goal.
		if( ! ( u.can( "change data", ds.getOrganization() ) ||
				u.sharesProject(ds.getOrigin()))) {
			errJson("User has no rights", response);
			return Optional.empty();						
		}
		
		return Optional.of(ds);
	}
	
	// TODO: These should be post requests, to be done much later
	
	// this one should be quick enough to not go to Queue
	public static void europeanaUnpublishGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			Optional<Dataset> oDs = accessAllowed( request, response );
			if( !oDs.isPresent()) return;
			// remove the Publication record, we dont care if its successful or not
			// In theory we should check if its running though
			Optional<PublicationRecord> oPr = DB.getPublicationRecordDAO().findByOriginalDataset( oDs.get().getOrigin()) 
					.stream()
					.filter( pr -> pr.getTarget().equals( "fashion.europeana"))
					.findAny();
			if( oPr.isPresent() ) {
				// remove from OAI
				PublicationHelper.oaiUnpublish(oDs.get(), Config.get( "fashion.queue.routingKey"), 
						Config.get( "fashion.oai.server"), Integer.parseInt( Config.get( "fashion.oai.port" )));
				DB.getPublicationRecordDAO().makeTransient(oPr.get());
			} else {
				errJson( "No Europeana publication exists for #" + oDs.get().getDbID(), response);
				return;
			}
			okJson( "msg", "Europenana unpublish succeeded.", response );
		} catch( Exception e ) {
			log.error( "", e );
			errJson( "Something went wrong: '" + e.getMessage() + "'", response );
			return ;
		}
	}

	// maybe the same as unpublish
	public static void europeanaCleanGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		europeanaUnpublishGet(request, response);
	}
	
	// the dataset needs to be FP (portal.schema) and wants to be mapped and enriched (2 steps)
	// this takes a parameter annotations=, to prepare with 'wasGeneratedBy' tags
	public static void europeanaPrepareGet( HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			final Optional<Dataset> oDs = accessAllowed( request, response );
			if( !oDs.isPresent()) return;
			if( ! PublicationHelper.isSchemaAndHasValidItems(oDs.get(), Config.get( "fashion.portal.schema"))) {
				errJson( "Dataset cannot be prepared. No valid items of required schema found.", response );
				return;
			}
			
			// if it already has children, we cannot do this
			if( ! oDs.get().getDirectlyDerived().isEmpty()) {
				errJson( "Dataset cannot be prepared. Cannot have already annotations or transformations.", response );
				return;
			}
			
			boolean shouldHaveSoftwareAgentMarks =  Arrays.asList( oDs.get().getOrigin().getProjectNames()).contains("fashionxx");
			final boolean generateAnnotations =  request.getParameterMap().containsKey( "annotations") || shouldHaveSoftwareAgentMarks;
			
			FashionEdmPublisher.europeanaPrepareAsync(oDs.get(), generateAnnotations);
			okJson( "msg", "Europenana prepare for publish queued.", response );
		} catch( Exception e ) {
			log.error( "", e);
			errJson( e.getMessage(), response );
		}
	}
	
	// the dataset needs default enrichment as transformation
	public static void defaultEnrichmentGet( HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			final Optional<Dataset> oDs = accessAllowed( request, response );
			if( !oDs.isPresent()) return;
			List<XmlSchema> lxs = new ArrayList<>();
			lxs.addAll( XmlSchema.getSchemasFromConfig("fashion.oai.schema"));
			lxs.addAll( XmlSchema.getSchemasFromConfig("fashionxx.oai.schema"));
			if( ! PublicationHelper.isSchemaAndHasValidItems(oDs.get(),  lxs)) {
				errJson( "Dataset cannot be prepared. No valid items of required schema found.", response );
				return;
			}
			
			// if it already has children, we cannot do this
			if( ! oDs.get().getDirectlyDerived().isEmpty()) {
				errJson( "Dataset cannot be prepared. Cannot have already annotations or transformations.", response );
				return;
			}
			final boolean generateAnnotations =  request.getParameterMap().containsKey( "annotations");
			
			FashionEdmPublisher.defaultEnrichAsync(oDs.get(), generateAnnotations);
			okJson( "msg", "Default Enrichment successfully queued.", response );
		} catch( Exception e ) {
			log.error( "", e);
			errJson( e.getMessage(), response );
		}
	}
	
	public static void xxEnrichmentGet( HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			final Optional<Dataset> oDs = accessAllowed( request, response );
			if( !oDs.isPresent()) return;
			List<XmlSchema> lxs = new ArrayList<>();
			lxs.addAll( XmlSchema.getSchemasFromConfig("fashion.oai.schema"));
			lxs.addAll( XmlSchema.getSchemasFromConfig("fashionxx.oai.schema"));
			if( ! PublicationHelper.isSchemaAndHasValidItems(oDs.get(),  lxs)) {
				errJson( "Dataset cannot be prepared. No valid items of required schema found.", response );
				return;
			}
			
			// if it already has children, we cannot do this
			if( ! oDs.get().getDirectlyDerived().isEmpty()) {
				errJson( "Dataset cannot be prepared. Cannot have already annotations or transformations.", response );
				return;
			}
			XxEnrich.xxEnricAsync(oDs.get());
			okJson( "msg", "XX Enrichment and Tag successfully queued.", response );
		} catch( Exception e ) {
			log.error( "", e);
			errJson( e.getMessage(), response );
		}
	}
	
	
	public static void europeanaPublishGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			Optional<Dataset> oDs = accessAllowed( request, response );
			final User publisher = (User) request.getAttribute("user");
			if( !oDs.isPresent()) return;
			final Dataset ds = oDs.get();
			
			Optional<PublicationRecord> oPr = DB.getPublicationRecordDAO().findByOriginalDataset( ds.getOrigin()) 
					.stream()
					.filter( pr -> pr.getTarget().equals( "fashion.europeana"))
					.findAny();
			
			if( oPr.isPresent()) {
				errJson( "This dataset is already published or publishing.", response );
				return;
			}
			
			
			// Which dataset to publish
			// No magic on this one. Publish this if it is allowed, magic somewhere else (which dataset to publish)
			if( ! PublicationHelper.isSchemaAndHasValidItems(ds, String.join( 
					",", Config.get( "fashionxx.oai.schema"), Config.get("fashion.oai.schema" )))) {
				errJson( "Dataset cannot be published. No valid items of required schema found.", response );
				return;
			}
			
			FashionEdmPublisher.europeanaPublishAsync(ds, publisher);
			okJson( "msg",  "Dataset queued for publishing", response );
		} catch( Exception e ) {
			log.error( "", e );
			errJson( "Something went wrong: '" + e.getMessage() + "'", response );
		}
	}
	
	
	public static void portalUnpublishGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			Optional<Dataset> oDs = accessAllowed( request, response );
			if( !oDs.isPresent()) return;

			// remove from Portal
			new FashionPortalPublication().portalUnpublish(oDs.get());
			
			// remove the Publication record, we dont care if its successful or not
			// In theory we should check if its running though
			Optional<PublicationRecord> oPr = DB.getPublicationRecordDAO().findByOriginalDataset( oDs.get().getOrigin()) 
					.stream()
					.filter( pr -> pr.getTarget().equals( "fashion.portal"))
					.findAny();
			if( oPr.isPresent() )
				DB.getPublicationRecordDAO().makeTransient(oPr.get());
			else {
				errJson( "No Portal publication exists for #" + oDs.get().getDbID() + ". This might not be a problem.", response);
				return;
			}
			okJson( "msg", "Portal unpublish succeeded.", response );
		} catch( Exception e ) {
			log.error( "", e );
			errJson( "Something went wrong: '" + e.getMessage() + "'", response );
			return ;
		}
	}
	
	public static void portalCleanGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		portalUnpublishGet(request, response);
	}
	
	
	public static void portalPublishGet(HttpServletRequest request, HttpServletResponse response) throws Exception {
		try {
			Optional<Dataset> oDs = accessAllowed( request, response );
			final User publisher = (User) request.getAttribute("user");
			if( !oDs.isPresent()) return;
			final Dataset ds = oDs.get();
			
			Optional<PublicationRecord> oPr = DB.getPublicationRecordDAO().findByOriginalDataset( ds.getOrigin()) 
					.stream()
					.filter( pr -> pr.getTarget().equals( "fashion.portal"))
					.findAny();
			
			if( oPr.isPresent()) {
				errJson( "This dataset is already published or publishing.", response );
				return;
			}
						
			// Which dataset to publish
			// No magic on this one. Publish this if it is allowed, magic somewhere else (which dataset to publish)
			if( ! PublicationHelper.isSchemaAndHasValidItems(ds, Config.get( "fashion.portal.schema"))) {
				errJson( "Dataset cannot be published. No valid items of required schema found.", response );
				return;
			}
			
			// Publish takes some time, so we need to queue it
			// but we should mark it as started
			PublicationRecord pr = new PublicationRecord();

			pr.setStartDate(new Date());
			pr.setPublisher(publisher);
			pr.setOriginalDataset(ds.getOrigin());
			pr.setPublishedDataset(ds );
			pr.setStatus(Dataset.PUBLICATION_RUNNING);
			pr.setOrganization(ds.getOrganization());
			pr.setTarget("fashion.portal");
			DB.getPublicationRecordDAO().makePersistent(pr);
			DB.commit();

			Runnable r = () -> {
				try {
					DB.getSession().beginTransaction();
					
					Dataset myDs = DB.getDatasetDAO().getById(ds.getDbID(), false);
					User publishingUser = DB.getUserDAO().getById(publisher.getDbID(), false);
					
					new FashionPortalPublication().portalPublish(myDs, publishingUser);
					
					DB.commit();
				} catch( Exception e ) {
					log.error( "Problems during publish", e );
				} finally {
					DB.closeSession();
					DB.closeStatelessSession();
				}
			};
			
			Queues.queue( r, "db");
			okJson( "msg", "Publication to Portal queued.", response );
		} catch( Exception e ) {
			log.error( "", e );
			errJson( "Something went wrong: '" + e.getMessage() + "'", response );
			return ;
		}
	}	
}
