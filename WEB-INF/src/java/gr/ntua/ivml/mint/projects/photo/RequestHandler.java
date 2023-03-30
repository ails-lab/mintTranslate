package gr.ntua.ivml.mint.projects.photo;

import static gr.ntua.ivml.mint.api.RequestHandler.errJson;
import static gr.ntua.ivml.mint.api.RequestHandler.getPathInt;
import static gr.ntua.ivml.mint.api.RequestHandler.okJson;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;

import gr.ntua.ivml.mint.concurrent.Queues;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.f.ErrorCondition;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Project;
import gr.ntua.ivml.mint.persistent.PublicationRecord;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.util.Config;
import gr.ntua.ivml.mint.util.Jackson;
import gr.ntua.ivml.mint.util.PublicationHelper;

/**
 * Static messages routed here by the router servlet, starting with /api/fashion/...
 * @author stabenau
 *
 */
public class RequestHandler {
	
	public static final Logger log=Logger.getLogger(RequestHandler.class);
	
	public static class Option {
		public String project = "photo";
		public String category = "publication";
		public String label;
		public boolean inProgress = false;
		public String url;
		
		// json|htmlNewPanel|htmlReplacePanel
		public String response = "json";
	};

	// get the dataset and check if the user is allowed to do anything 
	// and if the dataset belongs to fashion.
	// if dataset is null, response already contains errcode and mesg
	private static Dataset accessAllowed( HttpServletRequest request, HttpServletResponse response, ErrorCondition err ) throws Exception {
		
		// err does nothing once it has an error condition. grabs return null
		Integer datasetId = err.grab(()->getPathInt( request, 3 ), "Invalid dataset id."); 
		Dataset ds = err.grab( ()->DB.getDatasetDAO().getById((long) datasetId, false ), "Unknown Dataset" ); 
		User u = err.grab(()-> (User) request.getAttribute("user"), "No User logged in");

		Project p = err.grab(()-> DB.getProjectDAO().findByName("photo"), "'photo' project not present in db" );
		err.check(() -> p.hasProject(ds.getOrigin()), "Dataset not in photo" );
		
		err.check( ()-> u.can( "change data", ds.getOrganization() ) || p.hasProject(u), "User has no access");

		return err.grab(()->ds,"");
	}
	
	// TODO: These should be post requests, to be done much later
	
	// this one should be quick enough to not go to Queue
	public static void oaiUnpublishGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			ErrorCondition err = new ErrorCondition();
			Dataset ds = accessAllowed( request, response, err );

			// remove from OAI
			err.onValid( () -> PublicationHelper.oaiUnpublish( ds
					, Config.get( "photo.queue.routingKey")
					, Config.get( "photo.oai.server" )
					, Integer.parseInt( Config.get( "photo.oai.port" ))
					));
			
			// remove the Publication record, we dont care if its successful or not
			// In theory we should check if its running though
			err.onValid(() -> {
				Optional<PublicationRecord> oPr = DB.getPublicationRecordDAO().findByOriginalDataset( ds.getOrigin()) 
						.stream()
						.filter( pr -> pr.getTarget().equals( "photo"))
						.findAny();
				if( oPr.isPresent() )
					DB.getPublicationRecordDAO().makeTransient(oPr.get());
				okJson( "msg", "OAI unpublish succeeded.", response );
			} );
			
			err.onFailed(()-> errJson( err.get(), response ));
		} catch( Exception e ) {
			log.error( "", e );
			errJson( "Something went wrong: '" + e.getMessage() + "'", response );
			return ;
		}
	}

	// maybe the same as unpublish
	public static void oaiCleanGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		oaiUnpublishGet(request, response);
	}
	
	
	public static void oaiPublishGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			ErrorCondition err = new ErrorCondition();
			Dataset ds = accessAllowed( request, response, err );
			
			Optional<PublicationRecord> oPr = err.grab( ()-> DB.getPublicationRecordDAO().findByOriginalDataset( ds.getOrigin()) 
					.stream()
					.filter( pr -> pr.getTarget().equals( "photo"))
					.findAny(), "DB access failed");
			
			err.check( ()-> !oPr.isPresent(), "This dataset is already published or publishing.");

			err.check(()-> PublicationHelper.isSchemaAndHasValidItems(ds, Config.get( "photo.oai.schema")),
					"Dataset cannot be published. No valid items of required schema found.");
			final User publisher = err.grab(()-> (User) request.getAttribute("user"), "No User logged in");

			err.onValid( ()-> {
				// Which dataset to publish
				// No magic on this one. Publish this if it is allowed, magic somewhere else (which dataset to publish)
				
				// Publish takes some time, so we need to queue it
				// but we should mark it as started
				PublicationRecord pr = new PublicationRecord();
	
				pr.setStartDate(new Date());
				pr.setPublisher((User)request.getAttribute( "user" ));
				pr.setOriginalDataset(ds.getOrigin());
				pr.setPublishedDataset(ds );
				pr.setStatus(Dataset.PUBLICATION_RUNNING);
				pr.setOrganization(ds.getOrganization());
				pr.setTarget("photo");
				DB.getPublicationRecordDAO().makePersistent(pr);
				DB.commit();
	
				Runnable r = () -> {
					try {
						DB.getSession().beginTransaction();
						Dataset myDs = DB.getDatasetDAO().getById(ds.getDbID(), false);
						User pubUser = DB.getUserDAO().getById(publisher.getDbID(), false);
						
						PublicationHelper.oaiPublish(myDs, "photo", pubUser
							, Config.get( "photo.queue.exchange")
							, Config.get( "photo.queue.routingKey")
							, Config.get( "photo.oai.server" )
							, Integer.parseInt( Config.get( "photo.oai.port" ))
							, null /* interceptor to translate orgIds*/
						);
						
						DB.commit();
					} catch( Exception e ) {
						log.error( "Problems during publish", e );
					} finally {
						DB.closeSession();
						DB.closeStatelessSession();
					}
				};
				
				Queues.queue( r, "db");
				ds.logEvent( "Publication to photo OAI queued.");
				okJson( "msg", "Publication to OAI queued.", response );
			} );

			err.onFailed(()-> errJson( err.get(), response ));

		} catch( Exception e ) {
			log.error( "", e );
			errJson( "Something went wrong: '" + e.getMessage() + "'", response );
			return ;
		}
	}
	
	public static ArrayNode datasetPublishOptions( Dataset ds, User user ) {
		ArrayNode result = Jackson.om().createArrayNode();
		
		List<String> projects = Arrays.asList( ds.getOrigin().getProjectNames());

		// maybe nothing to do here, although we only get here if there is?
		if( !projects.contains("photo")) return result;
		
		
		List<PublicationRecord> pubs = DB.getPublicationRecordDAO().findByOriginalDataset( ds.getOrigin());
		Optional<PublicationRecord> relevantPub = pubs.stream().filter( pr -> pr.getTarget().equals( "photo")).findAny();
		Option option = new Option();
	
		if( relevantPub.isPresent()) {
			// show is running, needs cleaning or unpublish
			String status = relevantPub.get().getStatus();
			switch( status ) {
			case PublicationRecord.PUBLICATION_OK:
				option.label = "Unpublish from Photography OAI";
				option.url = "api/photo/oaiUnpublish/"+relevantPub.get().getPublishedDataset().getDbID();
				break;
			case PublicationRecord.PUBLICATION_RUNNING:
				option.label = "Photography OAI publication running";
				option.inProgress = true;
				option.url = null;
				break;
			case PublicationRecord.PUBLICATION_FAILED:
				option.label = "Clean failed Photography OAI publication";
				option.url = "api/photo/oaiClean/"+relevantPub.get().getPublishedDataset().getDbID();
				break;
			}
			result.add( Jackson.om().valueToTree(option));
		} else {
			// is there a unique suitable dataset in the tree?
			Optional<Dataset> oDs = PublicationHelper.uniqueSuitableDataset(ds, Config.get( "photo.oai.schema"));
			if( oDs.isPresent() ) {
				option.label = "Publish to Photography OAI";
				option.url = "api/photo/oaiPublish/"+oDs.get().getDbID();				
				result.add( Jackson.om().valueToTree(option));
			}
		}
		
		return result;
	}
	
	
	// if there is a lido schema ds but no edm, we can crosswalk
	public static boolean canPrepareForPublish( Dataset ds ) {
		
		return false;
	}
}
