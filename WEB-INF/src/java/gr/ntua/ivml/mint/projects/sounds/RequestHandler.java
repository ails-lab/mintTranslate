package gr.ntua.ivml.mint.projects.sounds;

import static gr.ntua.ivml.mint.api.RequestHandler.errJson;
import static gr.ntua.ivml.mint.api.RequestHandler.okJson;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;

import gr.ntua.ivml.mint.api.RequestHandlerUtil;
import gr.ntua.ivml.mint.concurrent.Queues;
import gr.ntua.ivml.mint.concurrent.XSLTransform;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.db.DB.SessionRunnable;
import gr.ntua.ivml.mint.f.ErrorCondition;
import gr.ntua.ivml.mint.f.ThrowingConsumer;
import gr.ntua.ivml.mint.persistent.Crosswalk;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Item;
import gr.ntua.ivml.mint.persistent.PublicationRecord;
import gr.ntua.ivml.mint.persistent.Transformation;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.persistent.XmlSchema;
import gr.ntua.ivml.mint.util.Config;
import gr.ntua.ivml.mint.util.Interceptors.EndEstimateInterceptor;
import gr.ntua.ivml.mint.util.Interceptors.ProgressInterceptor;
import gr.ntua.ivml.mint.util.Jackson;
import gr.ntua.ivml.mint.util.PublicationHelper;
import gr.ntua.ivml.mint.util.PublicationHelper.OaiItemPublisher;

/**
 * Static messages routed here by the router servlet, starting with /api/sounds/...
 * @author stabenau
 *
 */
public class RequestHandler {
	
	public static final Logger log=Logger.getLogger(RequestHandler.class);
	
	public static class Option {
		public String project = "sounds";
		public String category = "publication";
		public String label;
		public boolean inProgress = false;
		public String url;
		
		// json|htmlNewPanel|htmlReplacePanel
		public String response = "json";
	};

	final static String edmSchemaPrefix = "rdf"; // the prefix used for the OAI
	final static String edmSchemaUri = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	final static String edmSoundsSchemaPrefix = "EDMSounds"; // the prefix used for the OAI
	final static String edmSoundsSchemaUri = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

	// this one should be quick enough to not go to Queue
	public static void oaiUnpublishGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			ErrorCondition err = new ErrorCondition();
			Dataset ds = RequestHandlerUtil.accessDataset( request, response, "sounds", err );

			// remove from OAI
			err.onValid( () -> PublicationHelper.oaiUnpublish( ds
					, Config.get( "sounds.queue.routingKey")
					, Config.get( "sounds.oai.server" )
					, Integer.parseInt( Config.get( "sounds.oai.port" ))
					));
			
			// remove the Publication record, we dont care if its successful or not
			// In theory we should check if its running though
			err.onValid(() -> {
				Optional<PublicationRecord> oPr = DB.getPublicationRecordDAO().findByOriginalDataset( ds.getOrigin()) 
						.stream()
						.filter( pr -> pr.getTarget().equals( "sounds"))
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
	
	public static void crosswalkGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			ErrorCondition err = new ErrorCondition();
			Dataset ds = RequestHandlerUtil.accessDataset( request, response, "sounds", err );
			Dataset crosswalkDs = err.grab( ()->canPrepareForPublish(ds).orElse(null), "Dataset cannot crosswalk");

			Crosswalk crosswalk = err.grab(()->{
				List<XmlSchema> targetSchemas = XmlSchema.getSchemasFromConfig("sounds.oai.schema");
				XmlSchema sourceSchema = crosswalkDs.getSchema();
				Crosswalk cs = null;
				for( XmlSchema targetSchema:targetSchemas ) {
					List<Crosswalk> csList =  DB.getCrosswalkDAO().findBySourceAndTarget( sourceSchema,  targetSchema );
					if( csList.size() == 1 ) {
						cs = csList.get(0);
						break;
					}
				}
				return cs;
			}, "Crosswalk not configured in DB" );
			
			Transformation tr = err.grab(()-> {
				Transformation trLocal = Transformation.fromDataset(crosswalkDs, crosswalk);
				trLocal.setName( "Crosswalk to " + crosswalk.getTargetSchema().getName());
				Transformation tr2 =  DB.getTransformationDAO().makePersistent(trLocal);
				DB.commit();
				return tr2;
			}, "Failure to create Transformation" );
			
			err.onValid(()-> {
				XSLTransform transformOp = new XSLTransform(tr);
				Queues.queue(transformOp, "db");
				okJson( "msg", "Crosswalk to "+ crosswalk.getTargetSchema().getName()+" queued.", response );
			});
			err.onFailed(()-> errJson( err.get(), response ));
		} catch( Exception e ) {
			log.error( "", e );
			errJson( "Something went wrong: '" + e.getMessage() + "'", response );
			return ;	
		}
	}
	
	
	
	
	public static void oaiPublishGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			ErrorCondition err = new ErrorCondition();
			Dataset ds = RequestHandlerUtil.accessDataset( request, response, "sounds", err );
			
			Optional<PublicationRecord> oPr = err.grab( ()-> DB.getPublicationRecordDAO().findByOriginalDataset( ds.getOrigin()) 
					.stream()
					.filter( pr -> pr.getTarget().equals( "sounds"))
					.findAny(), "DB access failed");
			
			err.check( ()-> !oPr.isPresent(), "This dataset is already published or publishing.");

			err.check(()-> PublicationHelper.isSchemaAndHasValidItems(ds, Config.get( "sounds.oai.schema")),
					"Dataset cannot be published. No valid items of required schema found.");
			final User publisher = err.grab(()-> (User) request.getAttribute("user"), "No User logged in");

			err.onValid( ()-> {
				// Which dataset to publish
				// No magic on this one. Publish this if it is allowed, magic somewhere else (which dataset to publish)
				
				// Publish takes some time, so we need to queue it
				// but we should mark it as started
				PublicationRecord pr = PublicationRecord.createPublication(
						(User)request.getAttribute( "user" ), ds, "sounds" ); 
		
				SessionRunnable r = () -> {
					try {
						Dataset myDs = DB.getDatasetDAO().getById(ds.getDbID(), false);
						User pubUser = DB.getUserDAO().getById(publisher.getDbID(), false);
						
						PublicationHelper.oaiPublish(myDs, "sounds", pubUser
							, Config.get( "sounds.queue.exchange")
							, Config.get( "sounds.queue.routingKey")
							, Config.get( "sounds.oai.server" )
							, Integer.parseInt( Config.get( "sounds.oai.port" ))
							, null /* interceptor to translate orgIds, or do other stuff to the ItemMessage */
						);
						
						DB.commit();
					} catch( Exception e ) {
						log.error( "Problems during publish", e );
					} 
				};
				
				Queues.queue( r, "db");
				ds.logEvent( "Publication to sounds OAI queued.");
				okJson( "msg", "Publication to OAI queued.", response );
			} );

			err.onFailed(()-> errJson( err.get(), response ));

		} catch( Exception e ) {
			log.error( "", e );
			errJson( "Something went wrong: '" + e.getMessage() + "'", response );
			return ;
		}
	}
	
	public static void europeanaPublishAsync( Dataset ds, User publisher) throws Exception {
		// publish just ds and potentially a parent edm-fp set
		Set<Long> schemaIds = XmlSchema
				.getSchemasFromConfig("sounds.oai.schemaExtra")
				.stream()
				.map( XmlSchema::getDbID)
				.collect( Collectors.toSet());
		// find first parent with that schema
		Dataset parent = ds.getParentDataset();
		while( parent != null ) {
			XmlSchema parentSchema = parent.getSchema();
			if( parentSchema != null )
				if( schemaIds.contains(parent.getSchema().getDbID())) break;
			parent = parent.getParentDataset();
		}
		final Dataset soundsDataset = parent;
		final PublicationRecord pr = PublicationRecord.createPublication(publisher, ds, "sounds");
		
		// create the OAIpublisher
		final OaiItemPublisher oaiItemPublisher = new OaiItemPublisher( Config.get( "sounds.queue.exchange" ), Config.get( "sounds.queue.routingKey"),
				Config.get( "sounds.oai.server"), Integer.parseInt(Config.get( "sounds.oai.port")));
		
		
		long tmpTotalCount = ds.getValidItemCount();
		if( soundsDataset != null ) {
			tmpTotalCount += soundsDataset.getValidItemCount();
			ds.logEvent("Sending two datasets to OAI");
		}
		final long totalCount = tmpTotalCount;
		
		DB.SessionRunnable r =() -> {
			
			// put stuff in hibernate session
			Dataset localSoundsEdmDs = (soundsDataset==null)?
					null:
					DB.getDatasetDAO().getById(soundsDataset.getDbID(), false );
			Dataset localDs = DB.getDatasetDAO().getById(ds.getDbID(), false );
			PublicationRecord localPr = DB.getPublicationRecordDAO().getById(pr.getDbID(), false);

			// run it with end guess and progress markers in log
			try (ProgressInterceptor progressInterceptor = new ProgressInterceptor("Published 'fashion.europeana' %d of %d for Dataset #"+localDs.getDbID(), totalCount)) {

				// Set it up: endestimate->progress->script ( storingConsumer)
				oaiItemPublisher.setDataset(localDs, edmSchemaPrefix, edmSchemaUri);
				ThrowingConsumer<Item> itemSink = item -> oaiItemPublisher.sendItem(item);

				final ThrowingConsumer<Item> sourceConsumer = new EndEstimateInterceptor(totalCount,
						localDs)
						.into(progressInterceptor)
						.intercept(itemSink);

				localDs.processAllValidItems(item -> sourceConsumer.accept(item), true );
				if( localSoundsEdmDs != null ) {
					// sounds schema is just rdf
					oaiItemPublisher.setDataset(localSoundsEdmDs, edmSoundsSchemaPrefix, edmSoundsSchemaUri);
					localSoundsEdmDs.processAllValidItems(item -> sourceConsumer.accept(item), true );
				}			
			} catch( Exception e ) {
				log.error( "Publication failed", e );
				localPr.setStatus(PublicationRecord.PUBLICATION_FAILED);
				localPr.setEndDate(new Date());
				localPr.setPublishedItemCount(-1);
				localPr.setReport(e.getMessage());
				DB.getPublicationRecordDAO().makePersistent(localPr);
			} finally {
				// this sets the pr
				oaiItemPublisher.finishPublication(localPr);
			}
		};
		
		Queues.queue(r, "db" );
	}

	public static ArrayNode datasetPublishOptions( Dataset ds, User user ) {
		ArrayNode result = Jackson.om().createArrayNode();
		
		List<String> projects = Arrays.asList( ds.getOrigin().getProjectNames());

		// maybe nothing to do here, although we only get here if there is?
		if( !projects.contains("photo")) return result;
		
		
		List<PublicationRecord> pubs = DB.getPublicationRecordDAO().findByOriginalDataset( ds.getOrigin());
		Optional<PublicationRecord> relevantPub = pubs.stream().filter( pr -> pr.getTarget().equals( "sounds")).findAny();
		Option option = new Option();
	
		if( relevantPub.isPresent()) {
			// show is running, needs cleaning or unpublish
			String status = relevantPub.get().getStatus();
			switch( status ) {
			case PublicationRecord.PUBLICATION_OK:
				option.label = "Unpublish from Sounds OAI";
				option.url = "api/sounds/oaiUnpublish/"+relevantPub.get().getPublishedDataset().getDbID();
				break;
			case PublicationRecord.PUBLICATION_RUNNING:
				option.label = "Sounds OAI publication running";
				option.inProgress = true;
				option.url = null;
				break;
			case PublicationRecord.PUBLICATION_FAILED:
				option.label = "Clean failed Sounds OAI publication";
				option.url = "api/sounds/oaiClean/"+relevantPub.get().getPublishedDataset().getDbID();
				break;
			}
			result.add( Jackson.om().valueToTree(option));
		} else {
			// is there a unique suitable dataset in the tree?
			Optional<Dataset> oDs = PublicationHelper.uniqueSuitableDataset(ds, Config.get( "sounds.oai.schema"));
			if( oDs.isPresent() ) {
				option.label = "Publish to Sounds OAI";
				option.url = "api/sounds/oaiPublish/"+oDs.get().getDbID();				
				result.add( Jackson.om().valueToTree(option));
			} 
			
			Optional<Dataset> possibleCrosswalkDs = canPrepareForPublish(ds);
			possibleCrosswalkDs.ifPresent( crosswalkDs -> {
				option.label = "Crosswalk SoundsEDM to EDM";
				option.url = "api/sounds/crosswalk/"+crosswalkDs.getDbID();				
				result.add( Jackson.om().valueToTree(option));
			} );
		}
		
		return result;
	}
	
	
	// if there is a sounds EDM ds but no edm, we can crosswalk
	public static Optional<Dataset> canPrepareForPublish( Dataset ds ) {
		Optional<Dataset> oDs = PublicationHelper.uniqueSuitableDataset(ds, Config.get( "sounds.oai.schemaExtra"));		
		if( oDs.isPresent() ) {
			Dataset extra = oDs.get();
			Optional<Dataset> oDs2 = PublicationHelper.uniqueSuitableDataset(extra, Config.get( "sounds.oai.schema"));
			if( !oDs2.isPresent()) {
				return oDs;
			}
		}
		return Optional.empty();
	}
}
