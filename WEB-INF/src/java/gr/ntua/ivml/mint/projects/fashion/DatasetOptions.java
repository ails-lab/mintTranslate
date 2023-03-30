package gr.ntua.ivml.mint.projects.fashion;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.node.ArrayNode;

import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.PublicationRecord;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.persistent.XmlSchema;
import gr.ntua.ivml.mint.util.Config;
import gr.ntua.ivml.mint.util.Jackson;
import gr.ntua.ivml.mint.util.PublicationHelper;

/**
 * return suitable json to describe what options of activity this dataset has.
 * 
 * [
 * 		{
 * 		"project": "fashion"
 * 		"category": "publish"
 * 		"label": "Publish to the Portal"
 * 		// if in progress, maybe show a loader gif, if there is a url, it
 * 	    // might give you the option to cancel or review progress
 * 		"inProgress": true|false
 * 		
 * 		// if not in progress, label and url indicate what action is possible
 * 		"url":"/api/fashion/datasetPublish/$id?options..."
 * 
 * 	    // the response might contain some javascript/html to interact with the user
 * 		"response": "json| newPanel| replacePanel" 
 * 		}
 * ] 
 * @author stabenau
 *
 */
public class DatasetOptions {
	
	// Jackson should make this into the json we want
	// will go somewhere else, once we have a second project
	public static class Option {
		public String project = "fashion";
		public String category = "publication";
		public String label;
		public boolean inProgress = false;
		public String url;
		
		// json|htmlNewPanel|htmlReplacePanel
		public String response = "json";
	};
	
	// only publishing options are evaluated
	// if you have more options methods in more projects, add them to the api/RequestHandler
	public static ArrayNode options( Dataset ds, User u ) {
		ArrayNode result = Jackson.om().createArrayNode();
	
		List<String> projects = Arrays.asList( ds.getOrigin().getProjectNames());

		// maybe nothing to do here, although we only get here if there is?
		if( !projects.contains("fashion")) return result;
		
		
		List<PublicationRecord> pubs = DB.getPublicationRecordDAO().findByOriginalDataset( ds.getOrigin());
		Optional<PublicationRecord> pPortal = pubs.stream().filter( pr -> "fashion.portal".equals(pr.getTarget())).findAny();
		addPortalOptions( pPortal, result, ds );
		Optional<PublicationRecord> pEuropeana = pubs.stream().filter( pr ->"fashion.europeana".equals(pr.getTarget())).findAny();
		addEuropeanaOptions( pEuropeana, result, ds );
		
		// need prepare for publish options
		return result;
	}
	
	
	/*
	 * When Pub is already running or finished, options are clear
	 * unique edm is there ... allow publish of it
	 *  - if it has edmfp ancestor, publish that as well
	 * if there is no enriched ancestor suggest enriching it (together with the publish suggestion)
	 *  - enrichment based on a fixed name for it
	 * if there is a unique edmfp and NO edm descendent suggest preparing it.
	 * 
	 * if you want a new preparation, you would need to delete the existing edm ... thats should be acceptable
	 */
	private static void addEuropeanaOptions(Optional<PublicationRecord> pEuropeana, ArrayNode result, Dataset ds ) {
		Option option = new Option();
		if( pEuropeana.isPresent()) {
			// show is running, needs cleaning or unpublish
			String status = pEuropeana.get().getStatus();
			switch( status ) {
			case PublicationRecord.PUBLICATION_OK:
				option.label = "Unpublish from Europeana-Fashion";
				option.url = "api/fashion/europeanaUnpublish/"+pEuropeana.get().getPublishedDataset().getDbID();
				break;
			case PublicationRecord.PUBLICATION_RUNNING:
				option.label = "Europeana-Fashion publication running";
				option.inProgress = true;
				option.url = null;
				break;
			case PublicationRecord.PUBLICATION_FAILED:
				option.label = "Clean failed Europeana-Fashion publication";
				option.url = "api/fashion/europeanaClean/"+pEuropeana.get().getPublishedDataset().getDbID();
				break;
			}
			result.add( Jackson.om().valueToTree(option));
		} else {
			List<XmlSchema> schemas = XmlSchema.getSchemasFromConfig( "fashion.oai.schema");
			schemas.addAll(XmlSchema.getSchemasFromConfig( "fashionxx.oai.schema"));

			Optional<Dataset> oDs = PublicationHelper.uniqueSuitableDataset(ds, 
					schemas );

			if( oDs.isPresent() ) {
				option.label = "Publish to Europeana-Fashion";
				option.url = "api/fashion/europeanaPublish/"+oDs.get().getDbID();				
				result.add( Jackson.om().valueToTree(option));
				
				List<Dataset> ancestors = oDs.get().getParentDatasets();
				ancestors.add(ds);
				// no default enrichment present, offer the option
				if( ! ancestors
						.stream()
						.filter( dataset -> FashionEdmPublisher.FASHION_DEFAULT_ENRICHMENT_NAME.equals( dataset.getName()))
						.findAny()
						.isPresent()) {
					Option option2 = new Option();
					option2.label = "Enrich with Creators/Places";
					option2.url = "api/fashion/defaultEnrichment/"+oDs.get().getDbID();				
					result.add( Jackson.om().valueToTree(option2));
				}
				
				if( ! ancestors
						.stream()
						.filter( dataset -> FashionEdmPublisher.FASHION_XX_ENRICHMENT_NAME.equals( dataset.getName()))
						.findAny()
						.isPresent()) {
					Option option2 = new Option();
					option2.label = "Enrich with XX enrichments";
					option2.url = "api/fashion/xxEnrichment/"+oDs.get().getDbID();				
					result.add( Jackson.om().valueToTree(option2));
				}
			}
			
			// can we prepare an fp set to be edm?
			Optional<Dataset> oDsFp = PublicationHelper.uniqueSuitableDataset(ds, 
					Config.get( "fashion.portal.schema"));
			
			if( oDsFp.isPresent()) {
				
				// does it NOT have publishable descendent?
				if( ! oDsFp.get().hasAnySchemaAndValidItems(schemas)) {
					Option option2 = new Option();
					option2.label = "Prepare for Europeana Publish";
					option2.url = "api/fashion/europeanaPrepare/"+oDsFp.get().getDbID();				
					result.add( Jackson.om().valueToTree(option2));
				}
			}
		}
	}

	private static void addPortalOptions( Optional<PublicationRecord> pPortal, ArrayNode result, Dataset ds ) {
		Option option = new Option();
		if( pPortal.isPresent()) {
			// show is running, needs cleaning or unpublish
			String status = pPortal.get().getStatus();
			switch( status ) {
			case PublicationRecord.PUBLICATION_OK:
				option.label = "Unpublish from staging portal";
				option.url = "api/fashion/portalUnpublish/"+pPortal.get().getPublishedDataset().getDbID();
				break;
			case PublicationRecord.PUBLICATION_RUNNING:
				option.label = "Staging portal publication running";
				option.inProgress = true;
				option.url = null;
				break;
			case PublicationRecord.PUBLICATION_FAILED:
				option.label = "Clean failed Portal Publication";
				option.url = "api/fashion/portalClean/"+pPortal.get().getPublishedDataset().getDbID();
				break;
			}
			result.add( Jackson.om().valueToTree(option));
		} else {
			Optional<Dataset> oDs = PublicationHelper.uniqueSuitableDataset(ds, Config.get( "fashion.portal.schema"));
			if( oDs.isPresent() ) {
				option.label = "Publish to Portal";
				option.url = "api/fashion/portalPublish/"+oDs.get().getDbID();				
				result.add( Jackson.om().valueToTree(option));
			}	
		}		
	}
	
	// TODO: Prepare for publish options missing.
}
